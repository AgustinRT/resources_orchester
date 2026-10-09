from typing import List

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from resources_orchester.infrastructure.api_router.dtos.data_field import DataField
from resources_orchester.infrastructure.api_router.dtos.messages_request_dto import MessagesRequestDTO

# Importar el servicio a testear
from src.resources_orchester.application.resource_services.resource_manager_general import ResourceManagerGeneral

# Importar los modelos de Dominio y DTOs (necesarios para tipado)
from src.resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.api_router.dtos.database_request_dto import DatabaseRequestDTO
from resources_orchester.infrastructure.api_router.dtos.rest_request_dto import RestRequestDTO

# --- Fixture de Inicialización ---
@pytest_asyncio.fixture
async def resource_manager():
    """Proporciona una instancia del servicio que será mockeada si es necesario."""
    return ResourceManagerGeneral()


# =======================================================================
# MOCK HELPERS: DTOs y Modelos (Para simular datos de prueba sin crear archivos temporales)
# =======================================================================

def create_mock_data_field() -> DataField:
    """Crea un DataField mock para usar en los payloads."""
    mockedFields: DataField = DataField(name="test", description=["lorem Ipson Desc"], type="string", validations=["not null", "not blanck"])
    return mockedFields

def create_mock_messages_dto() -> MessagesRequestDTO:
    """Crea un payload de mensajes básico y válido."""
    # Usamos el modelo real, pero llenando con datos mockeados.
    return MessagesRequestDTO(
                                name="test_resource", 
                                description=["es_ES example", "en_US example"], 
                                process_apply_add=[],
                                process_apply_del=[],
                                fields_add=[],
                                fields_del=[]
                            )


# def create_mock_database_dto() -> DatabaseRequestDTO:
#     """Crea un payload de BD básico y válido."""
#     return DatabaseRequestDTO(
#         name=None, 
#         description=["es_ES db example", "en_US db example"], 
#         schema="test_schema", 
#         permissions=[] if [] else None, # Permitir lista vacía o None
#         process_apply_add=[], 
#         process_apply_del=[], 
#         fields_add=[create_mock_data_field()], 
#         fields_del=[]
#     )

# def create_mock_rest_dto() -> RestRequestDTO:
#     """Crea un payload REST básico y válido."""
#     # Endpoints deben ser listas de dicts, con claves (GET/POST) y valores (list of strings).
#     return RestRequestDTO(
#         name="test_api", 
#         description=["es_ES api example", "en_US api example"], 
#         endpoints_request=[{"GET": ["list"]}, {"POST": ["create"]}], # Lista de dicts para Endpoints
#         endpoints_response=[{"success": ["bool"]}],
#         fields_add=[create_mock_data_field()],
#         fields_del=[]
#     )


# =======================================================================
# PRUEBAS DE MENSAJES (MESSAGING / KAFKA)
# =======================================================================

@pytest.mark.asyncio
async def test_create_message_resource_happy_path(resource_manager: ResourceManagerGeneral):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources 
    y simula la creación exitosa del recurso.
    """
    payload = create_mock_messages_dto()
    payload.fields_add.append(create_mock_data_field())
    result = await resource_manager.create_message_resource(payload)

    # Assertions sobre el Dominio (DataResources)
    assert isinstance(result, DataResources)
    assert result.name == "test_resource"
    assert result.competence == "messaging"
    assert result.type_data == "json"
    assert len(result.fields) > 0

@pytest.mark.asyncio
async def test_update_message_resource_happy_path(resource_manager: ResourceManagerGeneral):
    """
    TEST HAPPY PATH: Verifica que la actualización de un mensaje es procesada y devuelve el dominio actualizado.
    """
    # Payload con cambios (simulados)
    payload = create_mock_messages_dto()
    payload.fields_add.append(create_mock_data_field())
    resource_name = "test_resource"

    result = await resource_manager.update_message_resource(resource_name, payload)

    assert isinstance(result, DataResources)
    # Verificamos que el nombre sigue siendo correcto y la competencia se mantuvo (sin cambiarlo).
    assert result.name == resource_name 
    assert "updated-" in str(result.process_apply[0]) # Verifica la lógica de proceso en update


@pytest.mark.asyncio
async def test_message_create_empty_fields_and_optional_data():
    """TEST EDGE CASE: Prueba que el servicio maneje correctamente DTOs con campos opcionales vacíos."""
    payload = create_mock_messages_dto()
    payload.process_apply_add = None
    manager = ResourceManagerGeneral() # Creamos instancia manualmente para el test

    result = await manager.create_message_resource(payload)

    assert isinstance(result, DataResources)
    assert result.process_apply is None


# =======================================================================
# PRUEBAS DE BASE DE DATOS (DATABASE / DB)
# =======================================================================

# @pytest.mark.asyncio
# async def test_create_database_resource_happy_path(resource_manager: ResourceManagerGeneral):
#     """TEST HAPPY PATH: Verifica que un DTO de BD se transforma correctamente en DataResources."""
#     payload = create_mock_database_dto()
#     result = await resource_manager.create_database_resource(payload)

#     # Assertions sobre el Dominio (DataResources)
#     assert isinstance(result, DataResources)
#     assert result.competence == "database"
#     assert result.type_data == "dbRow"
#     assert len(result.fields) > 0 # Debe mapear al menos un campo

# @pytest.mark.asyncio
# async def test_update_database_resource_happy_path(resource_manager: ResourceManagerGeneral):
#     """TEST HAPPY PATH: Verifica que la actualización de BD se procese y devuelva el dominio."""
#     payload = create_mock_database_dto()
#     resource_name = "my_db_table"

#     result = await resource_manager.update_database_resource(resource_name, payload)

#     assert isinstance(result, DataResources)
#     assert result.name == resource_name


@pytest.mark.asyncio
async def test_database_create_missing_description_fails():
    """TEST ERROR HANDLING: Debe fallar si la descripción (obligatoria) del DTO está ausente."""
    # Creamos un payload que falla al inicializar porque description es obligatorio.
    payload = DatabaseRequestDTO(
        name="fail", 
        description=[], # Vacío o None, dependiendo de cómo FastAPI lo valide en realidad
        schema="test_schema"
        # Aquí el error se detectará en la validación del DTO antes de entrar al servicio.
    )
    manager = ResourceManagerGeneral()

    # Nota: En un entorno real con Pydantic/FastAPI, este test debería verificar 
    # que FastAPI lanza 422 UNPROCESSABLE_ENTITY. Aquí solo verificamos el fallo interno
    # o la capacidad de inicializar.
    with pytest.raises(Exception) as excinfo:
        await manager.create_database_resource(payload)
    
    # El error exacto dependerá de si el mock DTO pasó validación, pero confirmamos que falla.
    print("Test passed for expected failure (Missing Description).")


# =======================================================================
# PRUEBAS REST (API GATEWAY / HTTP/JSON)
# =======================================================================

# @pytest.mark.asyncio
# async def test_create_rest_resource_happy_path(resource_manager: ResourceManagerGeneral):
#     """TEST HAPPY PATH: Verifica que un DTO de API se transforma correctamente en DataResources."""
#     payload = create_mock_rest_dto()
#     result = await resource_manager.create_rest_resource(payload)

#     # Assertions sobre el Dominio (DataResources)
#     assert isinstance(result, DataResources)
#     assert result.competence == "api_gateway"
#     assert result.type_data == "http/json"


# @pytest.mark.asyncio
# async def test_rest_resource_update_happy_path(resource_manager: ResourceManagerGeneral):
#     """TEST HAPPY PATH: Verifica que la actualización de API se procese correctamente."""
#     payload = create_mock_rest_dto()
#     resource_name = "my_api_endpoint"

#     result = await resource_manager.update_rest_resource(resource_name, payload)

#     assert isinstance(result, DataResources)
#     assert result.name == resource_name


# =======================================================================
# PRUEBAS DE COMUNICACIÓN (RECURSOS GENERALES)
# =======================================================================

# @pytest.mark.asyncio
# async def test_communication_info_valid_strategy():
#     """TEST HAPPY PATH: Verifica que el servicio maneja correctamente una estrategia de comunicación válida."""
#     manager = ResourceManagerGeneral()
#     # Usamos un payload mockeado para la estructura, aunque solo usa el Enum.
#     from src.resources_orchester.domain.model.communications_resource import CommunicationsResource
    
#     valid_comms = CommunicationsResource(
#         name="test", description={}, auth_apply="JWT", strategy="Test"
#     )

#     result = await manager.process_communications_info(valid_comms)
#     assert result is True


# @pytest.mark.asyncio
# async def test_communication_info_invalid_strategy():
#     """TEST ERROR HANDLING: Verifica que el servicio devuelva False para una estrategia de seguridad no soportada."""
#     manager = ResourceManagerGeneral()

#     # Usamos un payload mockeado con un Enum inválido
#     from src.resources_orchester.domain.model.communications_resource import CommunicationsResource
#     invalid_comms = CommunicationsResource(
#         name="test", description={}, auth_apply="WEAK_AUTH", strategy="Test" # Esto es lo que debe fallar
#     )

#     result = await manager.process_communications_info(invalid_comms)
#     assert result is False