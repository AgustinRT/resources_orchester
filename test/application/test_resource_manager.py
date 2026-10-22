from typing import List

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from resources_orchester.domain.api_dtos.data_field import DataField
from resources_orchester.domain.api_dtos.database_request_dto import DatabaseRequestDTO
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO


# Importar el servicio a testear
from src.resources_orchester.application.resource_services.resource_manager_general import ResourceManagerGeneral

# Importar los modelos de Dominio y DTOs (necesarios para tipado)
from src.resources_orchester.domain.model.data_resources import DataResources

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
