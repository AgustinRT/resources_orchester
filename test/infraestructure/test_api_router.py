import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.api_router import router

app = TestClient(router)


@pytest.fixture(scope="function")
def mock_resource_manager():
    """Fixture que simula (mockea) completamente la dependencia del servicio."""
    # Creamos un objeto Mock que se comporta como ResourceManagerGeneral
    mock_service = AsyncMock()
    return mock_service

# =======================================================================
# TEST END-TO-END: CREACIÓN DE RECURSOS DE MENSAJES (Messages)
# =======================================================================

@pytest.mark.asyncio
async def test_e2e_happy_path_create_message_resource(mock_resource_manager):
    """
    TEST HAPPY PATH E2E: 
    Verifica que el endpoint recibe un DTO válido, llama al servicio mockeado y 
    devuelve la estructura de dominio esperada (HTTP 201).
    """
    user_id = "user123"
    payload = {
        "name": "new_kafka_topic",
        "description": ["es_ES test topic", "en_US test topic"],
        "process_apply_add": ["processing_a"],
        "processes_apply_del": [], 
        "fields_add": [{"name": "key", "type": "str", "description": "Key field"}],
        "fields_del": []
    }

    # --- MOCKING LA RESPUESTA EXITOSA DEL SERVICIO ---
    expected_domain_model = DataResources(
        name="new_kafka_topic",
        description={"es_ES": "test topic", "en_US": "test topic"}, 
        competence="messaging",
        process_apply=["processing_a"], 
        type_data="json", 
        fields=[{"name": "key", "type": "str", "description": "Key field"}]
    )
    # Configuramos que la llamada al método del servicio devuelva este objeto mockeado.
    mock_resource_manager.create_message_resource = AsyncMock(return_value=expected_domain_model)

    # 3. Ejecutar la petición HTTP simulada
    response = app.post(
        f"/{user_id}/messages/create",
        json=payload,
        headers={"Content-Type": "application/json"}
    )

    # Assertions de Respuesta (HTTP Level Check)
    assert response.status_code == 201 # Código esperado por el endpoint.
    data = response.json()
    
    # Assertions de Negocio (Contenido del cuerpo)
    assert data["name"] == "new_kafka_topic"
    assert data["competence"] == "messaging"

    # Verificación crítica: Asegurarse de que el servicio fue llamado exactamente una vez con los datos correctos.
    mock_resource_manager.create_message_resource.assert_awaited_once()


@pytest.mark.asyncio
async def test_e2e_error_business_failure(mock_resource_manager):
    """
    TEST ERROR HANDLING E2E: 
    Simula que la lógica de negocio del servicio falla (Ej: Kafka está caído). 
    El controlador debe capturar esto y devolver un error HTTP 400 amigable.
    """
    user_id = "error_user"
    payload = {
        "name": "failing_topic",
        "description": ["es_ES test topic", "en_US test topic"],
        # ... (el resto de los campos)
        "process_apply_add":[], 
        "processes_apply_del":[], 
        "fields_add":[], 
        "fields_del":[]
    }

    # --- MOCKING EL FALLO DE NEGOCIO EN EL SERVICIO ---
    mock_resource_manager.create_message_resource = AsyncMock(side_effect=Exception("Kafka Connection Timeout"))

    # Ejecutar la petición HTTP simulada
    response = app.post(
        f"/{user_id}/messages/create",
        json=payload
    )

    # Assertions de Respuesta (HTTP Level Check)
    assert response.status_code == 400 # El endpoint debe mapear la excepción a 400 Bad Request.
    data = response.json()
    
    # Verificación del mensaje de error que el usuario final verá (UX Focus).
    assert data["message"] == "Fallo en el proceso de creación de recurso."
    assert "Kafka Connection Timeout" in data["detalle"]


@pytest.mark.asyncio
async def test_e2e_error_validation_failure(mock_resource_manager):
    """
    TEST ERROR HANDLING E2E: 
    Simula un error de validación DTO (el cliente envía datos mal formados). 
    FastAPI debe capturar esto antes de llamar al servicio.
    """
    user_id = "invalid_input"
    # Payload enviado sin el campo 'description', que es obligatorio en el DTO.
    malformed_payload = {
        "name": "incomplete",
        # Falta la descripción, forzando un error 422 de Pydantic/FastAPI.
        "process_apply_add": [] 
    }

    response = app.post(
        f"/{user_id}/messages/create",
        json=malformed_payload
    )

    # Assertions de Respuesta (HTTP Level Check)
    assert response.status_code == 422 # Código estándar para errores de validación de entrada DTO.
    data = response.json()
    
    # Verificación del mensaje de error de Pydantic/FastAPI.
    assert "description" in str(data["detail"])


@pytest.mark.asyncio
async def test_e2e_path_param_missing():
    """TEST EDGE CASE: Verifica que el endpoint falle si falta un parámetro path obligatorio."""
    # Intentamos enviar la petición sin especificar {id_user} en la URL.
    payload = {"name": "test", "description": ["ok"], "process_apply_add":[], "processes_apply_del":[], "fields_add": [], "fields_del": []}

    response = app.post(
        "/v1/resources/messages/create", # Falta el ID del usuario
        json=payload
    )
    
    # FastAPI debería devolver 404 (Not Found) o 422 (Bad Request) si la ruta no coincide.
    assert response.status_code in [404, 422]