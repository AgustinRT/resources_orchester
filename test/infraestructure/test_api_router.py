import httpx
import pytest
from unittest.mock import AsyncMock

from fastapi import FastAPI

from resources_orchester.domain.exception.infrastructure_exceptions import MessagingException, PersistenceException
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.bootstrap.dependencies import get_create_message_resource

from test.infraestructure.app_tester import build_test_app, client_for

# httpx.AsyncClient + ASGITransport sobre la aplicación real construida con create_app (mismo
# patrón que test_api_router_search.py). ASGITransport no ejecuta el lifespan: no se crean
# conexiones a PostgreSQL ni a Kafka.


@pytest.fixture
def app() -> FastAPI:
    return build_test_app()


def _client(app: FastAPI) -> httpx.AsyncClient:
    return client_for(app)


def _create_url(app: FastAPI, user_id: str) -> str:
    return app.url_path_for("create_message_resource_endpoint", id_user=user_id)


@pytest.fixture(scope="function", autouse=True)
def mock_resource_manager(app: FastAPI):
    """
    Fixture que simula (mockea) completamente la dependencia del servicio y la inyecta en la
    aplicación con dependency_overrides. Es autouse: sin override, FastAPI resolvería las
    dependencias reales (PostgreSQL y Kafka) incluso en los tests de validación (422/404).
    """
    mock_service = AsyncMock()
    app.dependency_overrides[get_create_message_resource] = lambda: mock_service
    yield mock_service
    app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_e2e_happy_path_create_message_resource(app, mock_resource_manager):
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
        "process_apply_del": [],
        "fields_add": [{"name": "key", "type": "str", "description": ["Key field"]}],
        "fields_del": []
    }

    # --- MOCKING LA RESPUESTA EXITOSA DEL SERVICIO ---
    expected_domain_model = DataResources(
        name="new_kafka_topic",
        description={"es_ES": "test topic", "en_US": "test topic"}, 
        competence="messaging",
        process_apply=["processing_a"], 
        type_data="json", 
        fields=[{"name": "key", "type": "str", "description": {"es_ES": "Key field"}}]
    )
    # Configuramos que la llamada al método del servicio devuelva este objeto mockeado.
    mock_resource_manager.create_message_resource = AsyncMock(return_value=expected_domain_model)

    # 3. Ejecutar la petición HTTP simulada
    async with _client(app) as client:
        response = await client.post(
            _create_url(app, user_id),
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
    sent_dto = mock_resource_manager.create_message_resource.await_args.args[0]
    assert sent_dto.name == "new_kafka_topic"
    assert sent_dto.fields_add[0].description == ["Key field"]


@pytest.mark.asyncio
async def test_e2e_error_business_failure(app, mock_resource_manager):
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
        "process_apply_del":[],
        "fields_add":[],
        "fields_del":[]
    }

    # --- MOCKING EL FALLO DE NEGOCIO EN EL SERVICIO ---
    mock_resource_manager.create_message_resource = AsyncMock(side_effect=Exception("Kafka Connection Timeout"))

    # Ejecutar la petición HTTP simulada
    async with _client(app) as client:
        response = await client.post(
            _create_url(app, user_id),
            json=payload
        )

    # Assertions de Respuesta (HTTP Level Check)
    assert response.status_code == 400 # El endpoint debe mapear la excepción a 400 Bad Request.
    data = response.json()
    
    # Verificación del mensaje de error que el usuario final verá (UX Focus).
    # HTTPException(detail={...}) se serializa bajo la clave "detail".
    assert data["detail"]["message"] == "Fallo en el proceso de creación de recurso."
    assert "Kafka Connection Timeout" in data["detail"]["detalle"]


@pytest.mark.asyncio
async def test_e2e_error_validation_failure(app, mock_resource_manager):
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

    async with _client(app) as client:
        response = await client.post(
            _create_url(app, user_id),
            json=malformed_payload
        )

    # Assertions de Respuesta (HTTP Level Check)
    assert response.status_code == 422 # Código estándar para errores de validación de entrada DTO.
    data = response.json()
    
    # Verificación del mensaje de error de Pydantic/FastAPI.
    assert "description" in str(data["detail"])
    mock_resource_manager.create_message_resource.assert_not_awaited()


@pytest.mark.asyncio
async def test_e2e_path_param_missing(app, mock_resource_manager):
    """TEST EDGE CASE: Verifica que el endpoint falle si falta un parámetro path obligatorio."""
    # Intentamos enviar la petición sin especificar {id_user} en la URL.
    payload = {"name": "test", "description": ["ok"], "process_apply_add":[], "process_apply_del":[], "fields_add": [], "fields_del": []}

    async with _client(app) as client:
        response = await client.post(
            f"{app.state.settings.api.prefix}/messages/create", # Falta el ID del usuario
            json=payload
        )
    
    # FastAPI debería devolver 404 (Not Found) o 422 (Bad Request) si la ruta no coincide.
    assert response.status_code in [404, 422]
    mock_resource_manager.create_message_resource.assert_not_awaited()


@pytest.mark.asyncio
async def test_e2e_messaging_unavailable_returns_503(app, mock_resource_manager):
    """
    TEST ERROR HANDLING E2E: si Kafka no está disponible el servicio lanza MessagingException;
    no es un error del cliente, así que el endpoint responde 503 (y la transacción se revierte).
    """
    payload = {"name": "topic_without_broker", "description": ["es_ES d", "en_US d"], "fields_add": []}
    mock_resource_manager.create_message_resource = AsyncMock(side_effect=MessagingException("Kafka no disponible"))

    async with _client(app) as client:
        response = await client.post(_create_url(app, "user503"), json=payload)

    assert response.status_code == 503
    assert response.json()["detail"]["detalle"] == "Kafka no disponible"


@pytest.mark.asyncio
async def test_e2e_persistence_failure_returns_500(app, mock_resource_manager):
    """
    TEST ERROR HANDLING E2E (review D2): si la transacción no se puede confirmar, el caso de uso lanza
    PersistenceException y el endpoint responde 500 (error del servidor): nunca 201 ni 400.
    """
    payload = {"name": "not_committed", "description": ["es_ES d", "en_US d"], "fields_add": []}
    mock_resource_manager.create_message_resource = AsyncMock(side_effect=PersistenceException("commit rejected"))

    async with _client(app) as client:
        response = await client.post(_create_url(app, "user500"), json=payload)

    assert response.status_code == 500
    assert response.json()["detail"]["message"] == "No se ha podido guardar el recurso; no se ha creado ni notificado."
    assert "commit rejected" not in response.text  # internal details are logged, not returned

