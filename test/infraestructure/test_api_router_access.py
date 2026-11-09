"""
Resource-access endpoints (vector table resource_user_access).

* HTTP contract: the ManageResourceAccess service is mocked (dependency override); checks request
  validation, success responses and how each service error is reported.
* End-to-end: real DI graph (router -> ResourceAccessPortIn -> ManageResourceAccess ->
  ResourceAccessRepository -> PostgreSQL/pgvector Testcontainer). Only the AI model is replaced
  (FakeEmbeddingClient) and the DB session is the rolled-back test session.
"""
from typing import AsyncGenerator
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from resources_orchester.domain.exception.domain_exceptions import NotFoundException, ValidationException
from resources_orchester.domain.exception.infrastructure_exceptions import EmbeddingException, PersistenceException
from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.domain.model.resource_access_match import ResourceAccessMatch
from resources_orchester.infrastructure.bootstrap import dependencies
from resources_orchester.infrastructure.bootstrap.dependencies import (
    get_db_session,
    get_embedding_client,
    get_resource_access_service,
    get_unit_of_work,
)
from resources_orchester.infrastructure.persistence.repositories.reference_repository import ReferenceRepository
from resources_orchester.infrastructure.persistence.unit_of_work import SqlAlchemyUnitOfWork

from test.infraestructure.app_tester import build_test_app, client_for
from test.infraestructure.fake_embedding_client import FAKE_MODEL_NAME, FakeEmbeddingClient
from test.infraestructure.utils_tester import create_random_data_resources


@pytest.fixture
def app() -> FastAPI:
    test_app = build_test_app()
    yield test_app
    test_app.dependency_overrides.clear()


@pytest.fixture
def mock_service(app: FastAPI) -> AsyncMock:
    service = AsyncMock()
    service.register_access.side_effect = lambda request: ResourceAccess(
        username=request.username, resource_id=request.resource_id, predicate=request.predicate,
        predicate_embed=[0.1, 0.2], model_name=FAKE_MODEL_NAME)
    service.search_accesses.return_value = []
    service.search_similar_accesses.return_value = []
    app.dependency_overrides[get_resource_access_service] = lambda: service
    return service


def _client(app: FastAPI) -> httpx.AsyncClient:
    return client_for(app)


def _access_url(app: FastAPI) -> str:
    return app.url_path_for("create_resource_access_endpoint")


def _search_access_url(app: FastAPI) -> str:
    return app.url_path_for("search_resource_access_endpoint")


def _similar_access_url(app: FastAPI) -> str:
    return app.url_path_for("similar_resource_access_endpoint")


VALID_ACCESS = {"username": "jdoe", "resource_id": 7, "predicate": "¿Qué campos tiene el topic de pedidos?"}


def _dependency_calls(dependant) -> set:
    calls = set()
    for sub in dependant.dependencies:
        calls.add(sub.call)
        calls |= _dependency_calls(sub)
    return calls


def test_default_routes():
    app = build_test_app()
    assert _access_url(app) == "/v1/resources/access"
    assert _search_access_url(app) == "/v1/resources/access"
    assert _similar_access_url(app) == "/v1/resources/access/similar"


def test_access_endpoints_resolve_the_service_repository_and_embedding_client_but_not_kafka(app: FastAPI):
    routes = [inner for r in app.routes for inner in getattr(getattr(r, "original_router", None), "routes", [r])]
    for name in ("create_resource_access_endpoint", "search_resource_access_endpoint", "similar_resource_access_endpoint"):
        route = next(r for r in routes if getattr(r, "name", None) == name)
        calls = _dependency_calls(route.dependant)
        assert {dependencies.get_resource_access_service, dependencies.get_resource_access_port_out,
                dependencies.get_embedding_client, dependencies.get_db_session} <= calls
        assert calls.isdisjoint({dependencies.get_event_outbox_port_out, dependencies.get_reference_command_port_out,
                                 dependencies.get_create_message_resource})


# =======================================================================
# POST /access
# =======================================================================

@pytest.mark.asyncio
async def test_post_access_registers_through_the_service_and_reports_success(app, mock_service):
    async with _client(app) as client:
        response = await client.post(_access_url(app), json=VALID_ACCESS)

    assert response.status_code == 201
    assert response.json() == {"success": True, "message": "Acceso al recurso registrado.",
                               "data": {**VALID_ACCESS, "model_name": FAKE_MODEL_NAME}}  # no vector in the response
    (request,), _ = mock_service.register_access.await_args
    assert request.model_dump() == VALID_ACCESS


@pytest.mark.asyncio
async def test_post_access_trims_text_fields(app, mock_service):
    async with _client(app) as client:
        response = await client.post(_access_url(app), json={**VALID_ACCESS, "username": "  jdoe ", "predicate": " hola "})

    assert response.status_code == 201
    assert response.json()["data"] == {**VALID_ACCESS, "username": "jdoe", "predicate": "hola", "model_name": FAKE_MODEL_NAME}


@pytest.mark.asyncio
@pytest.mark.parametrize("error, status_code, message, detail", [
    (ValidationException("username: too long"), 400, "Acceso no válido; no se ha registrado.", "username: too long"),
    (NotFoundException("El recurso 7 no está registrado."), 404, "Recurso no registrado; no se ha registrado el acceso.",
     "El recurso 7 no está registrado."),
    (EmbeddingException("voyage/voyage-3.5: timeout"), 503, "Modelo de IA no disponible; no se ha podido registrar el acceso.",
     "voyage/voyage-3.5: timeout"),
    (PersistenceException("db down"), 500, "No se ha podido registrar el acceso.", "Error de persistencia"),
])
async def test_post_access_reports_service_failures(app, mock_service, error, status_code, message, detail):
    mock_service.register_access.side_effect = error

    async with _client(app) as client:
        response = await client.post(_access_url(app), json=VALID_ACCESS)

    assert response.status_code == status_code
    assert response.json() == {"detail": {"success": False, "message": message, "detalle": detail}}


@pytest.mark.asyncio
@pytest.mark.parametrize("body, bad_field", [
    ({k: v for k, v in VALID_ACCESS.items() if k != "username"}, "username"),        # missing
    ({k: v for k, v in VALID_ACCESS.items() if k != "resource_id"}, "resource_id"),  # missing
    ({k: v for k, v in VALID_ACCESS.items() if k != "predicate"}, "predicate"),      # missing
    ({**VALID_ACCESS, "username": ""}, "username"),                                  # empty
    ({**VALID_ACCESS, "username": "   "}, "username"),                               # blank
    ({**VALID_ACCESS, "username": "x" * 151}, "username"),                           # too long
    ({**VALID_ACCESS, "username": 123}, "username"),                                 # not a string
    ({**VALID_ACCESS, "resource_id": "7"}, "resource_id"),                           # string, not int
    ({**VALID_ACCESS, "resource_id": 7.5}, "resource_id"),                           # not an int
    ({**VALID_ACCESS, "resource_id": True}, "resource_id"),                          # bool is not an id
    ({**VALID_ACCESS, "resource_id": 0}, "resource_id"),                             # ids start at 1
    ({**VALID_ACCESS, "resource_id": None}, "resource_id"),                          # null
    ({**VALID_ACCESS, "predicate": ""}, "predicate"),                                # empty
    ({**VALID_ACCESS, "predicate": ["a"]}, "predicate"),                             # not a string
    ({**VALID_ACCESS, "unexpected": "x"}, "unexpected"),                             # unknown field
])
async def test_post_access_rejects_invalid_bodies(app, mock_service, body, bad_field):
    async with _client(app) as client:
        response = await client.post(_access_url(app), json=body)

    assert response.status_code == 422
    assert [error["loc"][-1] for error in response.json()["detail"]] == [bad_field]
    mock_service.register_access.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ["", "not json", "[]", '"jdoe"'])
async def test_post_access_rejects_bodies_that_are_not_a_json_object(app, mock_service, content):
    async with _client(app) as client:
        response = await client.post(_access_url(app), content=content, headers={"Content-Type": "application/json"})

    assert response.status_code == 422
    mock_service.register_access.assert_not_awaited()


# =======================================================================
# GET /access?username=&resource=
# =======================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("params, expected_call", [
    ({"username": "jdoe"}, {"username": "jdoe", "resource_id": None}),
    ({"resource": "7"}, {"username": None, "resource_id": 7}),
    ({"username": "jdoe", "resource": "7"}, {"username": "jdoe", "resource_id": 7}),
])
async def test_get_access_forwards_username_and_or_resource(app, mock_service, params, expected_call):
    mock_service.search_accesses.return_value = [ResourceAccess(**VALID_ACCESS)]

    async with _client(app) as client:
        response = await client.get(_search_access_url(app), params=params)

    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "1 acceso(s) encontrado(s).",
                               "results": [{**VALID_ACCESS, "model_name": None}]}
    mock_service.search_accesses.assert_awaited_once_with(**expected_call)


@pytest.mark.asyncio
async def test_get_access_without_username_nor_resource_is_a_400(app, mock_service):
    mock_service.search_accesses.side_effect = ValidationException("Indique al menos username o resource.")

    async with _client(app) as client:
        response = await client.get(_search_access_url(app))

    assert response.status_code == 400
    assert response.json()["detail"] == {"success": False, "message": "Parámetros de consulta no válidos.",
                                         "detalle": "Indique al menos username o resource."}


@pytest.mark.asyncio
@pytest.mark.parametrize("params, bad_param", [
    ({"username": ""}, "username"),
    ({"username": "x" * 151}, "username"),
    ({"resource": "abc"}, "resource"),
    ({"resource": "1.5"}, "resource"),
    ({"resource": "0"}, "resource"),
])
async def test_get_access_rejects_invalid_parameters(app, mock_service, params, bad_param):
    async with _client(app) as client:
        response = await client.get(_search_access_url(app), params=params)

    assert response.status_code == 422
    assert [error["loc"][-1] for error in response.json()["detail"]] == [bad_param]
    mock_service.search_accesses.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_access_reports_persistence_failures(app, mock_service):
    mock_service.search_accesses.side_effect = PersistenceException("db down")

    async with _client(app) as client:
        response = await client.get(_search_access_url(app), params={"username": "jdoe"})

    assert response.status_code == 500
    assert response.json()["detail"]["success"] is False


# =======================================================================
# GET /access/similar?predicate=&resource=
# =======================================================================

@pytest.mark.asyncio
@pytest.mark.parametrize("params, expected_resource", [({"predicate": "campos"}, None), ({"predicate": "campos", "resource": "7"}, 7)])
async def test_get_similar_forwards_the_text_and_optional_resource(app, mock_service, params, expected_resource):
    mock_service.search_similar_accesses.return_value = [
        ResourceAccessMatch(access=ResourceAccess(**VALID_ACCESS, predicate_embed=[0.1], model_name=FAKE_MODEL_NAME), similarity=0.91)]

    async with _client(app) as client:
        response = await client.get(_similar_access_url(app), params=params)

    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "1 acceso(s) similar(es) encontrado(s).",
                               "results": [{**VALID_ACCESS, "model_name": FAKE_MODEL_NAME, "similarity": 0.91}]}
    mock_service.search_similar_accesses.assert_awaited_once_with("campos", resource_id=expected_resource)


@pytest.mark.asyncio
@pytest.mark.parametrize("params, bad_param", [({}, "predicate"), ({"predicate": ""}, "predicate"),
                                               ({"predicate": "p", "resource": "0"}, "resource")])
async def test_get_similar_rejects_invalid_parameters(app, mock_service, params, bad_param):
    async with _client(app) as client:
        response = await client.get(_similar_access_url(app), params=params)

    assert response.status_code == 422
    assert [error["loc"][-1] for error in response.json()["detail"]] == [bad_param]
    mock_service.search_similar_accesses.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("error, status_code", [(ValidationException("predicate no puede estar vacío."), 400),
                                                (EmbeddingException("voyage/voyage-3.5: HTTP 401"), 503),
                                                (PersistenceException("db down"), 500)])
async def test_get_similar_reports_service_failures(app, mock_service, error, status_code):
    mock_service.search_similar_accesses.side_effect = error

    async with _client(app) as client:
        response = await client.get(_similar_access_url(app), params={"predicate": "campos"})

    assert response.status_code == status_code
    assert response.json()["detail"]["success"] is False


# =======================================================================
# END-TO-END: real wiring and repository, PostgreSQL/pgvector Testcontainer, fake AI model
# =======================================================================

QUERY = "¿Qué campos tiene el topic?"
E2E_VECTORS = {QUERY: [1.0, 0.0], "¿Qué campos tiene el topic de pedidos?": [0.8, 0.6], "otra cosa": [0.0, 1.0]}


@pytest_asyncio.fixture
async def real_wiring(app: FastAPI, session_connect) -> AsyncGenerator[FakeEmbeddingClient]:
    """Real DI graph; only the DB session (rolled back), its unit of work and the AI model are replaced."""
    embeddings = FakeEmbeddingClient(vectors=E2E_VECTORS)

    async def _test_session():
        yield session_connect

    app.dependency_overrides[get_db_session] = _test_session
    app.dependency_overrides[get_unit_of_work] = lambda: SqlAlchemyUnitOfWork(session_connect)  # no outbox relay without lifespan
    app.dependency_overrides[get_embedding_client] = lambda: embeddings
    yield embeddings


@pytest.mark.asyncio
async def test_register_then_query_by_user_resource_and_similarity(app, real_wiring, session_connect):
    resource_id = (await ReferenceRepository(session_connect).save_data_dictionary_reference(create_random_data_resources())).dictionary_id
    first = {"username": "jdoe_e2e", "resource_id": resource_id, "predicate": "¿Qué campos tiene el topic de pedidos?"}
    second = {"username": "ana_e2e", "resource_id": resource_id, "predicate": "otra cosa"}

    async with _client(app) as client:
        created = [await client.post(_access_url(app), json=body) for body in (first, second)]
        by_user = await client.get(_search_access_url(app), params={"username": "jdoe_e2e"})
        by_resource = await client.get(_search_access_url(app), params={"resource": resource_id})
        by_both = await client.get(_search_access_url(app), params={"username": "ana_e2e", "resource": resource_id})
        missing_filters = await client.get(_search_access_url(app))
        similar = await client.get(_similar_access_url(app), params={"predicate": QUERY, "resource": resource_id})

    assert [r.status_code for r in created] == [201, 201]
    assert created[0].json()["data"] == {**first, "model_name": FAKE_MODEL_NAME}

    assert [r["username"] for r in by_user.json()["results"]] == ["jdoe_e2e"]
    assert [r["username"] for r in by_resource.json()["results"]] == ["ana_e2e", "jdoe_e2e"]  # newest first
    assert [r["username"] for r in by_both.json()["results"]] == ["ana_e2e"]
    assert missing_filters.status_code == 400

    # configured defaults: limit 10, min similarity 0.75 -> "otra cosa" (similarity 0) is left out
    assert similar.status_code == 200
    (match,) = similar.json()["results"]
    assert (match["username"], match["model_name"]) == ("jdoe_e2e", FAKE_MODEL_NAME)
    assert match["similarity"] == pytest.approx(0.8, abs=1e-6)


@pytest.mark.asyncio
async def test_unknown_resource_is_a_404_and_ai_model_down_is_a_503_with_nothing_stored(app, real_wiring, session_connect):
    resource_id = (await ReferenceRepository(session_connect).save_data_dictionary_reference(create_random_data_resources())).dictionary_id

    async with _client(app) as client:
        unknown = await client.post(_access_url(app), json={**VALID_ACCESS, "resource_id": resource_id + 1000})
        real_wiring.fail_with = EmbeddingException(f"{FAKE_MODEL_NAME}: sin respuesta del modelo (timeout).")
        model_down = await client.post(_access_url(app), json={**VALID_ACCESS, "resource_id": resource_id})
        similar_down = await client.get(_similar_access_url(app), params={"predicate": QUERY})
        real_wiring.fail_with = None
        stored = await client.get(_search_access_url(app), params={"resource": resource_id})

    assert unknown.status_code == 404
    assert (model_down.status_code, model_down.json()["detail"]["detalle"]) == (503, f"{FAKE_MODEL_NAME}: sin respuesta del modelo (timeout).")
    assert similar_down.status_code == 503
    assert stored.json()["results"] == []
