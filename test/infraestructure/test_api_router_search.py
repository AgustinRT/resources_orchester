"""
Tests for GET {API_PREFIX}/search (search persisted resources by name and type).

1. HTTP contract tests: the application service is replaced through FastAPI's
   dependency_overrides, so only query-param parsing/validation and error mapping
   are exercised.
2. End-to-end tests over Testcontainers: the real application (create_app) and the
   real DI chain (router -> SearchResources -> ResourceQueryPortOut ->
   ReferenceRepository -> PostgreSQL). Only the DB session provider (pointed at the
   rolled-back test session) is overridden.

httpx.AsyncClient + ASGITransport is used instead of TestClient so the app runs on
the same event loop as the async SQLAlchemy session. ASGITransport does not run the
lifespan, so the Kafka consumer is not started.
"""
import uuid
from typing import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.exception.domain_exceptions import ValidationException
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.bootstrap import dependencies
from resources_orchester.infrastructure.bootstrap.dependencies import get_db_session, get_search_resources
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut

from test.infraestructure.app_tester import build_test_app, client_for
from test.infraestructure.utils_tester import create_random_data_resources


@pytest.fixture
def app() -> FastAPI:
    test_app = build_test_app()
    yield test_app
    test_app.dependency_overrides.clear()


def _client(app: FastAPI):
    return client_for(app)


def _search_url(app: FastAPI) -> str:
    return app.url_path_for("search_resources_endpoint")


# =======================================================================
# HTTP CONTRACT (service mocked)
# =======================================================================

@pytest.fixture
def mock_service(app: FastAPI) -> AsyncMock:
    service = AsyncMock()
    app.dependency_overrides[get_search_resources] = lambda: service
    return service


def _resource(name: str) -> DataResources:
    return DataResources(name=name, description={"es_ES": "d"}, competence="database",
                         process_apply=None, type_data="dbRow", fields=[])


@pytest.mark.asyncio
async def test_search_endpoint_returns_resources_and_forwards_name_and_type(app, mock_service: AsyncMock):
    mock_service.search_resources_by_name.return_value = [_resource("orders_table"), _resource("orders_archive")]

    async with _client(app) as client:
        response = await client.get(_search_url(app), params={"name": "orders", "type": "DB_TABLE"})

    assert response.status_code == 200
    assert [r["name"] for r in response.json()] == ["orders_table", "orders_archive"]
    mock_service.search_resources_by_name.assert_awaited_once_with("orders", DataUse.DB_TABLE)


@pytest.mark.asyncio
async def test_search_endpoint_type_is_optional(app, mock_service: AsyncMock):
    mock_service.search_resources_by_name.return_value = []

    async with _client(app) as client:
        response = await client.get(_search_url(app), params={"name": "orders"})

    assert response.status_code == 200
    assert response.json() == []
    mock_service.search_resources_by_name.assert_awaited_once_with("orders", None)


@pytest.mark.asyncio
@pytest.mark.parametrize("params", [
    {},                                   # name is mandatory
    {"name": ""},                         # min_length=1
    {"name": "x" * 151},                  # longer than the column (VARCHAR(150))
    {"name": "orders", "type": "TABLE"},  # not a DataUse value
])
async def test_search_endpoint_rejects_invalid_query_params(app, mock_service: AsyncMock, params):
    async with _client(app) as client:
        response = await client.get(_search_url(app), params=params)

    assert response.status_code == 422
    mock_service.search_resources_by_name.assert_not_awaited()


@pytest.mark.asyncio
async def test_search_endpoint_maps_validation_errors_to_400(app, mock_service: AsyncMock):
    mock_service.search_resources_by_name.side_effect = ValidationException("El nombre a buscar no puede estar vacío.")

    async with _client(app) as client:
        response = await client.get(_search_url(app), params={"name": "   "})

    assert response.status_code == 400
    assert response.json()["detail"]["detalle"] == "El nombre a buscar no puede estar vacío."


# =======================================================================
# END-TO-END over Testcontainers (real wiring, real PostgreSQL)
# =======================================================================

@pytest_asyncio.fixture
async def real_wiring(app: FastAPI, session_connect) -> AsyncGenerator[None]:
    """Real DI graph; only the DB session provider points at the rolled-back test session."""
    async def _test_session():
        yield session_connect

    app.dependency_overrides[get_db_session] = _test_session
    yield


def _dependency_calls(dependant) -> set:
    calls = set()
    for sub in dependant.dependencies:
        calls.add(sub.call)
        calls |= _dependency_calls(sub)
    return calls


def test_search_endpoint_only_resolves_what_the_search_needs(app: FastAPI):
    """
    Review D9: the read-only endpoint depends on the search use case and its query port only:
    no unit of work, outbox, command ports or Kafka are built for a GET.
    """
    # FastAPI 0.142 wraps included routers (_IncludedRouter); the APIRoutes live in original_router.
    routes = [inner for r in app.routes for inner in getattr(getattr(r, "original_router", None), "routes", [r])]
    route = next(r for r in routes if getattr(r, "name", None) == "search_resources_endpoint")
    calls = _dependency_calls(route.dependant)

    assert dependencies.get_search_resources in calls and dependencies.get_resource_query_port_out in calls
    assert calls.isdisjoint({dependencies.get_unit_of_work, dependencies.get_event_outbox_port_out,
                             dependencies.get_reference_command_port_out, dependencies.get_application_query_port_out,
                             dependencies.get_create_message_resource})


@pytest.mark.asyncio
async def test_search_endpoint_end_to_end(app, real_wiring, reference_command_port_out: ReferenceCommandPortOut):
    token = uuid.uuid4().hex
    seeded = {}
    for name, competence in [(f"shipment_{token}_event", "messaging"),
                             (f"shipment_{token}_table", "database"),
                             (f"unrelated_{uuid.uuid4().hex}", "database")]:
        resource = create_random_data_resources()
        resource.name, resource.competence = name, competence
        await reference_command_port_out.save_data_dictionary_reference(resource)
        seeded[name] = resource

    async with _client(app) as client:
        by_name = await client.get(_search_url(app), params={"name": f"SHIPMENT_{token}"})
        by_name_and_type = await client.get(_search_url(app), params={"name": token, "type": "DB_TABLE"})
        no_match = await client.get(_search_url(app), params={"name": f"missing_{uuid.uuid4().hex}"})

    assert by_name.status_code == 200
    assert [r["name"] for r in by_name.json()] == [f"shipment_{token}_event", f"shipment_{token}_table"]

    assert by_name_and_type.status_code == 200
    body = by_name_and_type.json()
    assert len(body) == 1
    expected = seeded[f"shipment_{token}_table"]
    assert body[0]["name"] == expected.name
    assert body[0]["competence"] == "database"
    assert body[0]["type_data"] == "dbRow"
    assert body[0]["description"] == expected.description
    assert body[0]["fields"] == [f.model_dump() for f in expected.fields]

    assert no_match.status_code == 200
    assert no_match.json() == []

