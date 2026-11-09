"""
Unit tests of the ManageResourceAccess service with its ports faked: the storage port is a
MagicMock(spec=<Protocol>) (same convention as test_use_cases.py) and the unit of work records
its calls in the same list, so the ORDER (save before commit, no commit after a failure) is checked.
"""
from typing import List
from unittest.mock import MagicMock

import pytest

from resources_orchester.application.ports.outbound.resource_access_port_out import ResourceAccessPortOut
from resources_orchester.application.use_cases.manage_resource_access import ManageResourceAccess
from resources_orchester.domain.api_dtos.resource_access_dto import ResourceAccessRequestDTO
from resources_orchester.domain.exception.domain_exceptions import NotFoundException, ValidationException
from resources_orchester.domain.exception.infrastructure_exceptions import EmbeddingException, PersistenceException
from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.domain.model.resource_access_match import ResourceAccessMatch

MODEL = "fake/model"


class RecordingUnitOfWork:
    def __init__(self, calls: List[str]):
        self.calls = calls

    async def __aenter__(self):
        self.calls.append("uow.begin")
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.calls.append("uow.end")

    async def commit(self):
        self.calls.append("uow.commit")

    async def rollback(self):
        self.calls.append("uow.rollback")


def _embedded(access: ResourceAccess) -> ResourceAccess:
    """What the storage adapter returns: the access with its embedding."""
    return access.model_copy(update={"predicate_embed": [0.1, 0.2], "model_name": MODEL})


@pytest.fixture
def calls() -> List[str]:
    return []


@pytest.fixture
def port_out(calls) -> MagicMock:
    port = MagicMock(spec=ResourceAccessPortOut)

    async def save(access):
        calls.append("port.save_access")
        return _embedded(access)
    port.save_access.side_effect = save
    port.find_accesses.return_value = []
    port.find_similar_accesses.return_value = []
    return port


def _service(port_out, calls, list_limit=10, similarity_limit=10, min_similarity=0.75) -> ManageResourceAccess:
    return ManageResourceAccess(unit_of_work=RecordingUnitOfWork(calls), resource_access_port_out=port_out,
                                list_limit=list_limit, similarity_limit=similarity_limit, min_similarity=min_similarity)


@pytest.fixture
def service(port_out, calls) -> ManageResourceAccess:
    return _service(port_out, calls)


def _request(**changes) -> ResourceAccessRequestDTO:
    return ResourceAccessRequestDTO(**{"username": "jdoe", "resource_id": 7, "predicate": "¿Qué campos tiene?", **changes})


# --- register_access ------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_builds_the_model_stores_it_and_commits(service, port_out, calls):
    stored = await service.register_access(_request())

    expected = ResourceAccess(username="jdoe", resource_id=7, predicate="¿Qué campos tiene?")
    port_out.save_access.assert_awaited_once_with(expected)
    assert stored == _embedded(expected)  # returned as stored: with embedding and model
    assert calls == ["uow.begin", "port.save_access", "uow.commit", "uow.end"]


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [EmbeddingException("fake/model: timeout"), NotFoundException("recurso 7"),
                                   PersistenceException("db down")])
async def test_register_failures_propagate_and_nothing_is_committed(service, port_out, calls, error):
    port_out.save_access.side_effect = error

    with pytest.raises(type(error)):
        await service.register_access(_request())

    assert "uow.commit" not in calls
    assert calls[-1] == "uow.end"  # the unit of work is left (rolls back) even on failure


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_values, field", [
    ({"username": "   "}, "username"),
    ({"predicate": ""}, "predicate"),
    ({"resource_id": 0}, "resource_id"),
])
async def test_register_rejects_data_that_breaks_the_domain_rules(service, port_out, calls, bad_values, field):
    """model_construct skips the DTO validation (a non-HTTP caller): the service validates the domain rules itself."""
    request = ResourceAccessRequestDTO.model_construct(**{"username": "jdoe", "resource_id": 7, "predicate": "p", **bad_values})

    with pytest.raises(ValidationException) as error:
        await service.register_access(request)

    assert error.value.message.startswith(f"{field}:")
    port_out.save_access.assert_not_awaited()
    assert calls == []  # no transaction opened


# --- search_accesses (username / resource) --------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("username, resource_id", [("jdoe", None), (None, 7), ("jdoe", 7)])
async def test_search_accepts_username_resource_or_both_with_the_configured_limit(port_out, calls, username, resource_id):
    found = [ResourceAccess(username="jdoe", resource_id=7, predicate="p")]
    port_out.find_accesses.return_value = found

    result = await _service(port_out, calls, list_limit=25).search_accesses(username=username, resource_id=resource_id)

    assert result == found
    port_out.find_accesses.assert_awaited_once_with(username, resource_id, 25)


@pytest.mark.asyncio
async def test_search_limit_can_be_overridden_per_call(service, port_out):
    await service.search_accesses(username="jdoe", limit=3)
    port_out.find_accesses.assert_awaited_once_with("jdoe", None, 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs, message", [
    ({}, "al menos username o resource"),
    ({"username": None, "resource_id": None}, "al menos username o resource"),
    ({"username": "  "}, "username"),
    ({"username": ""}, "username"),
    ({"resource_id": 0}, "resource"),
    ({"resource_id": True}, "resource"),
    ({"username": "jdoe", "limit": 0}, "limit"),
])
async def test_search_rejects_invalid_parameters(service, port_out, kwargs, message):
    with pytest.raises(ValidationException, match=message):
        await service.search_accesses(**kwargs)
    port_out.find_accesses.assert_not_awaited()


# --- search_similar_accesses ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_similar_search_uses_the_configured_limit_and_minimum_similarity(port_out, calls):
    matches = [ResourceAccessMatch(access=ResourceAccess(username="jdoe", resource_id=7, predicate="p"), similarity=0.9)]
    port_out.find_similar_accesses.return_value = matches

    result = await _service(port_out, calls, similarity_limit=4, min_similarity=0.6).search_similar_accesses("campos", resource_id=7)

    assert result == matches
    port_out.find_similar_accesses.assert_awaited_once_with("campos", 7, 4, 0.6)


@pytest.mark.asyncio
async def test_similar_search_values_can_be_overridden_per_call(service, port_out):
    await service.search_similar_accesses("campos", limit=2, min_similarity=0.5)
    port_out.find_similar_accesses.assert_awaited_once_with("campos", None, 2, 0.5)


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs, message", [
    ({"predicate": ""}, "predicate"),
    ({"predicate": "   "}, "predicate"),
    ({"predicate": "p", "resource_id": -1}, "resource"),
    ({"predicate": "p", "limit": 0}, "limit"),
    ({"predicate": "p", "min_similarity": 1.1}, "min_similarity"),
    ({"predicate": "p", "min_similarity": float("nan")}, "min_similarity"),
])
async def test_similar_search_rejects_invalid_parameters(service, port_out, kwargs, message):
    with pytest.raises(ValidationException, match=message):
        await service.search_similar_accesses(**kwargs)
    port_out.find_similar_accesses.assert_not_awaited()


@pytest.mark.asyncio
async def test_similar_search_propagates_embedding_failures(service, port_out):
    port_out.find_similar_accesses.side_effect = EmbeddingException("fake/model: down")

    with pytest.raises(EmbeddingException):
        await service.search_similar_accesses("campos")
