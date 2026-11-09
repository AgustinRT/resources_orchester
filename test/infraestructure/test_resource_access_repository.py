"""
ResourceAccessRepository on the PostgreSQL (pgvector) Testcontainer, with the AI model replaced by
FakeEmbeddingClient (tests choose each vector, so the similarities are known exactly).
Each test runs in a rolled-back session.
"""
import math
import uuid
from typing import List

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.domain.exception.domain_exceptions import NotFoundException
from resources_orchester.domain.exception.infrastructure_exceptions import EmbeddingException
from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.infrastructure.persistence.entities.resource_user_access import ResourceUserAccess
from resources_orchester.infrastructure.persistence.repositories.reference_repository import ReferenceRepository
from resources_orchester.infrastructure.persistence.repositories.resource_access_repository import ResourceAccessRepository

from test.infraestructure.fake_embedding_client import FAKE_MODEL_NAME, FakeEmbeddingClient
from test.infraestructure.utils_tester import create_random_data_resources

QUERY = "¿Qué campos tiene el topic?"
# Cosine similarity of each vector with the QUERY vector [1, 0, 0] is its first component (unit vectors).
VECTORS = {
    QUERY: [1.0, 0.0, 0.0],
    "identical": [1.0, 0.0, 0.0],                        # 1.0
    "very close": [0.96, 0.28, 0.0],                     # 0.96
    "close": [0.8, 0.6, 0.0],                            # 0.8   (>= 0.75)
    "just below": [0.7, math.sqrt(1 - 0.49), 0.0],       # 0.7   (<  0.75)
    "unrelated": [0.0, 1.0, 0.0],                        # 0.0
}


async def _new_resource(session: AsyncSession) -> int:
    return (await ReferenceRepository(session).save_data_dictionary_reference(create_random_data_resources())).dictionary_id


@pytest_asyncio.fixture
async def resource_id(session_connect) -> int:
    return await _new_resource(session_connect)


@pytest.fixture
def embeddings() -> FakeEmbeddingClient:
    return FakeEmbeddingClient(vectors=VECTORS)


@pytest.fixture
def repository(session_connect, embeddings) -> ResourceAccessRepository:
    return ResourceAccessRepository(session_connect, embeddings)


def _access(resource_id: int, predicate: str = "identical", username: str = "jdoe") -> ResourceAccess:
    return ResourceAccess(username=username, resource_id=resource_id, predicate=predicate)


async def _rows(session: AsyncSession, resource_id: int) -> int:
    return (await session.execute(select(func.count()).select_from(ResourceUserAccess)
                                  .where(ResourceUserAccess.resource_id == resource_id))).scalar_one()


# --- save_access ----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_stores_the_access_with_its_embedding_and_model(repository, session_connect, embeddings, resource_id):
    stored = await repository.save_access(_access(resource_id, "close"))

    assert (stored.username, stored.resource_id, stored.predicate, stored.model_name) == ("jdoe", resource_id, "close", FAKE_MODEL_NAME)
    assert stored.predicate_embed == VECTORS["close"]
    assert embeddings.calls == ["close"]

    row = (await session_connect.execute(select(ResourceUserAccess).where(ResourceUserAccess.resource_id == resource_id))).scalar_one()
    assert row.id is not None and row.model_name == FAKE_MODEL_NAME
    assert row.predicate_embed == pytest.approx(VECTORS["close"])  # pgvector stores float32


@pytest.mark.asyncio
async def test_the_same_user_can_register_many_accesses(repository, session_connect, resource_id):
    for _ in range(3):
        await repository.save_access(_access(resource_id))
    assert await _rows(session_connect, resource_id) == 3


@pytest.mark.asyncio
async def test_nothing_is_stored_when_the_embedding_fails(session_connect, resource_id):
    failing = FakeEmbeddingClient(fail_with=EmbeddingException("fake: down"))

    with pytest.raises(EmbeddingException):
        await ResourceAccessRepository(session_connect, failing).save_access(_access(resource_id))

    assert await _rows(session_connect, resource_id) == 0


@pytest.mark.asyncio
async def test_unknown_resource_raises_not_found(repository, session_connect):
    missing = (await session_connect.execute(text("SELECT coalesce(max(dictionary_id), 0) + 1000 FROM data_dict_registry"))).scalar_one()

    with pytest.raises(NotFoundException, match=str(missing)):
        await repository.save_access(_access(missing))


# --- find_accesses (username / resource, exact match) ---------------------------------------

@pytest.mark.asyncio
async def test_find_by_username_resource_or_both_newest_first(repository, session_connect):
    first_resource, second_resource = await _new_resource(session_connect), await _new_resource(session_connect)
    user, other = f"user_{uuid.uuid4().hex[:8]}", f"other_{uuid.uuid4().hex[:8]}"
    for resource, username, predicate in [(first_resource, user, "a"), (second_resource, user, "b"),
                                          (first_resource, other, "c"), (first_resource, user, "d")]:
        await repository.save_access(_access(resource, predicate, username))

    def summary(accesses: List[ResourceAccess]):
        return [(a.username, a.resource_id, a.predicate) for a in accesses]

    assert summary(await repository.find_accesses(user, None, 10)) == [
        (user, first_resource, "d"), (user, second_resource, "b"), (user, first_resource, "a")]
    assert summary(await repository.find_accesses(None, first_resource, 10)) == [
        (user, first_resource, "d"), (other, first_resource, "c"), (user, first_resource, "a")]
    assert summary(await repository.find_accesses(user, first_resource, 10)) == [
        (user, first_resource, "d"), (user, first_resource, "a")]
    assert summary(await repository.find_accesses(user, first_resource, 1)) == [(user, first_resource, "d")]


@pytest.mark.asyncio
async def test_find_needs_an_exact_match(repository, resource_id):
    await repository.save_access(_access(resource_id, username="jdoe"))

    for near_miss in ("JDOE", "jdo", "jdoe ", "%", "jdoe%"):
        assert await repository.find_accesses(near_miss, None, 10) == []
    assert len(await repository.find_accesses("jdoe", resource_id, 10)) == 1


@pytest.mark.asyncio
async def test_find_without_filters_is_refused(repository):
    with pytest.raises(ValueError):
        await repository.find_accesses(None, None, 10)


@pytest.mark.asyncio
async def test_find_returns_the_stored_embedding(repository, resource_id):
    await repository.save_access(_access(resource_id, "close"))

    (found,) = await repository.find_accesses(None, resource_id, 10)
    assert found.model_name == FAKE_MODEL_NAME and found.predicate_embed == pytest.approx(VECTORS["close"])


# --- find_similar_accesses ------------------------------------------------------------------

async def _seed(repository: ResourceAccessRepository, resource_id: int) -> None:
    for predicate in ("unrelated", "close", "just below", "identical", "very close"):
        await repository.save_access(_access(resource_id, predicate))


@pytest.mark.asyncio
async def test_similar_returns_only_matches_above_the_minimum_most_similar_first(repository, resource_id):
    await _seed(repository, resource_id)

    matches = await repository.find_similar_accesses(QUERY, resource_id, 10, 0.75)

    assert [m.access.predicate for m in matches] == ["identical", "very close", "close"]
    assert [m.similarity for m in matches] == pytest.approx([1.0, 0.96, 0.8], abs=1e-6)


@pytest.mark.asyncio
async def test_similar_respects_the_limit_and_minimum_it_receives(repository, resource_id):
    await _seed(repository, resource_id)

    assert [m.access.predicate for m in await repository.find_similar_accesses(QUERY, resource_id, 2, 0.75)] == ["identical", "very close"]
    assert [m.access.predicate for m in await repository.find_similar_accesses(QUERY, resource_id, 10, 0.95)] == ["identical", "very close"]
    assert [m.access.predicate for m in await repository.find_similar_accesses(QUERY, resource_id, 10, 0.0)] == [
        "identical", "very close", "close", "just below", "unrelated"]


@pytest.mark.asyncio
async def test_similar_can_search_every_resource_or_only_one(repository, session_connect, resource_id):
    other_resource = await _new_resource(session_connect)
    await repository.save_access(_access(resource_id, "identical"))
    await repository.save_access(_access(other_resource, "close"))

    only_one = await repository.find_similar_accesses(QUERY, resource_id, 10, 0.75)
    every = await repository.find_similar_accesses(QUERY, None, 100, 0.75)

    assert [(m.access.resource_id, m.access.predicate) for m in only_one] == [(resource_id, "identical")]
    assert {(resource_id, "identical"), (other_resource, "close")} <= {(m.access.resource_id, m.access.predicate) for m in every}


@pytest.mark.asyncio
async def test_similar_only_compares_vectors_of_the_current_model(session_connect, repository, resource_id):
    """Rows of another model, of another size with the same model name, or without embedding are skipped (no error)."""
    other_model = FakeEmbeddingClient(vectors=VECTORS, model_name="other/model")
    await ResourceAccessRepository(session_connect, other_model).save_access(_access(resource_id, "identical"))
    await session_connect.execute(text(
        """INSERT INTO resource_user_access (username, resource_id, predicate, predicate_embed, model_name)
           VALUES ('jdoe', :r, 'two dims', '[1, 0]', :m), ('jdoe', :r, 'no embedding', NULL, NULL)"""),
        {"r": resource_id, "m": FAKE_MODEL_NAME})
    await repository.save_access(_access(resource_id, "close"))

    matches = await repository.find_similar_accesses(QUERY, resource_id, 10, 0.0)

    assert [(m.access.predicate, m.access.model_name) for m in matches] == [("close", FAKE_MODEL_NAME)]


@pytest.mark.asyncio
async def test_similar_search_fails_when_the_query_cannot_be_embedded(session_connect, resource_id):
    failing = FakeEmbeddingClient(fail_with=EmbeddingException("fake: down"))

    with pytest.raises(EmbeddingException):
        await ResourceAccessRepository(session_connect, failing).find_similar_accesses(QUERY, resource_id, 10, 0.75)
