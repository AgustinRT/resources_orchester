"""
Schema of the ``resource_user_access`` vector table (init.sql, pgvector) on the PostgreSQL Testcontainer.
Each test runs in a rolled-back session; expected failures run inside a SAVEPOINT so the test can go on.
"""
import contextlib

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

INSERT = text("""INSERT INTO resource_user_access (username, resource_id, predicate, predicate_embed, model_name)
                 VALUES (:username, :resource_id, :predicate, CAST(:embed AS vector), :model_name) RETURNING id""")


async def _new_resource(session: AsyncSession) -> int:
    return (await session.execute(text(
        """INSERT INTO data_dict_registry (name_data, data_use_dictionay, description_data)
           VALUES ('access_test_resource', 'ASYNC_MESSAGE', '{}'::jsonb) RETURNING dictionary_id"""))).scalar_one()


async def _insert(session: AsyncSession, resource_id: int, username="jdoe", predicate="¿Qué campos tiene?",
                  embed=None, model_name=None) -> int:
    return (await session.execute(INSERT, {"username": username, "resource_id": resource_id, "predicate": predicate,
                                           "embed": embed, "model_name": model_name})).scalar_one()


@contextlib.asynccontextmanager
async def _rejected(session: AsyncSession, sqlstate: str):
    """The block must fail with ``sqlstate``; yields the captured error (pytest.ExceptionInfo)."""
    with pytest.raises(IntegrityError) as error:
        async with session.begin_nested():
            yield error
    assert error.value.orig.sqlstate == sqlstate


@pytest.mark.asyncio
async def test_pgvector_extension_is_installed(session_connect: AsyncSession):
    assert (await session_connect.execute(text("SELECT count(*) FROM pg_extension WHERE extname = 'vector'"))).scalar_one() == 1


@pytest.mark.asyncio
async def test_columns_and_id_sequence(session_connect: AsyncSession):
    rows = (await session_connect.execute(text(
        """SELECT column_name, data_type, udt_name, is_nullable, column_default
           FROM information_schema.columns WHERE table_name = 'resource_user_access' ORDER BY ordinal_position"""))).all()

    assert [(r.column_name, r.udt_name, r.is_nullable) for r in rows] == [
        ("id", "int8", "NO"),
        ("username", "varchar", "NO"),
        ("resource_id", "int4", "NO"),
        ("predicate", "text", "NO"),
        ("predicate_embed", "vector", "YES"),
        ("model_name", "varchar", "YES"),
    ]
    assert rows[0].column_default == "nextval('resource_user_access_seq'::regclass)"


@pytest.mark.asyncio
async def test_ids_come_from_the_sequence(session_connect: AsyncSession):
    resource_id = await _new_resource(session_connect)
    first = await _insert(session_connect, resource_id)
    second = await _insert(session_connect, resource_id)

    assert second > first
    current = (await session_connect.execute(text("SELECT last_value FROM resource_user_access_seq"))).scalar_one()
    assert current == second


@pytest.mark.asyncio
async def test_an_access_is_stored_without_embedding_and_the_same_user_can_repeat(session_connect: AsyncSession):
    resource_id = await _new_resource(session_connect)
    for predicate in ("primera pregunta", "primera pregunta", "segunda pregunta"):
        await _insert(session_connect, resource_id, predicate=predicate)

    rows = (await session_connect.execute(text(
        "SELECT username, predicate, predicate_embed, model_name FROM resource_user_access WHERE resource_id = :r ORDER BY id"),
        {"r": resource_id})).all()
    assert [tuple(r) for r in rows] == [("jdoe", "primera pregunta", None, None), ("jdoe", "primera pregunta", None, None),
                                        ("jdoe", "segunda pregunta", None, None)]


@pytest.mark.asyncio
async def test_embeddings_support_similarity_search(session_connect: AsyncSession):
    resource_id = await _new_resource(session_connect)
    await _insert(session_connect, resource_id, predicate="far", embed="[1, 0, 0]", model_name="test/embedding-model-v1")
    await _insert(session_connect, resource_id, predicate="near", embed="[0, 0.9, 0.1]", model_name="test/embedding-model-v1")

    rows = (await session_connect.execute(text(
        """SELECT predicate, model_name, round((predicate_embed <=> CAST('[0, 1, 0]' AS vector))::numeric, 4) AS cosine_distance
           FROM resource_user_access WHERE resource_id = :r ORDER BY predicate_embed <=> CAST('[0, 1, 0]' AS vector)"""),
        {"r": resource_id})).all()

    assert [(r.predicate, r.model_name) for r in rows] == [("near", "test/embedding-model-v1"), ("far", "test/embedding-model-v1")]
    assert rows[1].cosine_distance == 1  # orthogonal vectors


@pytest.mark.asyncio
@pytest.mark.parametrize("column", ["username", "resource_id", "predicate"])
async def test_mandatory_columns_reject_null(session_connect: AsyncSession, column: str):
    values = {"resource_id": await _new_resource(session_connect), "username": "jdoe", "predicate": "p", column: None}

    async with _rejected(session_connect, "23502") as error:  # not_null_violation
        await _insert(session_connect, **values)
    assert error.value.orig.diag.column_name == column


@pytest.mark.asyncio
async def test_resource_id_must_exist_in_data_dict_registry(session_connect: AsyncSession):
    missing = (await session_connect.execute(text("SELECT coalesce(max(dictionary_id), 0) + 1000 FROM data_dict_registry"))).scalar_one()

    async with _rejected(session_connect, "23503") as error:  # foreign_key_violation
        await _insert(session_connect, missing)
    assert error.value.orig.diag.constraint_name == "fk_resource_id_data_dict_registry"


@pytest.mark.asyncio
@pytest.mark.parametrize("embed, model_name", [("[0.1, 0.2]", None), (None, "test/embedding-model-v1")])
async def test_embedding_and_model_name_go_together(session_connect: AsyncSession, embed, model_name):
    resource_id = await _new_resource(session_connect)

    async with _rejected(session_connect, "23514") as error:  # check_violation
        await _insert(session_connect, resource_id, embed=embed, model_name=model_name)
    assert error.value.orig.diag.constraint_name == "ck_resource_user_access_embed_with_model"


@pytest.mark.asyncio
async def test_a_resource_with_accesses_cannot_be_deleted(session_connect: AsyncSession):
    resource_id = await _new_resource(session_connect)
    await _insert(session_connect, resource_id)

    async with _rejected(session_connect, "23503"):
        await session_connect.execute(text("DELETE FROM data_dict_registry WHERE dictionary_id = :r"), {"r": resource_id})
