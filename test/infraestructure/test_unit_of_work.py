"""
SqlAlchemyUnitOfWork against the PostgreSQL Testcontainer with REAL commits (tables truncated after
each test). The commit-failure case uses a deferred constraint trigger: INSERT and flush succeed
and the database rejects the transaction at COMMIT, which is exactly the failure of review D2.
"""
import uuid

import pytest
from sqlalchemy import func, select

from resources_orchester.domain.exception.infrastructure_exceptions import PersistenceException
from resources_orchester.domain.model.application import Application
from resources_orchester.domain.model.stored_data_resource import StoredDataResource
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from resources_orchester.infrastructure.persistence.repositories.reference_repository import ReferenceRepository
from resources_orchester.infrastructure.persistence.unit_of_work import SqlAlchemyUnitOfWork

from test.infraestructure.utils_tester import create_random_data_resources

pytestmark = pytest.mark.usefixtures("truncate_resource_tables")


async def _stored_count(database: DatabaseManager, name: str) -> int:
    async with database.session() as session:
        return (await session.execute(select(func.count()).select_from(DataRefDict).where(DataRefDict.name_data == name))).scalar_one()


def _resource(name: str):
    resource = create_random_data_resources()
    resource.name = name
    return resource


@pytest.mark.asyncio
async def test_commit_persists_and_runs_the_after_commit_hook(database_manager: DatabaseManager):
    name, hook_calls = f"uow_ok_{uuid.uuid4().hex[:8]}", []
    async with database_manager.open_session() as session:
        async with SqlAlchemyUnitOfWork(session, after_commit=lambda: hook_calls.append(1)) as uow:
            await ReferenceRepository(session).save_data_dictionary_reference(_resource(name))
            await uow.commit()

    assert await _stored_count(database_manager, name) == 1
    assert hook_calls == [1]


@pytest.mark.asyncio
async def test_leaving_without_commit_rolls_back(database_manager: DatabaseManager):
    name = f"uow_no_commit_{uuid.uuid4().hex[:8]}"
    async with database_manager.open_session() as session:
        async with SqlAlchemyUnitOfWork(session):
            await ReferenceRepository(session).save_data_dictionary_reference(_resource(name))

    assert await _stored_count(database_manager, name) == 0


@pytest.mark.asyncio
async def test_an_error_in_the_block_rolls_back_and_propagates(database_manager: DatabaseManager):
    name = f"uow_error_{uuid.uuid4().hex[:8]}"
    async with database_manager.open_session() as session:
        with pytest.raises(RuntimeError, match="business failure"):
            async with SqlAlchemyUnitOfWork(session):
                await ReferenceRepository(session).save_data_dictionary_reference(_resource(name))
                raise RuntimeError("business failure")

    assert await _stored_count(database_manager, name) == 0


@pytest.mark.asyncio
async def test_database_errors_in_the_block_become_persistence_exception(database_manager: DatabaseManager):
    """A failed flush (FK violation: unknown application) surfaces as a domain PersistenceException."""
    async with database_manager.open_session() as session:
        with pytest.raises(PersistenceException):
            async with SqlAlchemyUnitOfWork(session):
                repository = ReferenceRepository(session)
                stored: StoredDataResource = await repository.save_data_dictionary_reference(_resource("fk"))
                await repository.save_data_reference_application(
                    stored, Application(application_id=999_999, name="ghost", description="", type_application_id=1))


@pytest.mark.asyncio
async def test_commit_failure_raises_and_stores_nothing(database_manager: DatabaseManager, fail_on_commit_trigger: str):
    name, hook_calls = f"{fail_on_commit_trigger}_{uuid.uuid4().hex[:8]}", []
    async with database_manager.open_session() as session:
        with pytest.raises(PersistenceException, match="No se pudo confirmar"):
            async with SqlAlchemyUnitOfWork(session, after_commit=lambda: hook_calls.append(1)) as uow:
                await ReferenceRepository(session).save_data_dictionary_reference(_resource(name))  # flush OK
                await uow.commit()  # rejected at COMMIT

    assert await _stored_count(database_manager, name) == 0
    assert hook_calls == []  # the relay is not woken up for a transaction that did not commit
