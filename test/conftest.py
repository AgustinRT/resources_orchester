"""
Shared pytest fixtures.

Containers are session-scoped and shared by every test package (infrastructure adapters,
application services, API and lifecycle tests), so each one is started at most once per run:

* PostgreSQL: each test gets its own session that is rolled back at the end
  (see session_with_rollback). Tests that run the real application lifespan commit for
  real and clean up with ``truncate_resource_tables``.
* Kafka: tests isolate themselves by using their own topic names (see ``unique_topic``).

Configuration is injected: fixtures build DatabaseSettings / KafkaSettings pointing at the
containers; nothing is passed through environment variables.
"""
import uuid
from typing import AsyncGenerator, Callable

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.application.ports.outbound.resource_query_port_out import ResourceQueryPortOut
from resources_orchester.application.ports.outbound.event_outbox_port_out import EventOutboxPortOut
from resources_orchester.application.ports.outbound.unit_of_work_port import UnitOfWorkPort
from resources_orchester.infrastructure.bootstrap.dependencies import (
    get_application_query_port_out,
    get_event_outbox_port_out,
    get_reference_query_port_out,
    get_reference_command_port_out,
    get_resource_query_port_out,
)
from resources_orchester.infrastructure.bootstrap.settings import DatabaseSettings, KafkaSettings
from resources_orchester.application.ports.outbound.application_query_port_out import ApplicationQueryPortOut
from resources_orchester.application.ports.outbound.reference_query_port_out import ReferenceQueryPortOut
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from resources_orchester.infrastructure.persistence.unit_of_work import SqlAlchemyUnitOfWork

from test.infraestructure.kafka_container_tester import rise_kafka_container
from test.infraestructure.postgres_container_tester import (
    FAIL_ON_COMMIT_PREFIX,
    INSTALL_FAIL_ON_COMMIT_TRIGGER,
    UNINSTALL_FAIL_ON_COMMIT_TRIGGER,
    database_settings_from_container,
    rise_postgres_container,
    session_with_rollback,
)


# --- PostgreSQL -------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def postgres_container():
    """Start a Postgres container with SQL initialization."""
    async with rise_postgres_container() as container:
        yield container


@pytest.fixture(scope="session")
def database_settings(postgres_container) -> DatabaseSettings:
    return database_settings_from_container(postgres_container)


@pytest_asyncio.fixture(scope="function")
async def database_manager(database_settings) -> AsyncGenerator[DatabaseManager]:
    """
    One DatabaseManager (engine + pool) per test. Pooled connections are bound to the event
    loop that opened them and pytest-asyncio uses a new loop per test, so the pool is disposed
    at the end of each test instead of being shared across loops.
    """
    database = DatabaseManager(database_settings)
    yield database
    await database.dispose()


@pytest_asyncio.fixture(scope="function")
async def session_connect(database_manager) -> AsyncGenerator[AsyncSession]:
    """Provide a psycopg3 session that rolls back after each test."""
    async with session_with_rollback(database_manager) as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def truncate_resource_tables(database_manager) -> AsyncGenerator[None]:
    """
    For tests that COMMIT for real (real application lifespan, outbox relay, commit failures):
    empties the resource and outbox tables afterwards. Every other test rolls back, so these
    tables only ever hold data from such tests; the seed tables are never touched.
    """
    yield
    async with database_manager.session() as session:
        await session.execute(text("TRUNCATE link_dict_application, data_dict_registry, event_outbox"))


@pytest_asyncio.fixture(scope="function")
async def fail_on_commit_trigger(database_manager) -> AsyncGenerator[str]:
    """
    Installs a deferred constraint trigger that makes COMMIT fail for resources whose name starts
    with the yielded prefix (flush succeeds, COMMIT is rejected). Removed after the test.
    """
    async with database_manager.session() as session:
        for statement in INSTALL_FAIL_ON_COMMIT_TRIGGER:
            await session.execute(text(statement))
    yield FAIL_ON_COMMIT_PREFIX
    async with database_manager.session() as session:
        for statement in UNINSTALL_FAIL_ON_COMMIT_TRIGGER:
            await session.execute(text(statement))


@pytest_asyncio.fixture(scope="function")
async def reference_command_port_out(session_connect) -> ReferenceCommandPortOut:
    return get_reference_command_port_out(session_connect)


@pytest_asyncio.fixture(scope="function")
async def reference_query_port_out(session_connect) -> ReferenceQueryPortOut:
    return get_reference_query_port_out(session_connect)


@pytest_asyncio.fixture(scope="function")
async def application_query_port_out(session_connect) -> ApplicationQueryPortOut:
    return get_application_query_port_out(session_connect)


@pytest_asyncio.fixture(scope="function")
async def resource_query_port_out(session_connect) -> ResourceQueryPortOut:
    return get_resource_query_port_out(session_connect)


@pytest_asyncio.fixture(scope="function")
async def event_outbox_port_out(session_connect) -> EventOutboxPortOut:
    return get_event_outbox_port_out(session_connect)


@pytest_asyncio.fixture(scope="function")
async def unit_of_work(session_connect) -> UnitOfWorkPort:
    """Real SqlAlchemyUnitOfWork on the test session (its commit releases a savepoint, see session_with_rollback)."""
    return SqlAlchemyUnitOfWork(session_connect)


# --- Kafka ------------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def kafka_container() -> AsyncGenerator[str]:
    """Starts one Kafka container for the whole run. Yields its bootstrap server ("host:port")."""
    async with rise_kafka_container() as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture
def unique_topic() -> Callable[[str], str]:
    """Topic names that are unique per call, so tests sharing the Kafka container never see each other's messages."""
    return lambda base: f"{base}-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def kafka_settings(kafka_container, unique_topic) -> KafkaSettings:
    """KafkaSettings for the test container, with private update/request/consumer topics."""
    update_topic, request_topic = unique_topic("update"), unique_topic("request")
    return KafkaSettings(bootstrap_servers=kafka_container,
                         update_topic=update_topic,
                         producer_topics=[update_topic, request_topic],
                         consumer_topics=[unique_topic("report")],
                         consumer_retry_backoff_seconds=0.5)
