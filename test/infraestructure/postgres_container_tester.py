import contextlib
from pathlib import Path

from resources_orchester.infrastructure.bootstrap.settings import DatabaseSettings
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from sqlalchemy.engine import make_url
from testcontainers.community.postgres import PostgresContainer
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator


# init.sql lives next to this module; resolve it relative to the file so the
# suite works on any machine / CI runner (it used to be a hardcoded home path).
INIT_SQL_PATH = Path(__file__).resolve().parent / "init.sql"


@contextlib.asynccontextmanager
async def rise_postgres_container():
    control_path = str(INIT_SQL_PATH)
    # Same image as docker-compose.yaml: init.sql needs the pgvector extension.
    container = PostgresContainer(image="pgvector/pgvector:pg16",
                                  username="admin",
                                  password="admin",
                                  # init.sql (re)creates and populates "resourcesdb"; connect to that
                                  # database instead of testcontainers' default ("test"), which stays empty.
                                  dbname="resourcesdb",
                                  driver=None # the driver must be None to use the psycopg3 driver
                                  )
    container.with_volume_mapping(
        host=control_path,
        container=f"/docker-entrypoint-initdb.d/init.sql"
    )
    try:
        container.start()
        yield container
    except Exception:
        container.stop()
        raise
    finally:
        container.stop()

def database_settings_from_container(container) -> DatabaseSettings:
    """DatabaseSettings pointing at the running test container (injected, no env vars involved)."""
    url = make_url(container.get_connection_url())
    return DatabaseSettings(host=url.host, port=url.port, user=url.username,
                            password=url.password, db=url.database)


@contextlib.asynccontextmanager
async def session_with_rollback(database: DatabaseManager) -> AsyncGenerator[AsyncSession]:
    """
    Yields a session inside an OUTER transaction that is rolled back when the test ends, so every
    test starts from the seed data in init.sql.

    SQLAlchemy's "join an external transaction" recipe: with join_transaction_mode="create_savepoint",
    a ``commit()`` issued by the code under test (e.g. a unit of work) only releases a SAVEPOINT,
    so even committing use cases leave no data behind. Tests that need REAL commits (outbox relay,
    commit failures at COMMIT time) use the ``truncate_resource_tables`` fixture instead.
    """
    async with database.engine.connect() as connection:
        outer_transaction = await connection.begin()
        session = AsyncSession(bind=connection, join_transaction_mode="create_savepoint", expire_on_commit=False)
        try:
            yield session
        finally:
            await session.close()
            await outer_transaction.rollback()


# Simulated COMMIT failure: a deferred constraint trigger only runs at COMMIT time, so INSERT and
# flush succeed and the transaction is rejected when it is committed (review D2).
FAIL_ON_COMMIT_PREFIX = "fail_on_commit"
INSTALL_FAIL_ON_COMMIT_TRIGGER = [
    f"""CREATE OR REPLACE FUNCTION test_fail_on_commit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.name_data LIKE '{FAIL_ON_COMMIT_PREFIX}%' THEN
            RAISE EXCEPTION 'simulated commit failure for %', NEW.name_data;
          END IF;
          RETURN NEW;
        END $$""",
    "DROP TRIGGER IF EXISTS test_fail_on_commit ON data_dict_registry",
    """CREATE CONSTRAINT TRIGGER test_fail_on_commit AFTER INSERT ON data_dict_registry
       DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION test_fail_on_commit()""",
]
UNINSTALL_FAIL_ON_COMMIT_TRIGGER = [
    "DROP TRIGGER IF EXISTS test_fail_on_commit ON data_dict_registry",
    "DROP FUNCTION IF EXISTS test_fail_on_commit()",
]
