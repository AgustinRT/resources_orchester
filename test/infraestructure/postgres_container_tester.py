import contextlib

from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from testcontainers.community.postgres import PostgresContainer
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator


@contextlib.asynccontextmanager
async def rise_postgres_container():
    control_path = "/home/agustin/lm_proyects/resources_orchester/test/infraestructure/init.sql"
    container = PostgresContainer(image="postgres:16-alpine",
                                  username="admin",
                                  password="admin",
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

@contextlib.asynccontextmanager
async def set_session_to_container(url_container: str) -> AsyncGenerator[AsyncSession]:

    postgre_manager = DatabaseManager(url_container)

    if not postgre_manager.health_pool():
        await postgre_manager.init_engine()

    async with postgre_manager.get_session() as session:
        yield session
