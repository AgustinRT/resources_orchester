import contextlib
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from resources_orchester.infrastructure.bootstrap.settings import DatabaseSettings


class DatabaseManager:
    """
    Owns the SQLAlchemy async engine (with its connection pool) and the session factory.

    Plain class: the composition root creates ONE instance per application in the lifespan,
    stores it in ``app.state`` and disposes it on shutdown. Configuration is injected; this
    class never reads environment variables or dotenv files.

    Creating the engine does not open connections: they are opened lazily and then reused
    from the pool (pool_size + max_overflow bound the connections per process).
    """

    def __init__(self, settings: DatabaseSettings):
        self._engine: AsyncEngine = create_async_engine(
            settings.sqlalchemy_url(),
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            pool_timeout=settings.pool_timeout_seconds,
            pool_pre_ping=settings.pool_pre_ping,
        )
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            autoflush=False,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        """Unit of work: commits if the block succeeds, rolls back if it raises."""
        session = self._session_factory()
        try:
            yield session
            await session.commit()
        except BaseException:
            await session.rollback()
            raise
        finally:
            await session.close()

    @contextlib.asynccontextmanager
    async def open_session(self) -> AsyncGenerator[AsyncSession]:
        """
        Session WITHOUT automatic commit: whatever is not committed explicitly (by a unit of
        work) is rolled back when the session closes. Used per HTTP request / Kafka message.
        """
        session = self._session_factory()
        try:
            yield session
        finally:
            await session.close()

    async def dispose(self) -> None:
        """Closes every pooled connection. Called once, on application shutdown."""
        await self._engine.dispose()
