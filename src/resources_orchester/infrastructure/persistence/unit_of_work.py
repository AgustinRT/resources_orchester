from types import TracebackType
from typing import Callable, Optional, Self

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.application.ports.outbound.unit_of_work_port import UnitOfWorkPort
from resources_orchester.domain.exception.infrastructure_exceptions import PersistenceException


class SqlAlchemyUnitOfWork(UnitOfWorkPort):
    """
    UnitOfWorkPort over the request/message AsyncSession shared by the repositories.

    * ``commit`` commits the session; a database error becomes PersistenceException.
    * Leaving the block always rolls back whatever was not committed (a no-op after a commit).
    * Database errors raised inside the block (e.g. a failed flush) also surface as
      PersistenceException, so the application only sees domain exceptions.
    * ``after_commit`` (optional) is called after each successful commit; the composition root
      uses it to wake up the outbox relay so events are published right away.
    """

    def __init__(self, session: AsyncSession, after_commit: Optional[Callable[[], None]] = None):
        self._session = session
        self._after_commit = after_commit

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Optional[type[BaseException]], exc: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> None:
        try:
            await self._session.rollback()
        except SQLAlchemyError as rollback_error:
            if exc is None:
                raise PersistenceException(f"No se pudo revertir la transacción: {rollback_error}") from rollback_error
            # an exception is already propagating: keep it (it explains why we are rolling back)
        if isinstance(exc, SQLAlchemyError):
            raise PersistenceException(f"Error de base de datos: {exc}") from exc

    async def commit(self) -> None:
        try:
            await self._session.commit()
        except SQLAlchemyError as error:
            raise PersistenceException(f"No se pudo confirmar la transacción: {error}") from error
        if self._after_commit is not None:
            self._after_commit()

    async def rollback(self) -> None:
        await self._session.rollback()
