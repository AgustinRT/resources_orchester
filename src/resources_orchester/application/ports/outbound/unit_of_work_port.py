from types import TracebackType
from typing import Optional, Protocol, Self


class UnitOfWorkPort(Protocol):
    """
    Transaction boundary owned by the application (driven port).

    Usage in a use case::

        async with self._uow:
            ...writes through the outbound ports (they share the unit of work)...
            await self._uow.commit()

    Anything not explicitly committed is rolled back when the block exits, including when it
    raises. ``commit`` raises ``PersistenceException`` if the database rejects the transaction,
    so callers never report success for data that was not stored.
    """

    async def __aenter__(self) -> Self: ...

    async def __aexit__(self, exc_type: Optional[type[BaseException]], exc: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...
