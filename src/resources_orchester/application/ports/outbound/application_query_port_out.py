from typing import List, Optional, Protocol

from resources_orchester.domain.model.application import Application


class ApplicationQueryPortOut(Protocol):
    """Driven port (read side) over the registered applications."""

    async def get_application_by_name(self, name_application: str) -> Optional[Application]:
        """The application with that unique name, or None."""
        ...

    async def get_application(self, id_application: int) -> Optional[Application]:
        """The application with that id, or None."""
        ...

    async def get_all_applications(self) -> List[Application]:
        """Every registered application, ordered by name."""
        ...
