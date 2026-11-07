from typing import List, Optional, Protocol

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.model.stored_data_resource import StoredDataResource


class ReferenceQueryPortOut(Protocol):
    """Driven port (read side) over the data dictionary: registered resources with their identity."""

    async def get_data_dictionary_by_use(self, data_use: DataUse) -> List[StoredDataResource]:
        """Registered resources of one type (DataUse)."""
        ...

    async def get_data_dictionary_by_applications(self, applications_name: Optional[List[str]] = None,
                                                  type_applications_id: Optional[int] = None) -> List[StoredDataResource]:
        """Resources linked to the given applications and/or to applications of the given type (no filter = all linked)."""
        ...

    async def get_data_dictionary_by_name(self, name: str, data_use: Optional[DataUse] = None) -> List[StoredDataResource]:
        """Resources whose name is exactly ``name`` (optionally of one type). Names are not unique, so it is a list."""
        ...
