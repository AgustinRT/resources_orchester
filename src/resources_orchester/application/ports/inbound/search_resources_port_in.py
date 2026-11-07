from typing import List, Optional, Protocol

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.model.data_resources import DataResources


class SearchResourcesPortIn(Protocol):
    """Use case (read-only): find registered resources by partial name and, optionally, by type."""

    async def search_resources_by_name(self, name: str, data_use: Optional[DataUse] = None) -> List[DataResources]: ...
