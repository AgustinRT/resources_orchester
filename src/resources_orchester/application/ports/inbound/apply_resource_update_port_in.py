from typing import Protocol

from resources_orchester.domain.model.resource_update import ResourceUpdate
from resources_orchester.domain.model.stored_data_resource import StoredDataResource


class ApplyResourceUpdatePortIn(Protocol):
    """Use case driven by Kafka: apply an update received from another system to a registered resource."""

    async def apply(self, update: ResourceUpdate) -> StoredDataResource: ...
