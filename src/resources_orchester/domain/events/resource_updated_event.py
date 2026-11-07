from pydantic import BaseModel, ConfigDict

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.model.data_resources import DataResources


class ResourceUpdatedEvent(BaseModel):
    """
    Domain event: a resource was created or updated and other systems must be notified.

    Use cases do not publish it directly: they record it in the outbox inside the same
    transaction as the data change, and the outbox relay publishes it once committed.
    """
    model_config = ConfigDict(frozen=True)

    resource: DataResources
    family: FamilyResource
    data_use: DataUse
