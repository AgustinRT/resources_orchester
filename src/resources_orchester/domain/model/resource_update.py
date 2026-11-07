from typing import Dict, List

from pydantic import BaseModel, ConfigDict, Field

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.model.data_field_resource import DataFieldResource


class ResourceUpdate(BaseModel):
    """
    Command received from other systems (Kafka) to update a registered resource.
    The resource is identified by its name and type; description and fields are replaced.
    """
    model_config = ConfigDict(frozen=True)

    name: str = Field(..., min_length=1)
    data_use: DataUse
    description: Dict[str, str]
    fields: List[DataFieldResource]
