from pydantic import BaseModel, ConfigDict, Field

from resources_orchester.domain.model.resource_access import ResourceAccess


class ResourceAccessMatch(BaseModel):
    """A stored access returned by a similarity search, with how similar its predicate is to the searched text."""
    model_config = ConfigDict(frozen=True)

    access: ResourceAccess
    similarity: float = Field(..., description="Similitud coseno con el texto buscado (1 = idéntico).")
