from typing import List, Optional, Protocol

from resources_orchester.domain.api_dtos.resource_access_dto import ResourceAccessRequestDTO
from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.domain.model.resource_access_match import ResourceAccessMatch


class ResourceAccessPortIn(Protocol):
    """Use cases over the accesses of users to resources (vector table)."""

    async def register_access(self, request: ResourceAccessRequestDTO) -> ResourceAccess:
        """
        Validates the request, turns it into a ResourceAccess and stores it with its embedding.
        Raises ValidationException (domain rule broken), NotFoundException (unknown resource) or
        EmbeddingException (AI model unavailable). In every case nothing is stored.
        """
        ...

    async def search_accesses(self, username: Optional[str] = None, resource_id: Optional[int] = None,
                              limit: Optional[int] = None) -> List[ResourceAccess]:
        """
        Accesses of ``username`` and/or ``resource_id`` (exact match; at least one is required), newest first.
        ``limit`` None = the configured value. Raises ValidationException if the parameters are not valid.
        """
        ...

    async def search_similar_accesses(self, predicate: str, resource_id: Optional[int] = None, limit: Optional[int] = None,
                                      min_similarity: Optional[float] = None) -> List[ResourceAccessMatch]:
        """
        Accesses whose predicate is similar to ``predicate``, optionally only for ``resource_id``.
        ``limit`` / ``min_similarity`` None = the configured values.
        Raises ValidationException (invalid parameters) or EmbeddingException (AI model unavailable).
        """
        ...
