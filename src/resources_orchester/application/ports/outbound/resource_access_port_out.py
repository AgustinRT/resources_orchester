from typing import List, Optional, Protocol

from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.domain.model.resource_access_match import ResourceAccessMatch


class ResourceAccessPortOut(Protocol):
    """
    Driven port over the storage of resource accesses (vector table ``resource_user_access``).
    Writes become durable when the unit of work commits. Every value that shapes a search
    (limit, minimum similarity) is received as a parameter: the adapter decides none of them.
    """

    async def save_access(self, access: ResourceAccess) -> ResourceAccess:
        """
        Stores the access together with the embedding of its predicate and returns it as stored
        (``predicate_embed`` and ``model_name`` filled in).
        Raises EmbeddingException if the embedding cannot be obtained (nothing is stored) and
        NotFoundException if ``resource_id`` is not a registered resource.
        """
        ...

    async def find_accesses(self, username: Optional[str], resource_id: Optional[int], limit: int) -> List[ResourceAccess]:
        """Accesses whose username and/or resource_id match EXACTLY (a None filter is not applied), newest first."""
        ...

    async def find_similar_accesses(self, predicate: str, resource_id: Optional[int], limit: int,
                                    min_similarity: float) -> List[ResourceAccessMatch]:
        """
        Accesses whose predicate is similar to ``predicate`` (cosine similarity >= ``min_similarity``),
        most similar first, at most ``limit``. Only vectors built by the current model are compared.
        Optionally restricted to ``resource_id``. Raises EmbeddingException if ``predicate`` cannot be embedded.
        """
        ...
