from typing import List, Optional

from sqlalchemy import case, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.application.ports.outbound.resource_access_port_out import ResourceAccessPortOut
from resources_orchester.domain.exception.domain_exceptions import NotFoundException
from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.domain.model.resource_access_match import ResourceAccessMatch
from resources_orchester.infrastructure.persistence.embeddings.embedding_client import EmbeddingClient
from resources_orchester.infrastructure.persistence.entities.resource_user_access import ResourceUserAccess

# Name given to the FK in database/init.sql (and migration 003).
_RESOURCE_FK = "fk_resource_id_data_dict_registry"
_FOREIGN_KEY_VIOLATION = "23503"


def _to_domain(entity: ResourceUserAccess) -> ResourceAccess:
    return ResourceAccess(username=entity.username, resource_id=entity.resource_id, predicate=entity.predicate,
                          predicate_embed=list(entity.predicate_embed) if entity.predicate_embed is not None else None,
                          model_name=entity.model_name)


def _is_unknown_resource(error: IntegrityError) -> bool:
    orig = error.orig
    return (getattr(orig, "sqlstate", None) == _FOREIGN_KEY_VIOLATION
            and getattr(getattr(orig, "diag", None), "constraint_name", None) == _RESOURCE_FK)


class ResourceAccessRepository(ResourceAccessPortOut):
    """
    PostgreSQL (pgvector) adapter of ResourceAccessPortOut over ``resource_user_access``.

    Embeddings come from the injected EmbeddingClient (``get_embed``); which AI model answers is
    decided by configuration, not here. Like the other repositories it only flushes: the unit of
    work of the use case commits.
    """

    def __init__(self, db_session: AsyncSession, embedding_client: EmbeddingClient):
        self._db_session = db_session
        self._embedding_client = embedding_client

    async def save_access(self, access: ResourceAccess) -> ResourceAccess:
        # First the embedding (EmbeddingException -> nothing is written), before any SQL, so no
        # database transaction stays open while waiting for the AI model.
        embed = await self._embedding_client.get_embed(access.predicate)
        entity = ResourceUserAccess(username=access.username, resource_id=access.resource_id, predicate=access.predicate,
                                    predicate_embed=embed, model_name=self._embedding_client.model_name)
        self._db_session.add(entity)
        try:
            await self._db_session.flush()
        except IntegrityError as error:
            if _is_unknown_resource(error):
                raise NotFoundException(f"El recurso {access.resource_id} no está registrado.") from error
            raise
        return _to_domain(entity)

    async def find_accesses(self, username: Optional[str], resource_id: Optional[int], limit: int) -> List[ResourceAccess]:
        if username is None and resource_id is None:
            raise ValueError("find_accesses needs username and/or resource_id")  # never list the whole table
        stmt = select(ResourceUserAccess)
        if username is not None:
            stmt = stmt.where(ResourceUserAccess.username == username)
        if resource_id is not None:
            stmt = stmt.where(ResourceUserAccess.resource_id == resource_id)
        stmt = stmt.order_by(ResourceUserAccess.id.desc()).limit(limit)

        result = await self._db_session.execute(stmt)
        return [_to_domain(entity) for entity in result.scalars().all()]

    async def find_similar_accesses(self, predicate: str, resource_id: Optional[int], limit: int,
                                    min_similarity: float) -> List[ResourceAccessMatch]:
        embed = await self._embedding_client.get_embed(predicate)

        column = ResourceUserAccess.predicate_embed
        # Vectors of another model (or size) are not comparable: only the current model is searched.
        # The size check is inside CASE because PostgreSQL does not guarantee the order in which WHERE
        # conditions run, and "<=>" fails on vectors of different sizes.
        distance = case((func.vector_dims(column) == len(embed), column.cosine_distance(embed)), else_=None)
        similarity = (1 - distance).label("similarity")
        stmt = (select(ResourceUserAccess, similarity)
                .where(ResourceUserAccess.model_name == self._embedding_client.model_name,
                       column.is_not(None),
                       distance <= 1 - min_similarity)
                .order_by(distance, ResourceUserAccess.id.desc())
                .limit(limit))
        if resource_id is not None:
            stmt = stmt.where(ResourceUserAccess.resource_id == resource_id)

        result = await self._db_session.execute(stmt)
        return [ResourceAccessMatch(access=_to_domain(entity), similarity=float(score)) for entity, score in result.all()]
