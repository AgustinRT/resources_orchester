import math
from typing import List, Optional

from pydantic import ValidationError

from resources_orchester.application.ports.inbound.resource_access_port_in import ResourceAccessPortIn
from resources_orchester.application.ports.outbound.resource_access_port_out import ResourceAccessPortOut
from resources_orchester.application.ports.outbound.unit_of_work_port import UnitOfWorkPort
from resources_orchester.domain.api_dtos.resource_access_dto import ResourceAccessRequestDTO
from resources_orchester.domain.exception.domain_exceptions import ValidationException
from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.domain.model.resource_access_match import ResourceAccessMatch


def _describe(error: ValidationError) -> str:
    """Short, readable summary of a pydantic error: 'field: message; field: message'."""
    return "; ".join(f"{'.'.join(str(part) for part in e['loc']) or 'access'}: {e['msg']}" for e in error.errors())


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_resource_id(resource_id: Optional[int]) -> None:
    if resource_id is not None and (not _is_int(resource_id) or resource_id <= 0):
        raise ValidationException("resource debe ser un entero mayor que 0.")


def _check_limit(limit: int) -> int:
    if not _is_int(limit) or limit < 1:
        raise ValidationException("limit debe ser un entero mayor o igual que 1.")
    return limit


def _check_min_similarity(min_similarity: float) -> float:
    if (isinstance(min_similarity, bool) or not isinstance(min_similarity, (int, float))
            or not math.isfinite(min_similarity) or not 0 <= min_similarity <= 1):
        raise ValidationException("min_similarity debe ser un número entre 0 y 1.")
    return float(min_similarity)


def _is_blank(value: Optional[str]) -> bool:
    return not isinstance(value, str) or not value.strip()


class ManageResourceAccess(ResourceAccessPortIn):
    """
    Service for the resource-access endpoints.

    * Registering runs in ONE unit of work: the access is stored with its embedding and
      committed, or (embedding unavailable, unknown resource, database error) nothing is stored.
    * The search values (limits, minimum similarity) are NOT decided here: the composition root
      injects the configured defaults, and each call may override them.
    """

    def __init__(self, unit_of_work: UnitOfWorkPort, resource_access_port_out: ResourceAccessPortOut, *,
                 list_limit: int, similarity_limit: int, min_similarity: float):
        self._uow = unit_of_work
        self._resource_access_port_out = resource_access_port_out
        self._list_limit = list_limit
        self._similarity_limit = similarity_limit
        self._min_similarity = min_similarity

    async def register_access(self, request: ResourceAccessRequestDTO) -> ResourceAccess:
        try:
            # The embedding (predicate_embed / model_name) is added by the storage adapter.
            access = ResourceAccess(username=request.username, resource_id=request.resource_id,
                                    predicate=request.predicate)
        except ValidationError as error:
            raise ValidationException(_describe(error)) from error

        async with self._uow:
            stored = await self._resource_access_port_out.save_access(access)
            await self._uow.commit()
        return stored

    async def search_accesses(self, username: Optional[str] = None, resource_id: Optional[int] = None,
                              limit: Optional[int] = None) -> List[ResourceAccess]:
        if username is None and resource_id is None:
            raise ValidationException("Indique al menos username o resource.")
        if username is not None and _is_blank(username):
            raise ValidationException("username no puede estar vacío.")
        _check_resource_id(resource_id)
        effective_limit = _check_limit(self._list_limit if limit is None else limit)

        return await self._resource_access_port_out.find_accesses(username, resource_id, effective_limit)

    async def search_similar_accesses(self, predicate: str, resource_id: Optional[int] = None, limit: Optional[int] = None,
                                      min_similarity: Optional[float] = None) -> List[ResourceAccessMatch]:
        if _is_blank(predicate):
            raise ValidationException("predicate no puede estar vacío.")
        _check_resource_id(resource_id)
        effective_limit = _check_limit(self._similarity_limit if limit is None else limit)
        effective_min = _check_min_similarity(self._min_similarity if min_similarity is None else min_similarity)

        return await self._resource_access_port_out.find_similar_accesses(predicate, resource_id, effective_limit,
                                                                          effective_min)
