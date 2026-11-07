from typing import List, Optional

from resources_orchester.application.ports.inbound.search_resources_port_in import SearchResourcesPortIn
from resources_orchester.application.ports.outbound.resource_query_port_out import ResourceQueryPortOut
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.exception.domain_exceptions import ValidationException
from resources_orchester.domain.model.data_resources import DataResources


class SearchResources(SearchResourcesPortIn):
    """Read-only use case: it only needs the query port (no unit of work, no events, no Kafka)."""

    def __init__(self, resource_query_port_out: ResourceQueryPortOut):
        self._resource_query_port_out = resource_query_port_out

    async def search_resources_by_name(self, name: str, data_use: Optional[DataUse] = None) -> List[DataResources]:
        """
        Busca recursos persistidos cuyo nombre contenga ``name`` (coincidencia parcial,
        sin distinguir mayúsculas/minúsculas), filtrando opcionalmente por tipo (DataUse).

        Raises:
            ValidationException: si el término de búsqueda está vacío o solo contiene espacios
                                 (evita devolver la tabla completa por accidente).
        """
        search_term = (name or "").strip()
        if not search_term:
            raise ValidationException("El nombre a buscar no puede estar vacío.")

        return await self._resource_query_port_out.search_data_resources_by_name(search_term, data_use)
