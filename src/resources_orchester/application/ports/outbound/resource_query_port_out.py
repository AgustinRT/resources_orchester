from typing import List, Optional, Protocol

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.model.data_resources import DataResources


class ResourceQueryPortOut(Protocol):
    """
    Puerto de Salida (Driven / Output Port) de SOLO LECTURA sobre los recursos persistidos.

    Lo define la capa de aplicación (quien lo necesita) y lo implementa la infraestructura
    (ReferenceRepository). A diferencia de los puertos de persistencia existentes, su contrato
    está expresado únicamente en modelos de dominio: ninguna entidad ORM cruza esta frontera.
    """

    async def search_data_resources_by_name(self, name_fragment: str, data_use: Optional[DataUse] = None) -> List[DataResources]:
        """
        Devuelve los recursos cuyo nombre contiene ``name_fragment`` (coincidencia parcial,
        sin distinguir mayúsculas/minúsculas). Los caracteres comodín de LIKE (``%``, ``_``)
        se tratan como literales. Si se indica ``data_use`` solo se devuelven recursos de ese tipo.
        Devuelve una lista vacía si no hay coincidencias.
        """
        ...
