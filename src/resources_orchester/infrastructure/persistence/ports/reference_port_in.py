from typing import Optional, Protocol, List

from resources_orchester.domain.enum.data_use import DataUse
# Importaciones asumidas necesarias para el contrato (Modelos y Enums)
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
from resources_orchester.infrastructure.persistence.entities.applications_type_definition import ApplicationsTypeDefinition

class ReferencePortIn(Protocol):
    """Puerto de Entrada (Input Port) para la consulta y lectura del Diccionario de Datos."""

    async def get_data_dictionary_by_use(self, data_use: DataUse) -> List[DataRefDict]:
        """Busca referencias por un tipo específico de uso (ENUM)."""
        ...

    async def get_data_dictionary_by_applications(self, applications_name: List[str] = None, type_applications_id: int = None) -> List[DataRefDict]:
        """Busca referencias asociadas a múltiples aplicaciones y un tipo específico."""
        ...
