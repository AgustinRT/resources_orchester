from typing import Protocol

from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from resources_orchester.infrastructure.persistence.entities.link_dict_application import DataDictionaryAppRef
# Importaciones asumidas necesarias para el contrato:
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
# Se asume la existencia del modelo de payload data, idealmente un Pydantic model o dataclass.
from typing import Any # Usando 'Any' si DataResources no está accesible aún

class ReferencePortOut(Protocol):
    """Puerto de Salida (Output Port) para la lógica transaccional del Diccionario de Datos."""

    async def save_data_dictionary_reference(self, data: DataResources) -> DataRefDict:
        """Guarda una referencia completamente nueva en el sistema."""
        ...

    async def update_data_dictionary_reference(self, id_dictionary: int, data: DataResources) -> DataRefDict:
        """Actualiza los detalles de una referencia existente por su ID."""
        ...

    async def delete_data_dictionary_reference(self, id_dictionary: int) -> bool:
        """Elimina completamente una referencia del sistema y retorna un indicador booleano de éxito."""
        ...


    async def save_data_reference_application(self, data_dictionary: DataRefDict, applications_assig: RegistryApplications) -> DataDictionaryAppRef:
        """conecta la referencia con su aplciacion."""
        ...
