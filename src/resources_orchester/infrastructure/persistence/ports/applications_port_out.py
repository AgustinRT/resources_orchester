from typing import Protocol, List

from resources_orchester.domain.model.data_resources import DataResources
# Importaciones asumidas necesarias para el contrato (Modelos y Payloads)
from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from typing import Any

class ApplicationsPortOut(Protocol):
    """Puerto de Salida (Output Port) para las operaciones transaccionales en Aplicaciones."""

    async def save_application(self, data: DataResources) -> RegistryApplications:
        """Crea y guarda un nuevo registro de sistema de aplicación."""
        ...

    async def delete_application(self, name_application: str) -> bool:
        """Elimina una aplicación por su nombre único."""
        ...
