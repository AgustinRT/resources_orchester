from typing import List, Protocol

from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications

class ApplicationsPortIn(Protocol):
    """Puerto de Entrada (Input Port) para la gestión del sistema de aplicaciones."""

    async def get_application_by_name(self, name_application: str) -> RegistryApplications:
        """Busca una aplicacion por medio de su nombre unico"""
        ...
    async def get_application(self, id_application: int) -> RegistryApplications:
        """Busca una aplicacion por medio de su id"""
        ...
    async def get_all_applications(self) -> List[RegistryApplications]:
        """Lista todas las aplciaciones"""
        ...