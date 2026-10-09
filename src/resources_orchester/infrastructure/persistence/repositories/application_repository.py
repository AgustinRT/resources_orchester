from typing import List
from resources_orchester.domain.model.data_resources import DataResources

from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from resources_orchester.infrastructure.persistence.entities.applications_type_definition import ApplicationsTypeDefinition
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
# Importar los puertos de las ubicaciones correctas
from resources_orchester.infrastructure.persistence.ports.applications_port_in import ApplicationsPortIn
from resources_orchester.infrastructure.persistence.ports.applications_port_out import ApplicationsPortOut

# Implementación del repositorio que debe adherirse a ambos contratos ( puertos)
class ApplicationRepository(ApplicationsPortIn, ApplicationsPortOut):
    """
    Implementa los contratos de persistencia para el sistema de aplicaciones, 
    sirviendo como adaptador entre la lógica de negocio y la capa de base de datos.
    """

    def __init__(self, db_session: AsyncSession):
        # Dependencia inyectada (DI)
        self._db_session = db_session
    
    async def get_application_by_name(self, name_application: str) -> RegistryApplications | None:
        """Busca una aplicación por medio de su nombre único."""
        print(f"DEBUG: Searching for application by name: {name_application}")
        try:
            stmt = select(RegistryApplications).where(
                RegistryApplications.name_application == name_application
            )
            result = await self._db_session.execute(stmt)
            return result.scalar_one()
        except Exception as e:
            print(e.__cause__)
            raise


    async def get_application(self, id_application: int) -> RegistryApplications | None:
        """Busca una aplicación por medio de su id."""

        stmt = select(RegistryApplications).where(
            RegistryApplications.application_id == id_application
        )
        result = await self._db_session.execute(stmt)
        return result.scalars().one_or_none()

    async def get_all_applications(self) -> List[RegistryApplications]:
        """Lista todas las aplicaciones."""
        stmt = select(RegistryApplications).order_by(RegistryApplications.name_application)
        result = await self._db_session.execute(stmt)
        return result.scalars().all()

    # --- Implementación del Output Port: ApplicationsPortOut ---
    
    async def _get_application_type(self, name_type: str) -> ApplicationsTypeDefinition:
        stmt = select(ApplicationsTypeDefinition).where(
            ApplicationsTypeDefinition.name_definition == name_type
        )
        result = await self._db_session.execute(stmt)
        type: List[ApplicationsTypeDefinition] = result.scalars().all()
        return type[0]

    async def save_application(self, data: DataResources) -> RegistryApplications:
        """Crea y guarda un nuevo registro de sistema de aplicación."""
        new_app = RegistryApplications(id=1, name="New App") # Crear entidad simulada
        return new_app

    async def delete_application(self, name_application: str) -> bool:
        """Elimina una aplicación por su nombre único."""
        # Lógica de eliminación en la base de datos.
        print(f"DEBUG: Deleting application with name: {name_application}")
        return True # Retornar si la eliminación fue exitosa