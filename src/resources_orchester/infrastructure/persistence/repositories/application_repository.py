from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.application.ports.outbound.application_query_port_out import ApplicationQueryPortOut
from resources_orchester.domain.model.application import Application
from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications


class ApplicationRepository(ApplicationQueryPortOut):
    """
    Adaptador de persistencia (PostgreSQL) de las aplicaciones registradas.
    Implementa explícitamente ApplicationQueryPortOut y traduce las entidades ORM a Application.
    """

    def __init__(self, db_session: AsyncSession):
        # Dependencia inyectada (DI)
        self._db_session = db_session

    @staticmethod
    def _to_domain(record: RegistryApplications) -> Application:
        return Application(
            application_id=record.application_id,
            name=record.name_application,
            description=record.description_application,
            type_application_id=record.type_application_id,
            location=record.location_application or {},
        )

    async def get_application_by_name(self, name_application: str) -> Optional[Application]:
        """Busca una aplicación por medio de su nombre único."""
        stmt = select(RegistryApplications).where(RegistryApplications.name_application == name_application)
        record = (await self._db_session.execute(stmt)).scalar_one_or_none()
        return self._to_domain(record) if record is not None else None

    async def get_application(self, id_application: int) -> Optional[Application]:
        """Busca una aplicación por medio de su id."""
        record = await self._db_session.get(RegistryApplications, id_application)
        return self._to_domain(record) if record is not None else None

    async def get_all_applications(self) -> List[Application]:
        """Lista todas las aplicaciones."""
        stmt = select(RegistryApplications).order_by(RegistryApplications.name_application)
        result = await self._db_session.execute(stmt)
        return [self._to_domain(record) for record in result.scalars().all()]
