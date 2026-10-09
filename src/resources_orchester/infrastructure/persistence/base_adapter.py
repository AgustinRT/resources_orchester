from typing import Annotated

from fastapi import Depends
from resources_orchester.infrastructure.persistence.ports.applications_port_in import ApplicationsPortIn
from resources_orchester.infrastructure.persistence.ports.reference_port_in import ReferencePortIn
from resources_orchester.infrastructure.persistence.ports.reference_port_out import ReferencePortOut
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from resources_orchester.infrastructure.persistence.repositories.application_repository import ApplicationRepository
from resources_orchester.infrastructure.persistence.repositories.reference_repository import ReferenceRepository
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    """
    Proporciona una sesión de base de datos asíncrona usando un context manager.
    El usuario debe usar 'async with DatabaseManager.get_db_session() as session:'
    """
    postgre_manager = DatabaseManager()
    if not postgre_manager.health_pool():
        await postgre_manager.init_engine()

    async with postgre_manager.get_session() as session:
        yield session

def get_reference_port_in(session: AsyncSession = Depends(get_db_session)) -> ReferencePortIn:
    return ReferenceRepository(session)

def get_reference_port_out(session: AsyncSession = Depends(get_db_session)) -> ReferencePortOut:
    return ReferenceRepository(session)

def get_reference_repository_test(session: AsyncSession = Depends(get_db_session)) -> ReferenceRepository:
    return ReferenceRepository(session)


def get_application_port_in(session: AsyncSession = Depends(get_db_session)) -> ApplicationsPortIn:
    return ApplicationRepository(session)

RefPortIn = Annotated[ReferencePortIn, Depends(get_reference_port_in)]
RefPortOut = Annotated[ReferencePortOut, Depends(get_reference_port_out)]
ReferenceRepo = Annotated[ReferenceRepository, Depends(get_reference_repository_test)]

AppliPortInt = Annotated[ApplicationsPortIn, Depends(get_application_port_in)]