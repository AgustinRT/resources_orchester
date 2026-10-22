from typing import List

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.persistence.entities import registry_applications
from resources_orchester.infrastructure.persistence.entities.link_dict_application import DataDictionaryAppRef
from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, delete, select
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict


class ReferenceRepository():
    """
    Implementación concreta (Adaptador) del Repositorio de Referencias.
    Utiliza el DatabaseManager para obtener y gestionar sesiones asíncronas, 
    cumpliendo con los contratos definidos en ReferencePortIn y ReferencePortOut.
    """

    def __init__(self, db_session: AsyncSession):
        self._db_session = db_session
    
    async def get_data_dictionary_by_use(self, data_use: DataUse) -> List[DataRefDict]:
        """Consulta referencias por el tipo de uso (ENUM)."""
        stmt = select(DataRefDict).where(
            DataRefDict.data_use_dictionay == data_use
        )
        result = await self._db_session.execute(stmt)
        return result.scalars().all()

    async def get_data_dictionary_by_applications(self, applications_name: List[str] = None, type_applications_id: int = None) -> List[DataRefDict]:
        """Consulta referencias asociadas a múltiples aplicaciones y un tipo específico."""

        stmt = select(DataRefDict).join(
                                    DataDictionaryAppRef, DataDictionaryAppRef.dictionary_id == DataRefDict.dictionary_id
                                ).join(
                                    RegistryApplications, RegistryApplications.application_id == DataDictionaryAppRef.application_id
                                ).where(
                                    and_(
                                        (RegistryApplications.type_application_id == type_applications_id if type_applications_id is not None else True),
                                        (RegistryApplications.name_application.in_(applications_name) if applications_name is not None and applications_name != [] else True)
                                    )
                                )
        result = await self._db_session.execute(stmt)
        return result.scalars().all()

    async def save_data_dictionary_reference(self, data: DataResources) -> DataRefDict:
        """Guarda una nueva referencia."""
        try:
            usable_fields = [x.model_dump() for x in data.fields]
            usable_descrp = data.description
            new_record: DataRefDict = DataRefDict(
                                description_data=usable_descrp,
                                data_use_dictionay=DataUse.ASYNC_MESSAGE if data.competence == "messaging" else DataUse.DB_TABLE if data.competence == "database" else DataUse.REST_OBJ,
                                fields=usable_fields
                            )

            self._db_session.add(new_record)
            await self._db_session.flush()
            return new_record
        except Exception as e:
            print(e.__cause__)
            raise

    async def update_data_dictionary_reference(self, id_dictionary: int, data: DataResources) -> DataRefDict:
        """Actualiza una referencia existente."""
        stmt = select(DataRefDict).where(DataRefDict.dictionary_id == id_dictionary)
        result = await self._db_session.execute(stmt)
        record = result.scalar_one_or_none()

        if not record:
            raise ValueError(f"Referencia con ID {id_dictionary} no encontrada.")
        
        for key, value in data.items():
            setattr(record, key, value)
        
        await self._db_session.flush()
        return record

    async def delete_data_dictionary_reference(self, id_dictionary: int) -> bool:
        """Elimina la referencia por su ID."""
       
        stmt = delete(DataRefDict).where(DataRefDict.dictionary_id == id_dictionary)
        result = await self._db_session.execute(stmt)
        await self._db_session.flush()
        return result.rowcount > 0

    async def save_data_reference_application(self, data_dictionary: DataRefDict, applications_assig: registry_applications.RegistryApplications) -> DataDictionaryAppRef:
        """Guarda una nueva referencia."""
        try:
            new_record: DataDictionaryAppRef = DataDictionaryAppRef(
                                dictionary_id=data_dictionary.dictionary_id,
                                application_id=applications_assig.application_id
                            )

            self._db_session.add(new_record)
            await self._db_session.flush()
            return new_record
        except Exception as e:
            print(e.__cause__)
            raise