from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut
from resources_orchester.application.ports.outbound.reference_query_port_out import ReferenceQueryPortOut
from resources_orchester.application.ports.outbound.resource_query_port_out import ResourceQueryPortOut
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.exception.domain_exceptions import NotFoundException
from resources_orchester.domain.model.application import Application
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.domain.model.stored_data_resource import ResourceApplicationLink, StoredDataResource
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
from resources_orchester.infrastructure.persistence.entities.link_dict_application import DataDictionaryAppRef
from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications


# Inverso de la regla de creación de los casos de uso (competence/type_data por familia, ver use_cases).
# La tabla solo persiste el DataUse, así que es la fuente para reconstruir el modelo de dominio.
_DOMAIN_ATTRS_BY_USE: Dict[DataUse, Tuple[str, str]] = {
    DataUse.ASYNC_MESSAGE: ("messaging", "json"),
    DataUse.DB_TABLE: ("database", "dbRow"),
    DataUse.REST_OBJ: ("api_gateway", "http/json"),
}


def _data_use_for(data: DataResources) -> DataUse:
    """Regla de persistencia: competencia del recurso -> tipo de uso almacenado."""
    if data.competence == "messaging":
        return DataUse.ASYNC_MESSAGE
    if data.competence == "database":
        return DataUse.DB_TABLE
    return DataUse.REST_OBJ


class ReferenceRepository(ReferenceQueryPortOut, ReferenceCommandPortOut, ResourceQueryPortOut):
    """
    Adaptador de persistencia (PostgreSQL) del diccionario de datos.

    Implementa explícitamente los puertos de salida ReferenceQueryPortOut, ReferenceCommandPortOut
    y ResourceQueryPortOut. Las entidades ORM no salen de esta clase: se traducen a modelos de
    dominio (DataResources / StoredDataResource / ResourceApplicationLink).
    Solo hace flush(); el commit pertenece a la unidad de trabajo (sesión de la petición).
    """

    def __init__(self, db_session: AsyncSession):
        self._db_session = db_session

    # --- Mapeo ORM <-> dominio ---

    @staticmethod
    def _to_domain(record: DataRefDict) -> DataResources:
        """Traduce la entidad ORM al modelo de dominio (la entidad no sale del adaptador)."""
        competence, type_data = _DOMAIN_ATTRS_BY_USE[record.data_use_dictionay]
        return DataResources(
            name=record.name_data,
            description=record.description_data,
            competence=competence,
            process_apply=None,  # no se persiste en data_dict_registry
            type_data=type_data,
            fields=record.fields or [],
        )

    @classmethod
    def _to_stored(cls, record: DataRefDict) -> StoredDataResource:
        return StoredDataResource(dictionary_id=record.dictionary_id, data_use=record.data_use_dictionay,
                                  resource=cls._to_domain(record))

    @staticmethod
    def _record_values(data: DataResources) -> Dict[str, Any]:
        return {
            "name_data": data.name,
            "description_data": data.description,
            "data_use_dictionay": _data_use_for(data),
            "fields": [field.model_dump() for field in data.fields],
        }

    # --- ResourceQueryPortOut ---

    async def search_data_resources_by_name(self, name_fragment: str, data_use: Optional[DataUse] = None) -> List[DataResources]:
        """Busca recursos por coincidencia parcial (ILIKE) del nombre y, opcionalmente, por tipo de uso."""
        # autoescape=True: '%' y '_' introducidos por el usuario se buscan de forma literal.
        conditions = [DataRefDict.name_data.icontains(name_fragment, autoescape=True)]
        if data_use is not None:
            conditions.append(DataRefDict.data_use_dictionay == data_use)

        stmt = select(DataRefDict).where(*conditions).order_by(DataRefDict.name_data, DataRefDict.dictionary_id)
        result = await self._db_session.execute(stmt)
        return [self._to_domain(record) for record in result.scalars().all()]

    # --- ReferenceQueryPortOut ---

    async def get_data_dictionary_by_use(self, data_use: DataUse) -> List[StoredDataResource]:
        """Consulta referencias por el tipo de uso (ENUM)."""
        stmt = select(DataRefDict).where(DataRefDict.data_use_dictionay == data_use)
        result = await self._db_session.execute(stmt)
        return [self._to_stored(record) for record in result.scalars().all()]

    async def get_data_dictionary_by_applications(self, applications_name: Optional[List[str]] = None,
                                                  type_applications_id: Optional[int] = None) -> List[StoredDataResource]:
        """Consulta referencias asociadas a múltiples aplicaciones y/o a un tipo de aplicación."""
        stmt = select(DataRefDict).join(
            DataDictionaryAppRef, DataDictionaryAppRef.dictionary_id == DataRefDict.dictionary_id
        ).join(
            RegistryApplications, RegistryApplications.application_id == DataDictionaryAppRef.application_id
        )
        if type_applications_id is not None:
            stmt = stmt.where(RegistryApplications.type_application_id == type_applications_id)
        if applications_name:
            stmt = stmt.where(RegistryApplications.name_application.in_(applications_name))

        result = await self._db_session.execute(stmt)
        return [self._to_stored(record) for record in result.scalars().all()]

    async def get_data_dictionary_by_name(self, name: str, data_use: Optional[DataUse] = None) -> List[StoredDataResource]:
        """Consulta referencias por nombre exacto (y opcionalmente tipo)."""
        stmt = select(DataRefDict).where(DataRefDict.name_data == name)
        if data_use is not None:
            stmt = stmt.where(DataRefDict.data_use_dictionay == data_use)
        result = await self._db_session.execute(stmt.order_by(DataRefDict.dictionary_id))
        return [self._to_stored(record) for record in result.scalars().all()]

    # --- ReferenceCommandPortOut ---

    async def save_data_dictionary_reference(self, data: DataResources) -> StoredDataResource:
        """Guarda una nueva referencia."""
        new_record = DataRefDict(**self._record_values(data))
        self._db_session.add(new_record)
        await self._db_session.flush()
        return self._to_stored(new_record)

    async def update_data_dictionary_reference(self, id_dictionary: int, data: DataResources) -> StoredDataResource:
        """Sustituye los datos de una referencia existente."""
        record = await self._db_session.get(DataRefDict, id_dictionary)
        if record is None:
            raise NotFoundException(f"Referencia con ID {id_dictionary} no encontrada.")

        for attribute, value in self._record_values(data).items():
            setattr(record, attribute, value)

        await self._db_session.flush()
        return self._to_stored(record)

    async def delete_data_dictionary_reference(self, id_dictionary: int) -> bool:
        """Elimina la referencia por su ID (falla por clave foránea si sigue asociada a aplicaciones)."""
        stmt = delete(DataRefDict).where(DataRefDict.dictionary_id == id_dictionary).returning(DataRefDict.dictionary_id)
        result = await self._db_session.execute(stmt)
        deleted = result.scalar_one_or_none()
        await self._db_session.flush()
        return deleted is not None

    async def save_data_reference_application(self, data_dictionary: StoredDataResource, application: Application) -> ResourceApplicationLink:
        """Asocia una referencia registrada a una aplicación."""
        new_record = DataDictionaryAppRef(dictionary_id=data_dictionary.dictionary_id,
                                          application_id=application.application_id)
        self._db_session.add(new_record)
        await self._db_session.flush()
        return ResourceApplicationLink(dictionary_id=new_record.dictionary_id, application_id=new_record.application_id)
