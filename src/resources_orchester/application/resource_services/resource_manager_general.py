from typing import List, Dict

from resources_orchester.domain.api_dtos.database_request_dto import DatabaseRequestDTO
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.api_dtos.rest_request_dto import RestRequestDTO
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.infrastructure.messaging.ports.kafka_port_out import KafkaPortOut
from src.resources_orchester.domain.enum.family_resources import FamilyResource

from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
from resources_orchester.infrastructure.persistence.ports.applications_port_in import ApplicationsPortIn
from resources_orchester.infrastructure.persistence.ports.reference_port_in import ReferencePortIn
from resources_orchester.infrastructure.persistence.ports.reference_port_out import ReferencePortOut 

from src.resources_orchester.domain.model.data_resources import DataResources, DataFieldResource

class ResourceManagerGeneral:
    """
    IMPLEMENTACIÓN CONCRETA DEL PUERTO DE ENTRADA: 
    Este servicio implementa la lógica de negocio del Use Case, 
    realizando la transformación (DTO -> Domain Model) y simulando el flujo transaccional ASÍNCRONO.
    """
    
    def __init__(self, reference_port_in: ReferencePortIn, reference_port_out: ReferencePortOut, application_port_in: ApplicationsPortIn, messaging_port_out: KafkaPortOut):
        """Inicializa el ResourceManagerGeneral inyectando los puertos de persistencia y comunicación."""
        self._reference_port_in = reference_port_in
        self._reference_port_out = reference_port_out
        self._application_port_in = application_port_in
        self._messaging_port_out = messaging_port_out

    async def create_message_resource(self, payload_message: MessagesRequestDTO) -> DataResources:
        """Transforma y persiste un DTO de mensaje en el dominio de recursos, 
            y notifica a Kafka sobre la creación del recurso."""
        fields_data: List[DataFieldResource] = []
        for new_field in payload_message.fields_add:
            descrp: Dict = {"es_ES": new_field.description[0]}
            fields_data.append(DataFieldResource(
                name=new_field.name,
                description=descrp,
                type=new_field.type,
                validations=new_field.validations
            ))

        data_resources = DataResources(
            name=payload_message.name or "Unknown Message Resource",
            description=self._dto_to_dict_description(payload_message.description),
            competence="messaging",
            process_apply=payload_message.process_apply_add if payload_message.process_apply_add else None,
            type_data="json", 
            fields=fields_data
        )

        name_application: str = "MS_producer_question" if data_resources.process_apply and data_resources.process_apply[0] == "some_ms" else "MS_listener_question"
        applications_assig: RegistryApplications = await self._application_port_in.get_application_by_name(name_application)
        new_reference: DataRefDict = await self._reference_port_out.save_data_dictionary_reference(data_resources)
        await self._reference_port_out.save_data_reference_application(new_reference, applications_assig)
        
        destination_message = {
            "topic": f"update",
            "family_destination": FamilyResource.DATAS,
            "type_destination": DataUse.ASYNC_MESSAGE if data_resources.competence == "messaging" else DataUse.DB_TABLE
        }
        
        self._messaging_port_out.send_message_to_update(destination_message, data_resources)
        
        return data_resources

    async def update_message_resource(self, resource_name: str, payload_message: MessagesRequestDTO) -> DataResources:
        """Actualiza un recurso de mensaje existente."""
        print(f"-> [ResourceService] Iniciando actualización para Mensajería en: {resource_name}")
        return DataResources(
            name=resource_name,
            description=self._dto_to_dict_description(payload_message.description),
            competence="messaging",
            process_apply=[f"updated-{resource_name}"],
            type_data="json", 
            fields=[]
        )
    
    async def create_database_resource(self, payload_database: DatabaseRequestDTO) -> DataResources:
        """Transforma un DTO de BD en un modelo de datos de recursos."""
        print("-> [ResourceService] Iniciando creación de recurso de Base de Datos (Domain Model).")
        return DataResources(
            name=payload_database.name or f"DB Resource {payload_database.schema}",
            description=self._dto_to_dict_description(payload_database.description),
            competence="database",
            process_apply=None,
            type_data="dbRow",
            fields=[{"name": df.name, "type": df.type, "description": str(df.description)} for df in (payload_database.fields_add or [])]
        )

    async def update_database_resource(self, resource_name: str, payload_database: DatabaseRequestDTO) -> DataResources:
        """Actualiza un recurso de BD existente."""
        print(f"-> [ResourceService] Iniciando actualización para Base de Datos en: {resource_name}")
        return DataResources(
            name=resource_name,
            description=self._dto_to_dict_description(payload_database.description),
            competence="database",
            process_apply=None,
            type_data="dbRow", 
            fields=[]
        )

    async def create_rest_resource(self, payload_rest: RestRequestDTO) -> DataResources:
        """Transforma un DTO de REST en un modelo de datos de recursos."""
        print("-> [ResourceService] Iniciando creación de recurso API (Domain Model).")
        return DataResources(
            name=payload_rest.name or "Unknown API Resource",
            description=self._dto_to_dict_description(payload_rest.description),
            competence="api_gateway",
            process_apply=None,
            type_data="http/json", 
            fields=[{"name": df.name, "type": df.type, "description": str(df.description)} for df in (payload_rest.fields_add or [])]
        )

    async def update_rest_resource(self, resource_name: str, payload_rest: RestRequestDTO) -> DataResources:
        """Actualiza un recurso API existente."""
        print(f"-> [ResourceService] Iniciando actualización para API REST en: {resource_name}")
        return DataResources(
            name=resource_name,
            description=self._dto_to_dict_description(payload_rest.description),
            competence="api_gateway", 
            process_apply=None,
            type_data="http/json", 
            fields=[]
        )

    def _dto_to_dict_description(self, descriptions: List[str]) -> Dict[str, str]:
        """Helper function to simulate conversion of List[str] description to Dict[str, str]."""
        if not descriptions:
            return {}
        return {"es_ES": descriptions[0], "en_US": descriptions[-1]}
