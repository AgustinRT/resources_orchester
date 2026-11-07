from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# Importación corregida del componente de contenido separado (ESTRUCTURA V2)
from .message_content_payload import MessageContentPayload 

# Importaciones de Dominio
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.model.data_resources import DataResources

class MessageResourcePayload(BaseModel):
    """
    DTO para mensajes de recursos serializables vía Kafka (VERSIÓN CORREGIDA). 
    Envuelve la información necesaria para el procesamiento en el consumidor.
    """

#     mcps1 - ERROR - ERROR processing Kafka message from topic 'update': 1 validation error for KafkaEnvelopeDTO
# message_data
#   Field required [type=missing, input_value={'family_resource': 'data...ions': ['not blank']}]}}, input_type=dict]
#     For further information visit https://errors.pydantic.dev/2.13/v/missing

        # {
    #     "destination": {"resource_type": "datas", "resource_value": "asinc_messages"},
    #     "message_data": {
    #         "source": "ms_order",                 # emitter (informative)
    #         "family_resource": "datas",           # FamilyResource (value or name)
    #         "type_resource": "asinc_messages",    # DataUse (value or name)
    #         "content": {                          # plain resource object
    #             "name": "test_resource",          # file name of the resource
    #             "description": ["..."],
    #             "competence": "messaging",
    #             "process_apply": null,
    #             "type_data": "json",
    #             "fields": [...]
    #         }
    #     }
    # }
    
    family_resource: FamilyResource = Field(..., description="Familia del recurso que se está manejando (Origen).")
    type_resource: DataUse = Field(..., description="Tipo de uso del recurso.");
    source: str = Field(..., description="Nombre único del recurso en el origen.")
    content: MessageContentPayload = Field(..., description="Carga útil completa y detallada del recurso.")


    @classmethod
    def from_data_resources(cls, destiny: dict[str, Any], data_resources: DataResources) -> 'MessageResourcePayload':
        """
        Genera un DTO de mensaje a partir de un objeto DataResources.
        Mapea los atributos clave del dominio al payload estructurado para Kafka, 
        cumpliendo con la estructura de MessageContentPayload.
        """
        
        # 1. Creamos el payload del contenido (excluyendo source)
        content_payload = MessageContentPayload(
            description=data_resources.description, # Mapeo directo: DataResources.description -> MessageContentPayload.description
            competence=data_resources.competence,   # Mapeo directo: DataResources.competence -> MessageContentPayload.competence
            process_apply=data_resources.process_apply, # Mapeo directo
            type_data=data_resources.type_data,       # Mapeo directo
            fields=[x.model_dump() for x in data_resources.fields]             # Asumiendo que DataResources ya expone 'fields' como List[Dict]
        )

        # 2. Creamos el payload final con la información de cabecera y contenido

        return cls(
            family_resource=destiny["family_destination"], # Mapeo directo: Domain -> DTO
            type_resource=destiny["resource_value"],       # Usando competence como DataUse para el mensaje completo
            source=str(data_resources.name),       # Mapeo de nombre
            content=content_payload                      # Asumiendo que MessageContentPayload captura la complejidad del contenido
        )

    @classmethod
    def from_message_resources(cls, message: dict[str, Any]) -> MessageResourcePayload:

        content_payload = MessageContentPayload.model_validate(message['content'])
        family: FamilyResource = FamilyResource.DATAS if str(message['family_resource']) == "question" else FamilyResource.CONTEXT
        type_resource: DataUse = DataUse.ASYNC_MESSAGE if str(message['type_resource']) == "MESSAGE" else DataUse.DB_TABLE

        return cls(
            family_resource=family, # Mapeo directo: Domain -> DTO
            type_resource=type_resource,       # Usando competence como DataUse para el mensaje completo
            source=str(message['source']),       # Mapeo de nombre
            content=content_payload                      # Asumiendo que MessageContentPayload captura la complejidad del contenido
        )

    def model_dump_json(self, *args, **kwargs) -> str:
        """Serializa el objeto a JSON."""
        return super().model_dump_json(*args, **kwargs)