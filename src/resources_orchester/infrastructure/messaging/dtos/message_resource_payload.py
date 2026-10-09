from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# Importación corregida del componente de contenido separado (ESTRUCTURA V2)
from .message_content_payload import MessageContentPayload 

# Importaciones de Dominio
from src.resources_orchester.domain.enum.family_resources import FamilyResource 
from src.resources_orchester.domain.enum.data_use import DataUse # Asumo existencia de este enum
from src.resources_orchester.domain.model.data_resources import DataResources

class MessageResourcePayload(BaseModel):
    """
    DTO para mensajes de recursos serializables vía Kafka (VERSIÓN CORREGIDA). 
    Envuelve la información necesaria para el procesamiento en el consumidor.
    """
    family_resource: FamilyResource = Field(..., description="Familia del recurso que se está manejando (Origen).")
    type_resource: DataUse = Field(..., description="Tipo de uso del recurso.");
    name_resource: str = Field(..., description="Nombre único del recurso en el origen.")
    content: MessageContentPayload = Field(..., description="Carga útil completa y detallada del recurso.")


    @classmethod
    def from_data_resources(cls, destiny: dict[str, any], data_resources: DataResources) -> 'MessageResourcePayload':
        """
        Genera un DTO de mensaje a partir de un objeto DataResources.
        Mapea los atributos clave del dominio al payload estructurado para Kafka, 
        cumpliendo con la estructura de MessageContentPayload.
        """
        
        # 1. Creamos el payload del contenido (excluyendo name_resource)
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
            type_resource=destiny["type_destination"],       # Usando competence como DataUse para el mensaje completo
            name_resource=str(data_resources.name),       # Mapeo de nombre
            content=content_payload                      # Asumiendo que MessageContentPayload captura la complejidad del contenido
        )

    @classmethod
    def from_message_resources(cls, message: dict[str, any]) -> MessageResourcePayload:

        content_payload = MessageContentPayload.model_validate(message['content'])
        family: FamilyResource = FamilyResource.DATAS if str(message['family_resource']) == "question" else FamilyResource.CONTEXT
        type_resource: DataUse = DataUse.ASYNC_MESSAGE if str(message['type_resource']) == "MESSAGE" else DataUse.DB_TABLE

        return cls(
            family_resource=family, # Mapeo directo: Domain -> DTO
            type_resource=type_resource,       # Usando competence como DataUse para el mensaje completo
            name_resource=str(message['name_resource']),       # Mapeo de nombre
            content=content_payload                      # Asumiendo que MessageContentPayload captura la complejidad del contenido
        )

    def model_dump_json(self, *args, **kwargs) -> str:
        """Serializa el objeto a JSON."""
        return super().model_dump_json(*args, **kwargs)