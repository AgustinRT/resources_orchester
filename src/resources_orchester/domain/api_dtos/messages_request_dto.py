from pydantic import BaseModel, Field
from typing import List, Optional
from .data_field import DataField

class MessagesRequestDTO(BaseModel):
    """DTO para las solicitudes relacionadas con mensajes o colas."""
    name: Optional[str] = Field(None, description="Nombre opcional del mensaje/recurso.")
    description: List[str] = Field(..., description="Descripción detallada en formato de lista.")
    process_apply_add: Optional[List[str]] = Field(None, description="Procesos a los que se debe añadir este mensaje/dato.")
    process_apply_del: Optional[List[str]] = Field(None, description="Procesos de los que se debe eliminar la asociación.")
    fields_add: Optional[List[DataField]] = Field(None, description="Campos nuevos a incluir en el mensaje o recurso.")
    fields_del: Optional[List[DataField]] = Field(None, description="Campos a eliminar del mensaje o recurso.")

