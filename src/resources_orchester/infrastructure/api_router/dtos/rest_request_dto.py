from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from .data_field import DataField

class RestRequestDTO(BaseModel):
    """DTO principal para manejar las solicitudes REST que definen endpoints y su estructura."""
    name: Optional[str] = Field(None, description="Nombre opcional del recurso API.")
    description: List[str] = Field(..., description="Descripción detallada en formato de lista.")

    endpoints_request: List[Dict[str, List[str]]] = Field(..., description="Lista obligatoria de endpoints que se esperan recibir (ej: {'GET': ['lista', 'crear']}).")
    endpoints_response: List[Dict[str, List[str]]] = Field(..., description="Lista obligatoria de respuestas posibles para el recurso API.")
    
    fields_add: Optional[List[DataField]] = Field(None, description="Campos opcionales nuevos que se pueden añadir a las peticiones/respuestas.")
    fields_del: Optional[List[DataField]] = Field(None, description="Campos opcionales marcados para eliminación o desuso en la API.")

