from pydantic import BaseModel, Field
from typing import List, Dict, Optional

from resources_orchester.domain.model.data_field_resource import DataFieldResource

class DataResources(BaseModel):
    """
    Modelo para representar los recursos de datos.
    Define la estructura y las metainformaciones de un conjunto de datos específico.
    """
    name: str = Field(..., description="Nombre ÚNICO del recurso de datos.")
    description: Dict[str, str] = Field(..., description="Descripción multiidioma (e.g., {'es_ES': '...', 'en_US': '...'}).")
    competence: str = Field(..., description="Tipo de competencia donde será usado el dato (ej: mensajes kafka).")
    process_apply: Optional[List[str]] = Field(None, description="Lista opcional de procesos a los que se puede asociar este dato.")
    type_data: str = Field(..., description="Referencia a la estructura de datos recibida por la aplicación (ej: 'json', 'byte[]', 'dbRow').")
    fields: List[DataFieldResource] = Field(..., description="Definición explicativa y estructural interna del dato.")