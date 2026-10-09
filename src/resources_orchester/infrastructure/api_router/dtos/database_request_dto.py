from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from .data_field import DataField

class DatabaseRequestDTO(BaseModel):
    """DTO para las solicitudes relacionadas con la estructura y permisos de bases de datos."""
    name: Optional[str] = Field(None, description="Nombre opcional del recurso o tabla en la BD.")
    description: List[str] = Field(..., description="Descripción detallada en formato de lista.")
    schema_use: str = Field(..., description="Schema de la base de datos objetivo (ej: public, resource).") # Obligatorio
    permissions: Optional[List[Dict]] = Field(None, description="Lista opcional de permisos requeridos para el acceso.")
    process_apply_add: Optional[List[str]] = Field(None, description="Procesos a los que se debe añadir la asociación en BD.")
    process_apply_del: Optional[List[str]] = Field(None, description="Procesos de los que se debe eliminar la asociación en BD.")
    fields_add: Optional[List[DataField]] = Field(None, description="Campos nuevos a añadir a la estructura de datos de la tabla.")
    fields_del: Optional[List[DataField]] = Field(None, description="Campos que deben ser marcados para eliminación o desuso.")

