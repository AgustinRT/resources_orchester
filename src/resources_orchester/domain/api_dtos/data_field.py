from pydantic import BaseModel, Field
from typing import List, Optional

class DataField(BaseModel):
    """DTO para describir un campo de dato en las solicitudes REST o BD."""
    name: str = Field(..., description="Nombre obligatorio del campo.")
    type: str = Field(..., description="Tipo de dato (ej: string, int, uuid).")
    description: List[str] = Field(..., description="Descripción detallada del campo en formato de lista.")
    validations: Optional[List[str]] = Field(None, description="reglas de validación para este campo especifico.")
