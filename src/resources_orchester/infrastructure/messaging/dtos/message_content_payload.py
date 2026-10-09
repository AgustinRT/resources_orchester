from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class MessageContentPayload(BaseModel):
    """ 
    Contiene los campos de un recurso (DataResources), excluyendo el nombre.
    Representa el payload detallado dentro del mensaje Kafka.
    """
    description: Dict[str, str] = Field(..., description="Descripción multilingüe del recurso.")
    competence: str = Field(..., description="Tipo funcional o competencia asociada al recurso (DataUse).")
    process_apply: Optional[List[str]] = Field(None, description="Lista de procesos aplicables.")
    type_data: str = Field(..., description="Tipo de dato del recurso (ej: json, dbRow, http/json).")
    fields: List[Dict] = Field(..., description="Listado de campos estructurados dentro del recurso.")

    class Config:
        # Permite el mapeo flexible para que Pydantic acepte datos en tiempo de ejecución.
        from_attributes = True 