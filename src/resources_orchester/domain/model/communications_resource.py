from pydantic import BaseModel, Field
from typing import Dict

# ===================================
# communications_resource Model
# ===================================

class CommunicationsResource(BaseModel):
    """
    Modelo para representar los recursos relacionados con comunicaciones.
    Define las reglas de seguridad y la estrategia de intercambio de información.
    """
    name: str = Field(..., description="Nombre ÚNICO del recurso de comunicación.")
    description: Dict[str, str] = Field(..., description="Descripción multiidioma (e.g., {'es_ES': '...', 'en_US': '...'}).")
    auth_apply: str = Field(..., description="Especificación del tipo de securización a aplicar en la comunicación (ej: JWT, OAuth2).")
    strategy: str = Field(..., description="Explicación detallada de la estrategia a seguir al comunicar los recursos.")