from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class ContextResource(BaseModel):
    """
    Modelo para representar los recursos de contexto.
    Define la ubicación y la estructura jerárquica de un contexto basado en documentación (MD).
    """
    name: str = Field(..., description="Nombre ÚNICO del recurso de contexto.")
    description: Dict[str, str] = Field(..., description="Descripción multiidioma (e.g., {'es_ES': '...', 'en_US': '...'}).")
    context_raw_path: str = Field(..., description="Ruta al archivo MD que contiene la versión original y completa del contexto.")
    sections: List[Dict] = Field(..., description="Árbol de secciones que componen el contenido estructurado del contexto (Markdown).")