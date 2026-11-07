from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field


class Application(BaseModel):
    """
    Aplicación registrada (microservicio, BD, front-end, ...) que consume o produce recursos.
    Modelo de dominio: la capa de aplicación nunca ve la entidad ORM que lo persiste.
    """
    model_config = ConfigDict(frozen=True)

    application_id: int = Field(..., description="Identificador de la aplicación.")
    name: str = Field(..., description="Nombre ÚNICO de la aplicación (ej: 'MS_listener_question').")
    description: str = Field(..., description="Descripción de la aplicación.")
    type_application_id: int = Field(..., description="Tipo de aplicación (MSH, MSS, DB, APPB, APPN...).")
    location: Dict[str, Any] = Field(default_factory=dict, description="Localización (región, grupo, familia...).")
