from pydantic import BaseModel, ConfigDict, Field

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.model.data_resources import DataResources


class StoredDataResource(BaseModel):
    """
    Un DataResources ya persistido en el registro (diccionario de datos), con su identidad.
    Envuelve al recurso en lugar de añadirle un id, para no alterar el contrato de la API
    (que devuelve DataResources).
    """
    model_config = ConfigDict(frozen=True)

    dictionary_id: int = Field(..., description="Identificador del registro en el diccionario de datos.")
    data_use: DataUse = Field(..., description="Tipo de uso con el que se registró el recurso.")
    resource: DataResources = Field(..., description="El recurso de datos.")


class ResourceApplicationLink(BaseModel):
    """Asociación entre un recurso registrado y una aplicación."""
    model_config = ConfigDict(frozen=True)

    dictionary_id: int
    application_id: int
