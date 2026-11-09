from typing import Annotated, List, Optional

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, StringConstraints, model_validator

# Trimmed, non-empty text. Whitespace-only values carry no information.
_NonBlankStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ResourceAccess(BaseModel):
    """
    A user's access to a registered resource, stored in the vector table.

    * ``username`` identifies a single user, but the same user can have many accesses, so it is
      NOT unique in this table.
    * ``predicate_embed`` is the vector of ``predicate``; ``model_name`` is the model that built it.
      Vectors from different models cannot be compared, so both are set together or neither
      is (an access is registered before its embedding exists).
    """
    model_config = ConfigDict(frozen=True)

    username: Annotated[_NonBlankStr, StringConstraints(max_length=150)] = Field(
        ..., description="Nombre único del usuario que accedió al recurso (se repite en cada acceso del usuario).")
    resource_id: int = Field(
        ..., gt=0, description="Recurso accedido (FK a data_dict_registry.dictionary_id).")
    predicate: _NonBlankStr = Field(
        ..., description="Texto del prompt original que el usuario envió al agente de IA.")
    predicate_embed: Optional[List[FiniteFloat]] = Field(
        None, min_length=1, description="Vector (embedding) del predicate. None hasta que se calcule.")
    model_name: Optional[_NonBlankStr] = Field(
        None, description="Nombre completo del modelo de IA que generó predicate_embed.")

    @model_validator(mode="after")
    def _embedding_and_model_go_together(self) -> "ResourceAccess":
        if (self.predicate_embed is None) != (self.model_name is None):
            raise ValueError("predicate_embed and model_name must be provided together (or neither of them).")
        return self
