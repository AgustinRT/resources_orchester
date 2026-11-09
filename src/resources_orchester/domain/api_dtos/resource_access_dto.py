from typing import Annotated, List, Optional

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from resources_orchester.domain.model.resource_access import ResourceAccess
from resources_orchester.domain.model.resource_access_match import ResourceAccessMatch

# Trimmed, non-empty text. Whitespace-only values are rejected (they carry no information).
NonBlankStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ResourceAccessRequestDTO(BaseModel):
    """
    DTO de entrada para registrar un acceso de un usuario a un recurso (información destinada
    a la base de datos vectorial).

    La validación es estricta: no se admiten campos desconocidos ni conversiones implícitas de
    tipo (p. ej. ``"7"`` no es un ``resource_id`` válido).
    """
    model_config = ConfigDict(extra="forbid", strict=True)

    username: Annotated[NonBlankStr, StringConstraints(max_length=150)] = Field(
        ..., description="Nombre único del usuario que ha accedido al recurso.")
    resource_id: int = Field(
        ..., gt=0, description="Identificador del recurso (FK a data_dict_registry.dictionary_id).")
    predicate: NonBlankStr = Field(
        ..., description="Texto del prompt original que el usuario usó al preguntar al agente de IA.")


class ResourceAccessDTO(BaseModel):
    """
    A stored access as returned by the API. The embedding vector is NOT exposed (it is large and
    only useful inside the vector database); ``model_name`` says which model built it, if any.
    """
    username: str
    resource_id: int
    predicate: str
    model_name: Optional[str] = None

    @classmethod
    def from_domain(cls, access: ResourceAccess) -> "ResourceAccessDTO":
        return cls(username=access.username, resource_id=access.resource_id,
                   predicate=access.predicate, model_name=access.model_name)


class ResourceAccessRegisteredResponseDTO(BaseModel):
    """Response of the POST: result of the operation and the access as stored."""
    success: bool = Field(..., description="True si el acceso se ha registrado.")
    message: str = Field(..., description="Resultado de la operación.")
    data: ResourceAccessDTO = Field(..., description="Acceso registrado.")


class ResourceAccessMatchDTO(ResourceAccessDTO):
    """A stored access found by similarity, with its score."""
    similarity: float = Field(..., description="Similitud coseno con el texto buscado (1 = idéntico).")

    @classmethod
    def from_match(cls, match: ResourceAccessMatch) -> "ResourceAccessMatchDTO":
        return cls(**ResourceAccessDTO.from_domain(match.access).model_dump(), similarity=match.similarity)


class ResourceAccessListResponseDTO(BaseModel):
    """Response of GET by username / resource: result of the operation and the accesses found (newest first)."""
    success: bool = Field(..., description="True si la consulta se ha realizado.")
    message: str = Field(..., description="Resultado de la operación.")
    results: List[ResourceAccessDTO] = Field(default_factory=list, description="Accesos encontrados.")


class ResourceAccessSimilarResponseDTO(BaseModel):
    """Response of GET by similarity: result of the operation and the matches (most similar first)."""
    success: bool = Field(..., description="True si la consulta se ha realizado.")
    message: str = Field(..., description="Resultado de la operación.")
    results: List[ResourceAccessMatchDTO] = Field(default_factory=list, description="Accesos similares encontrados.")
