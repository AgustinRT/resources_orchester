import math
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, List, Optional, Tuple

import httpx

from resources_orchester.domain.exception.infrastructure_exceptions import EmbeddingException


class EmbeddingClient(ABC):
    """
    Gets the embedding (vector) of a text from an AI model. Repositories only consume ``get_embed``
    and ``model_name``: which model answers is decided by configuration (see embedding_factory).
    """

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Complete name of the model that builds the vectors ("<provider>/<model>"), stored with each vector."""

    @abstractmethod
    async def get_embed(self, text: str) -> List[float]:
        """Vector of ``text``. Raises EmbeddingException if it cannot be obtained."""

    async def close(self) -> None:
        """Releases the connections (application shutdown)."""


class HttpEmbeddingClient(EmbeddingClient):
    """
    Base for providers reached over HTTP: sends the request, maps every failure (unreachable,
    timeout, HTTP error, malformed answer) to EmbeddingException and validates the vector.
    Subclasses only describe their request and how to read the vector from the answer.
    """
    provider: ClassVar[str]

    def __init__(self, model: str, base_url: str, timeout_seconds: float,
                 http_client: Optional[httpx.AsyncClient] = None):
        self._model = model
        self._http = http_client or httpx.AsyncClient(base_url=base_url, timeout=timeout_seconds)

    @property
    def model_name(self) -> str:
        return f"{self.provider}/{self._model}"

    @abstractmethod
    def _build_request(self, text: str) -> Tuple[str, Dict[str, Any], Dict[str, str]]:
        """(path, JSON body, headers) of the embedding request."""

    @abstractmethod
    def _read_vector(self, body: Any) -> Any:
        """Extracts the vector from the decoded JSON answer (KeyError/IndexError/TypeError if absent)."""

    async def get_embed(self, text: str) -> List[float]:
        path, payload, headers = self._build_request(text)
        try:
            response = await self._http.post(path, json=payload, headers=headers)
        except httpx.TimeoutException as error:
            raise EmbeddingException(f"{self.model_name}: sin respuesta del modelo (timeout).") from error
        except httpx.HTTPError as error:
            raise EmbeddingException(f"{self.model_name}: modelo no disponible ({type(error).__name__}: {error}).") from error

        if response.is_error:
            raise EmbeddingException(
                f"{self.model_name}: el modelo respondió HTTP {response.status_code}: {response.text[:200]}")
        try:
            return _valid_vector(self._read_vector(response.json()))
        except (ValueError, KeyError, IndexError, TypeError) as error:
            raise EmbeddingException(f"{self.model_name}: respuesta no válida del modelo ({error}).") from error

    async def close(self) -> None:
        await self._http.aclose()


def _valid_vector(value: Any) -> List[float]:
    """A non-empty list of finite numbers; anything else is an invalid answer."""
    if not isinstance(value, list) or not value:
        raise ValueError("el embedding debe ser una lista no vacía de números")
    if not all(isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) for x in value):
        raise ValueError("el embedding contiene valores no numéricos o no finitos")
    return [float(x) for x in value]
