"""Deterministic stand-in for the AI model (no network): tests choose the vector of each text."""
from typing import Dict, List, Optional

from resources_orchester.domain.exception.infrastructure_exceptions import EmbeddingException
from resources_orchester.infrastructure.persistence.embeddings.embedding_client import EmbeddingClient

FAKE_MODEL_NAME = "fake/test-embedding-model"


class FakeEmbeddingClient(EmbeddingClient):
    """
    ``vectors`` maps a text to its vector; other texts get ``default_vector``.
    ``fail_with`` makes every call raise (simulates the provider being down). ``calls`` records the texts.
    """

    def __init__(self, vectors: Optional[Dict[str, List[float]]] = None, default_vector: Optional[List[float]] = None,
                 model_name: str = FAKE_MODEL_NAME, fail_with: Optional[EmbeddingException] = None):
        self.vectors = dict(vectors or {})
        self.default_vector = default_vector or [1.0, 0.0, 0.0]
        self._model_name = model_name
        self.fail_with = fail_with
        self.calls: List[str] = []
        self.closed = False

    @property
    def model_name(self) -> str:
        return self._model_name

    async def get_embed(self, text: str) -> List[float]:
        self.calls.append(text)
        if self.fail_with is not None:
            raise self.fail_with
        return list(self.vectors.get(text, self.default_vector))

    async def close(self) -> None:
        self.closed = True
