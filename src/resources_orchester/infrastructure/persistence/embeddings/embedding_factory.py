"""
Chooses the embedding model from the configuration (EmbeddingSettings / .env.embedding).
Switching model only needs new EMBEDDING_* values; adding a provider needs its client file and
one entry in ``_BUILDERS``. Repositories never know which provider is in use.
"""
from typing import Callable, Dict

from resources_orchester.infrastructure.bootstrap.settings import EmbeddingSettings
from resources_orchester.infrastructure.persistence.embeddings.embedding_client import EmbeddingClient
from resources_orchester.infrastructure.persistence.embeddings.ollama_embedding_client import OllamaEmbeddingClient
from resources_orchester.infrastructure.persistence.embeddings.voyage_embedding_client import VoyageEmbeddingClient


def _voyage(settings: EmbeddingSettings) -> EmbeddingClient:
    return VoyageEmbeddingClient(model=settings.model,
                                 api_key=settings.api_key.get_secret_value() if settings.api_key else None,
                                 base_url=settings.base_url, timeout_seconds=settings.timeout_seconds)


def _ollama(settings: EmbeddingSettings) -> EmbeddingClient:
    return OllamaEmbeddingClient(model=settings.model, base_url=settings.base_url,
                                 timeout_seconds=settings.timeout_seconds)


_BUILDERS: Dict[str, Callable[[EmbeddingSettings], EmbeddingClient]] = {
    "voyage": _voyage,
    "ollama": _ollama,
}


def build_embedding_client(settings: EmbeddingSettings) -> EmbeddingClient:
    try:
        builder = _BUILDERS[settings.provider]
    except KeyError:
        raise ValueError(f"Proveedor de embeddings desconocido: '{settings.provider}'. Opciones: {sorted(_BUILDERS)}") from None
    return builder(settings)
