from typing import Any, Dict, Optional, Tuple

import httpx

from resources_orchester.infrastructure.persistence.embeddings.embedding_client import HttpEmbeddingClient


class OllamaEmbeddingClient(HttpEmbeddingClient):
    """
    Local Ollama server (free, no API key). The model must be pulled first (ollama pull <model>).
    POST {base_url}/api/embed  {"model": ..., "input": text}  ->  {"embeddings": [[...]]}
    """
    provider = "ollama"
    DEFAULT_BASE_URL = "http://localhost:11434"

    def __init__(self, model: str, base_url: Optional[str] = None, timeout_seconds: float = 30.0,
                 http_client: Optional[httpx.AsyncClient] = None):
        super().__init__(model, base_url or self.DEFAULT_BASE_URL, timeout_seconds, http_client)

    def _build_request(self, text: str) -> Tuple[str, Dict[str, Any], Dict[str, str]]:
        return "/api/embed", {"model": self._model, "input": text}, {}

    def _read_vector(self, body: Any) -> Any:
        return body["embeddings"][0]
