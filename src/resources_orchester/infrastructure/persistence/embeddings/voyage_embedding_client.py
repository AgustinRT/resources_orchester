from typing import Any, Dict, List, Optional, Tuple

import httpx

from resources_orchester.domain.exception.infrastructure_exceptions import EmbeddingException
from resources_orchester.infrastructure.persistence.embeddings.embedding_client import HttpEmbeddingClient


class VoyageEmbeddingClient(HttpEmbeddingClient):
    """
    Voyage AI embeddings API (the embeddings provider recommended by Anthropic).
    POST {base_url}/embeddings  {"input": [text], "model": ...}  ->  {"data": [{"embedding": [...]}]}
    """
    provider = "voyage"
    DEFAULT_BASE_URL = "https://api.voyageai.com/v1"

    def __init__(self, model: str, api_key: Optional[str], base_url: Optional[str] = None,
                 timeout_seconds: float = 30.0, http_client: Optional[httpx.AsyncClient] = None):
        super().__init__(model, base_url or self.DEFAULT_BASE_URL, timeout_seconds, http_client)
        self._api_key = api_key

    async def get_embed(self, text: str) -> List[float]:
        if not self._api_key:
            raise EmbeddingException(f"{self.model_name}: falta EMBEDDING_API_KEY (clave de Voyage AI).")
        return await super().get_embed(text)

    def _build_request(self, text: str) -> Tuple[str, Dict[str, Any], Dict[str, str]]:
        return "/embeddings", {"input": [text], "model": self._model}, {"Authorization": f"Bearer {self._api_key}"}

    def _read_vector(self, body: Any) -> Any:
        return body["data"][0]["embedding"]
