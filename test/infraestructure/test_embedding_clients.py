"""
Embedding clients (Voyage AI, Ollama) and the provider factory. No network: httpx.MockTransport
answers the requests, so the tests check the exact request each provider receives and how every
kind of failure becomes EmbeddingException.
"""
import json
from typing import Callable, List

import httpx
import pytest

from resources_orchester.domain.exception.infrastructure_exceptions import EmbeddingException
from resources_orchester.infrastructure.bootstrap.settings import EmbeddingSettings
from resources_orchester.infrastructure.persistence.embeddings.embedding_factory import build_embedding_client
from resources_orchester.infrastructure.persistence.embeddings.ollama_embedding_client import OllamaEmbeddingClient
from resources_orchester.infrastructure.persistence.embeddings.voyage_embedding_client import VoyageEmbeddingClient

VOYAGE_URL = "https://api.voyageai.com/v1"
OLLAMA_URL = "http://localhost:11434"


def _http(handler: Callable[[httpx.Request], httpx.Response], base_url: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=base_url)


def _recording(response: httpx.Response, requests: List[httpx.Request]):
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return response
    return handler


def _voyage(handler, api_key="vk-test") -> VoyageEmbeddingClient:
    return VoyageEmbeddingClient(model="voyage-3.5", api_key=api_key, http_client=_http(handler, VOYAGE_URL))


def _ollama(handler) -> OllamaEmbeddingClient:
    return OllamaEmbeddingClient(model="nomic-embed-text", http_client=_http(handler, OLLAMA_URL))


# --- Voyage AI ------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_voyage_request_and_answer():
    requests: List[httpx.Request] = []
    answer = {"object": "list", "data": [{"object": "embedding", "embedding": [0.1, -0.2, 3], "index": 0}],
              "model": "voyage-3.5", "usage": {"total_tokens": 5}}
    client = _voyage(_recording(httpx.Response(200, json=answer), requests))

    assert await client.get_embed("¿Qué campos tiene?") == [0.1, -0.2, 3.0]
    assert client.model_name == "voyage/voyage-3.5"

    (request,) = requests
    assert (request.method, str(request.url)) == ("POST", "https://api.voyageai.com/v1/embeddings")
    assert request.headers["Authorization"] == "Bearer vk-test"
    assert json.loads(request.content) == {"input": ["¿Qué campos tiene?"], "model": "voyage-3.5"}
    await client.close()


@pytest.mark.asyncio
async def test_voyage_without_api_key_fails_without_calling_the_api():
    requests: List[httpx.Request] = []
    client = _voyage(_recording(httpx.Response(200, json={}), requests), api_key=None)

    with pytest.raises(EmbeddingException, match="EMBEDDING_API_KEY"):
        await client.get_embed("text")
    assert requests == []


# --- Ollama ---------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ollama_request_and_answer():
    requests: List[httpx.Request] = []
    client = _ollama(_recording(httpx.Response(200, json={"model": "nomic-embed-text", "embeddings": [[0.5, 0.25]]}), requests))

    assert await client.get_embed("hola") == [0.5, 0.25]
    assert client.model_name == "ollama/nomic-embed-text"

    (request,) = requests
    assert (request.method, str(request.url)) == ("POST", "http://localhost:11434/api/embed")
    assert "Authorization" not in request.headers
    assert json.loads(request.content) == {"model": "nomic-embed-text", "input": "hola"}


# --- Failures (shared HTTP handling) --------------------------------------------------------

def _raise(error: Exception):
    def handler(request: httpx.Request) -> httpx.Response:
        raise error
    return handler


@pytest.mark.asyncio
@pytest.mark.parametrize("build", [_voyage, _ollama], ids=["voyage", "ollama"])
@pytest.mark.parametrize("handler, expected", [
    (_raise(httpx.ConnectError("connection refused")), "no disponible"),
    (_raise(httpx.ReadTimeout("slow")), "timeout"),
    (lambda r: httpx.Response(401, json={"detail": "Invalid API key"}), "HTTP 401"),
    (lambda r: httpx.Response(429, text="rate limited"), "HTTP 429"),
    (lambda r: httpx.Response(503, text="overloaded"), "HTTP 503"),
    (lambda r: httpx.Response(200, text="not json"), "respuesta no válida"),
    (lambda r: httpx.Response(200, json={"unexpected": True}), "respuesta no válida"),
], ids=["unreachable", "timeout", "401", "429", "503", "not-json", "wrong-shape"])
async def test_every_failure_becomes_embedding_exception(build, handler, expected):
    client = build(handler)

    with pytest.raises(EmbeddingException, match=expected) as error:
        await client.get_embed("text")
    assert error.value.message.startswith(client.model_name)
    assert "vk-test" not in error.value.message  # the API key never appears in errors


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", [[], ["a", 1], [True, 1.0], None, "0.1,0.2"])
async def test_invalid_vectors_are_rejected(vector):
    client = _ollama(lambda r: httpx.Response(200, json={"embeddings": [vector]}))

    with pytest.raises(EmbeddingException, match="respuesta no válida"):
        await client.get_embed("text")


@pytest.mark.asyncio
async def test_non_finite_values_are_rejected():
    client = _ollama(lambda r: httpx.Response(200, content=b'{"embeddings": [[0.1, NaN]]}',
                                              headers={"Content-Type": "application/json"}))

    with pytest.raises(EmbeddingException, match="no finitos"):
        await client.get_embed("text")


# --- Factory: the provider is chosen by configuration ---------------------------------------

def test_factory_builds_voyage_from_settings():
    client = build_embedding_client(EmbeddingSettings(provider="voyage", model="voyage-3.5-lite", api_key="vk"))

    assert isinstance(client, VoyageEmbeddingClient)
    assert client.model_name == "voyage/voyage-3.5-lite"
    assert str(client._http.base_url) == "https://api.voyageai.com/v1/"


def test_factory_builds_ollama_from_settings_with_custom_url():
    client = build_embedding_client(EmbeddingSettings(provider="ollama", model="nomic-embed-text",
                                                      base_url="http://ollama:11434", timeout_seconds=5))

    assert isinstance(client, OllamaEmbeddingClient)
    assert client.model_name == "ollama/nomic-embed-text"
    assert str(client._http.base_url) == "http://ollama:11434"
    assert client._http.timeout.read == 5


def test_unknown_providers_are_rejected_by_the_settings():
    with pytest.raises(ValueError):
        EmbeddingSettings(provider="unknown", model="m")
