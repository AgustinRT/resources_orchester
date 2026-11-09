"""
Configuration (review D7): settings are loaded through pydantic-settings from env vars and
dotenv files located relative to the package, never relative to the working directory.
"""
import os

import pytest
from pydantic import ValidationError

from resources_orchester.infrastructure.bootstrap.settings import (
    API_ENV_FILE,
    DATABASE_ENV_FILE,
    EMBEDDING_ENV_FILE,
    KAFKA_ENV_FILE,
    ApiSettings,
    DatabaseSettings,
    EmbeddingSettings,
    KafkaSettings,
    ResourceAccessSettings,
    load_settings,
)

_SETTINGS_ENV_PREFIXES = ("POSTGRES_", "KAFKA_", "API_", "EMBEDDING_", "RESOURCE_ACCESS_")


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    """Remove settings-related variables that the CI / developer shell could define."""
    for name in list(os.environ):
        if name.startswith(_SETTINGS_ENV_PREFIXES) or name == "PORT":
            monkeypatch.delenv(name)


def test_env_files_exist_next_to_the_code():
    assert DATABASE_ENV_FILE.is_file() and KAFKA_ENV_FILE.is_file() and API_ENV_FILE.is_file()
    assert EMBEDDING_ENV_FILE.is_file()


def test_settings_do_not_depend_on_the_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # the old code located its config with rglob() from the CWD

    settings = load_settings()

    assert (settings.database.user, settings.database.db) == ("admin", "resourcesdb")
    assert settings.kafka.producer_topics == ["update", "delete", "create"]
    assert settings.kafka.consumer_topics == ["report"]
    assert settings.api.search_resource == "/search"


def test_environment_variables_override_env_files(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-1:9092,broker-2:9092")
    monkeypatch.setenv("API_PREFIX", "/v2/resources")

    settings = load_settings()

    assert settings.database.host == "db.internal"
    assert settings.kafka.bootstrap_servers == "broker-1:9092,broker-2:9092"
    assert settings.api.prefix == "/v2/resources"


def test_database_port_accepts_legacy_PORT_and_POSTGRES_PORT_wins(monkeypatch):
    monkeypatch.setenv("PORT", "6543")
    assert DatabaseSettings().port == 6543

    monkeypatch.setenv("POSTGRES_PORT", "7654")
    assert DatabaseSettings().port == 7654


def test_database_url_uses_psycopg_and_hides_the_password():
    settings = DatabaseSettings(host="h", port=1234, user="u", password="s3cr3t", db="d")

    url = settings.sqlalchemy_url()

    assert url.render_as_string(hide_password=False) == "postgresql+psycopg://u:s3cr3t@h:1234/d"
    assert "s3cr3t" not in repr(settings)


def test_kafka_topic_lists_are_comma_separated(monkeypatch):
    monkeypatch.setenv("KAFKA_PRODUCER_TOPICS", "update, audit ,")
    monkeypatch.setenv("KAFKA_CONSUMER_TOPICS", "report,alerts")

    settings = KafkaSettings()

    assert settings.producer_topics == ["update", "audit"]
    assert settings.consumer_topics == ["report", "alerts"]


def test_update_topic_must_be_a_producer_topic():
    with pytest.raises(ValidationError, match="update_topic"):
        KafkaSettings(producer_topics=["delete"], update_topic="update")


def test_api_settings_defaults_match_env_file():
    settings = ApiSettings()
    assert (settings.prefix, settings.new_resource, settings.search_resource) == (
        "/v1/resources", "/{id_user}/messages/create", "/search")
    assert (settings.new_resource_access, settings.search_resource_access, settings.similar_resource_access) == (
        "/access", "/access", "/access/similar")


def test_embedding_settings_default_to_voyage_without_a_committed_key():
    settings = EmbeddingSettings()

    assert (settings.provider, settings.model, settings.base_url, settings.timeout_seconds) == ("voyage", "voyage-3.5", None, 30)
    assert settings.api_key is None  # the key never lives in the dotenv file
    assert "EMBEDDING_API_KEY=" not in EMBEDDING_ENV_FILE.read_text().replace("# ", "")


def test_switching_the_embedding_model_only_needs_configuration(monkeypatch):
    monkeypatch.setenv("EMBEDDING_PROVIDER", "ollama")
    monkeypatch.setenv("EMBEDDING_MODEL", "nomic-embed-text")
    monkeypatch.setenv("EMBEDDING_BASE_URL", "http://ollama:11434")

    settings = load_settings().embedding

    assert (settings.provider, settings.model, settings.base_url) == ("ollama", "nomic-embed-text", "http://ollama:11434")


def test_embedding_api_key_comes_from_the_environment_and_is_secret(monkeypatch):
    monkeypatch.setenv("EMBEDDING_API_KEY", "vk-secret")

    settings = EmbeddingSettings()

    assert settings.api_key is not None and settings.api_key.get_secret_value() == "vk-secret"
    assert "vk-secret" not in repr(settings)


@pytest.mark.parametrize("name, value", [("EMBEDDING_PROVIDER", "other"), ("EMBEDDING_MODEL", ""),
                                         ("EMBEDDING_TIMEOUT_SECONDS", "0")])
def test_invalid_embedding_settings_are_rejected(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        EmbeddingSettings()


def test_resource_access_search_values_default_to_limit_10_and_similarity_075():
    settings = ResourceAccessSettings()
    assert (settings.list_limit, settings.similarity_limit, settings.min_similarity) == (10, 10, 0.75)


def test_resource_access_search_values_are_configurable(monkeypatch):
    monkeypatch.setenv("RESOURCE_ACCESS_SIMILARITY_LIMIT", "3")
    monkeypatch.setenv("RESOURCE_ACCESS_MIN_SIMILARITY", "0.9")
    monkeypatch.setenv("RESOURCE_ACCESS_LIST_LIMIT", "50")

    settings = load_settings().resource_access

    assert (settings.list_limit, settings.similarity_limit, settings.min_similarity) == (50, 3, 0.9)


@pytest.mark.parametrize("name, value", [("RESOURCE_ACCESS_MIN_SIMILARITY", "1.5"), ("RESOURCE_ACCESS_MIN_SIMILARITY", "-0.1"),
                                         ("RESOURCE_ACCESS_SIMILARITY_LIMIT", "0"), ("RESOURCE_ACCESS_LIST_LIMIT", "0")])
def test_invalid_resource_access_search_values_are_rejected(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        ResourceAccessSettings()
