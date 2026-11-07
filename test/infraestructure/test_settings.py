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
    KAFKA_ENV_FILE,
    ApiSettings,
    DatabaseSettings,
    KafkaSettings,
    load_settings,
)

_SETTINGS_ENV_PREFIXES = ("POSTGRES_", "KAFKA_", "API_")


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    """Remove settings-related variables that the CI / developer shell could define."""
    for name in list(os.environ):
        if name.startswith(_SETTINGS_ENV_PREFIXES) or name == "PORT":
            monkeypatch.delenv(name)


def test_env_files_exist_next_to_the_code():
    assert DATABASE_ENV_FILE.is_file() and KAFKA_ENV_FILE.is_file() and API_ENV_FILE.is_file()


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
