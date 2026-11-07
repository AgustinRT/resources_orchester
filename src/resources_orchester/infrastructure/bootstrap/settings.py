"""
Application configuration (pydantic-settings), loaded ONCE at startup by the composition root.

Sources, highest priority first:
  1. Values passed explicitly to the constructor (tests inject settings this way).
  2. Process environment variables.
  3. The dotenv files below, located relative to THIS module (never relative to the
     current working directory, so the service behaves the same wherever it is started).
     Missing files are ignored, e.g. when the package is installed in an image and all
     configuration comes from the environment.

Nothing else in the code base reads os.environ or dotenv files.
"""
from pathlib import Path
from typing import Annotated, List

from pydantic import AliasChoices, BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from sqlalchemy.engine import URL

_PACKAGE_DIR = Path(__file__).resolve().parents[2]        # src/resources_orchester
PROJECT_ROOT = _PACKAGE_DIR.parents[1]                    # repository root (source checkout)

DATABASE_ENV_FILE = PROJECT_ROOT / ".env"
KAFKA_ENV_FILE = _PACKAGE_DIR / "infrastructure" / "messaging" / ".env.messaging.dev"
API_ENV_FILE = _PACKAGE_DIR / "infrastructure" / "api_router" / ".env.api"


class DatabaseSettings(BaseSettings):
    """PostgreSQL connection and pool. Env vars: POSTGRES_* (port also accepts legacy PORT)."""
    model_config = SettingsConfigDict(env_prefix="POSTGRES_", env_file=DATABASE_ENV_FILE,
                                      extra="ignore", populate_by_name=True)

    host: str = "localhost"
    port: int = Field(5432, validation_alias=AliasChoices("POSTGRES_PORT", "PORT"))
    user: str
    password: SecretStr
    db: str
    pool_size: int = Field(5, ge=1)
    max_overflow: int = Field(10, ge=0)
    pool_timeout_seconds: float = Field(30.0, gt=0)
    pool_pre_ping: bool = True

    def sqlalchemy_url(self) -> URL:
        return URL.create("postgresql+psycopg", username=self.user, password=self.password.get_secret_value(),
                          host=self.host, port=self.port, database=self.db)


class KafkaSettings(BaseSettings):
    """Kafka clients. Env vars: KAFKA_* (topic lists are comma separated: "update,delete,create")."""
    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=KAFKA_ENV_FILE, extra="ignore")

    bootstrap_servers: str = "localhost:9092"
    consumer_group_id: str = "mcps"
    producer_topics: Annotated[List[str], NoDecode] = ["update", "delete", "create"]
    consumer_topics: Annotated[List[str], NoDecode] = ["report"]
    update_topic: str = "update"
    request_timeout_ms: int = Field(40_000, gt=0)
    consumer_retry_backoff_seconds: float = Field(5.0, gt=0)
    outbox_poll_interval_seconds: float = Field(1.0, gt=0)
    outbox_batch_size: int = Field(100, ge=1)

    @field_validator("producer_topics", "consumer_topics", mode="before")
    @classmethod
    def _split_comma_separated(cls, value):
        if isinstance(value, str):
            return [topic.strip() for topic in value.split(",") if topic.strip()]
        return value

    @model_validator(mode="after")
    def _update_topic_is_a_producer_topic(self):
        if self.update_topic not in self.producer_topics:
            raise ValueError(f"update_topic '{self.update_topic}' must be one of producer_topics {self.producer_topics}")
        return self


class ApiSettings(BaseSettings):
    """HTTP routes. Env vars: API_PREFIX, API_NEW_RESOURCE, API_SEARCH_RESOURCE."""
    model_config = SettingsConfigDict(env_prefix="API_", env_file=API_ENV_FILE, extra="ignore")

    prefix: str = "/v1/resources"
    new_resource: str = "/{id_user}/messages/create"
    search_resource: str = "/search"


class Settings(BaseModel):
    """Aggregate handed to the composition root (create_app / lifespan / providers)."""
    database: DatabaseSettings
    kafka: KafkaSettings
    api: ApiSettings


def load_settings() -> Settings:
    """Reads every settings group from its sources. Called once per application instance."""
    return Settings(database=DatabaseSettings(), kafka=KafkaSettings(), api=ApiSettings())
