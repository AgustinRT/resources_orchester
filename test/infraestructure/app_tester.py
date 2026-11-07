"""Helpers to build the real FastAPI application (create_app) with injected test settings."""
from typing import Optional

import httpx
from fastapi import FastAPI

from resources_orchester.infrastructure.bootstrap.app_factory import create_app
from resources_orchester.infrastructure.bootstrap.settings import ApiSettings, DatabaseSettings, KafkaSettings, Settings

# Placeholder for tests that never reach the database (providers overridden / lifespan not run).
UNUSED_DATABASE = DatabaseSettings(host="db.invalid", port=5432, user="unused", password="unused", db="unused")


def build_test_app(database: Optional[DatabaseSettings] = None, kafka: Optional[KafkaSettings] = None,
                   api: Optional[ApiSettings] = None) -> FastAPI:
    return create_app(Settings(database=database or UNUSED_DATABASE,
                               kafka=kafka or KafkaSettings(),
                               api=api or ApiSettings()))


def client_for(app: FastAPI) -> httpx.AsyncClient:
    """
    In-process HTTP client. ASGITransport does NOT run the lifespan: tests that need the real
    app-scoped resources wrap the calls in ``app.router.lifespan_context(app)``.
    """
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
