"""
Entry point: ``fastapi run src/main.py`` / ``uvicorn src.main:app``.
The application is assembled by the composition root (infrastructure/bootstrap).
"""
from resources_orchester.infrastructure.bootstrap.app_factory import create_app

app = create_app()
