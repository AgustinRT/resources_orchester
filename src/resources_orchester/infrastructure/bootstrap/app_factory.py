from typing import Optional

from fastapi import FastAPI

from resources_orchester.infrastructure.api_router.router import build_router
from resources_orchester.infrastructure.bootstrap.lifespan import lifespan
from resources_orchester.infrastructure.bootstrap.settings import Settings, load_settings


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    """
    Application factory. Settings are loaded once here (or injected, e.g. by tests) and kept in
    ``app.state.settings``; the lifespan builds the app-scoped resources from them.
    """
    settings = settings or load_settings()

    app = FastAPI(
        title="general resources orchestator",
        description="application for manage de RAG to use from multiple MS and MCPs, give an API to update and revies the resources and share updates to MCPs",
        version="0.0.1",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.include_router(build_router(settings.api))

    @app.get("/")
    async def root():
        return {
            "message": app.description,
            "version": app.version
        }

    return app
