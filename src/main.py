from contextlib import asynccontextmanager
import contextlib
import os
from typing import AsyncGenerator

from fastapi import Depends, FastAPI
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_consumer_update import KafkaUpdateConsumerService
from resources_orchester.infrastructure.messaging.messaging_adapter import ActivatorConsumer, active_kafka_consumer_update
from src.resources_orchester.infrastructure.api_router.router import router

# @contextlib.asynccontextmanager
@asynccontextmanager
async def lifespan(app: FastAPI):

    async with active_kafka_consumer_update() as consumer:
        yield consumer

app = FastAPI(
    title="general resources orchestator",
    description="application for manage de RAG to use from multiple MS and MCPs, give an API to update and revies the resources and share updates to MCPs",
    version="0.0.1",
    lifespan=lifespan
)

app.include_router(router)

@app.get("/")
async def root():
    return {
        "message": app.description,
        "version": app.version
    }