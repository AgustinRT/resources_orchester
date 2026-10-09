import threading
from typing import Annotated

from fastapi import Depends, FastAPI
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_consumer_update import KafkaUpdateConsumerService
from resources_orchester.infrastructure.messaging.messaging_adapter import active_kafka_consumer_update
from src.resources_orchester.infrastructure.api_router.router import router

app = FastAPI(
    title="general resources orchestator",
    description="application for manage de RAG to use from multiple MS and MCPs, give an API to update and revies the resources and share updates to MCPs",
    version="0.0.1"
)

app.include_router(router)

active_kafka_consumer_update()


@app.get("/")
async def root():
    return {
        "message": app.description,
        "version": app.version
    }