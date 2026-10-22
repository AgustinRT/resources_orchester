from contextlib import asynccontextmanager
import contextlib
import os
from typing import Annotated, AsyncGenerator

from fastapi import Depends
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_admin import WorkerKafkaAdmin
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_consumer_update import KafkaUpdateConsumerService
from resources_orchester.infrastructure.messaging.ports.kafka_port_out import KafkaPortOut
from .kafka_workers.kafka_producer import WorkerKafkaProducer

async def get_messaging_port_out() -> KafkaPortOut:
    try:
        environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
        adapter = WorkerKafkaProducer(environment_execution)
        await adapter.init_producer()
        return adapter
    except Exception as e:
        # En caso de falla crítica al conectar, lanzamos la excepción para que FastAPI maneje el error.
        raise RuntimeError(f"Falló la inicialización del adaptador de mensajería (Kafka): {e}")

@contextlib.asynccontextmanager
async def active_kafka_consumer_update():
    environment_execution = os.getenv("ENVIROMENT_EXECUTION", "dev")
    consumer_update = KafkaUpdateConsumerService(environment_execution)
    await consumer_update.init_consumer()
    yield
    consumer_update.stop_consuming()

MessagingPortOut = Annotated[KafkaPortOut, Depends(get_messaging_port_out)]

ActivatorConsumer = Annotated[KafkaUpdateConsumerService, Depends(active_kafka_consumer_update)]
