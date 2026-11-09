import asyncio
import contextlib
import logging

from fastapi import FastAPI

from resources_orchester.infrastructure.bootstrap.dependencies import build_apply_resource_update_factory
from resources_orchester.infrastructure.bootstrap.settings import Settings
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_consumer_update import KafkaUpdateConsumerService
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_producer import WorkerKafkaProducer
from resources_orchester.infrastructure.messaging.outbox_relay import OutboxRelay
from resources_orchester.infrastructure.persistence.embeddings.embedding_factory import build_embedding_client
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager

logger = logging.getLogger(__name__)


def _log_unexpected_exit(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.error("Background task %s stopped unexpectedly", task.get_name(), exc_info=task.exception())


async def _stop(task: asyncio.Task) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Creates the application-scoped resources ONCE, exposes them through ``app.state`` for the
    providers in bootstrap/dependencies.py, and releases them on shutdown.

    Kafka is not required to start or to create resources: events wait in the transactional
    outbox and the relay publishes them when Kafka is reachable; the consumer keeps retrying.
    """
    settings: Settings = app.state.settings

    database = DatabaseManager(settings.database)
    producer = WorkerKafkaProducer(settings.kafka)
    relay = OutboxRelay(database, producer, settings.kafka)
    consumer = KafkaUpdateConsumerService(settings.kafka, build_apply_resource_update_factory(database))
    embedding_client = build_embedding_client(settings.embedding)
    logger.info("Embedding model: %s", embedding_client.model_name)
    if settings.embedding.provider == "voyage" and settings.embedding.api_key is None:
        logger.warning("EMBEDDING_API_KEY is not set: registering and similarity search of accesses will answer 503")

    await producer.start()
    relay_task = asyncio.create_task(relay.run(), name="outbox-relay")
    consumer_task = asyncio.create_task(consumer.run(), name="kafka-update-consumer")
    for task in (relay_task, consumer_task):
        task.add_done_callback(_log_unexpected_exit)

    app.state.database = database
    app.state.kafka_producer = producer
    app.state.outbox_relay = relay
    app.state.outbox_relay_task = relay_task
    app.state.kafka_consumer_task = consumer_task
    app.state.embedding_client = embedding_client
    try:
        yield
    finally:
        # stop the tasks that use the producer / pool before closing them
        await _stop(consumer_task)
        await _stop(relay_task)
        await producer.close()
        await embedding_client.close()
        await database.dispose()
        logger.info("Application resources released (consumer, outbox relay and producer stopped, embedding client closed, DB pool disposed)")
