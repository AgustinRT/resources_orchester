import asyncio
import json
import logging
from contextlib import AbstractAsyncContextManager
from typing import Any, Callable

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from pydantic import ValidationError

from resources_orchester.application.ports.inbound.apply_resource_update_port_in import ApplyResourceUpdatePortIn
from resources_orchester.domain.exception.domain_exceptions import DomainException
from resources_orchester.domain.model.data_field_resource import DataFieldResource
from resources_orchester.domain.model.resource_update import ResourceUpdate
from resources_orchester.infrastructure.bootstrap.settings import KafkaSettings
from resources_orchester.infrastructure.messaging.dtos.message_resource_payload import MessageResourcePayload

logger = logging.getLogger(__name__)

# Provided by the composition root: opens ONE session / unit of work and yields the use case.
ApplyResourceUpdateFactory = Callable[[], AbstractAsyncContextManager[ApplyResourceUpdatePortIn]]


def to_resource_update(payload: MessageResourcePayload) -> ResourceUpdate:
    """Inbound message (same envelope this service publishes) -> domain command."""
    return ResourceUpdate(
        name=payload.source,
        data_use=payload.type_resource,
        description=payload.content.description,
        fields=[DataFieldResource.model_validate(field) for field in payload.content.fields],
    )


class KafkaUpdateConsumerService:
    """
    Consumer for state-change events on the configured consumer topics.

    Runs as ONE background task owned by the application lifespan (see ``run``):
    * if the broker is unreachable it retries with a fixed backoff instead of preventing the
      application from starting;
    * cancelling the task (application shutdown) stops the consumer and closes its connections.

    Each message is an update for a registered resource (MessageResourcePayload envelope). It is
    handed to the ApplyResourceUpdate use case (ApplyResourceUpdatePortIn), obtained from the
    composition root with its own session / unit of work per message. Messages that are invalid
    or cannot be applied are logged and skipped: they never stop the consumer.
    """

    def __init__(self, settings: KafkaSettings, apply_update_factory: ApplyResourceUpdateFactory):
        self._settings = settings
        self._topics_consumer = list(settings.consumer_topics)
        self._apply_update_factory = apply_update_factory

    def _build_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(*self._topics_consumer,
                                bootstrap_servers=self._settings.bootstrap_servers,
                                value_deserializer=lambda x: json.loads(x.decode("utf-8")))

    async def run(self) -> None:
        """Connect (retrying while Kafka is down) and consume until the task is cancelled."""
        backoff = self._settings.consumer_retry_backoff_seconds
        while True:
            consumer = self._build_consumer()
            try:
                await consumer.start()
            except KafkaError as error:
                await consumer.stop()
                logger.warning("Kafka consumer could not connect (%s); retrying in %.1fs", error, backoff)
                await asyncio.sleep(backoff)
                continue

            logger.info("Kafka consumer listening on %s", ", ".join(self._topics_consumer))
            try:
                async for message in consumer:
                    await self.process_message(message)
            except KafkaError as error:
                logger.warning("Kafka consumer error (%s); reconnecting in %.1fs", error, backoff)
                await asyncio.sleep(backoff)
            finally:
                await consumer.stop()

    async def process_message(self, message: Any) -> None:
        """Validates the message and applies it through the use case. Never raises (poison messages are skipped)."""
        if message.topic not in self._topics_consumer:
            logger.warning("Tópico desconocido o no gestionado en este consumidor: %s", message.topic)
            return
        try:
            update = to_resource_update(MessageResourcePayload.model_validate(message.value))
        except (ValidationError, TypeError, ValueError) as error:
            logger.warning("Mensaje descartado (%s offset %s): formato no válido: %s", message.topic, message.offset, error)
            return

        try:
            async with self._apply_update_factory() as apply_update:
                stored = await apply_update.apply(update)
            logger.info("Recurso '%s' (%s) actualizado desde %s (id %s)",
                        update.name, update.data_use.value, message.topic, stored.dictionary_id)
        except DomainException as error:
            logger.warning("Actualización no aplicada (%s offset %s): %s", message.topic, message.offset, error.message)
        except Exception:
            logger.exception("Error inesperado procesando %s offset %s", message.topic, message.offset)
