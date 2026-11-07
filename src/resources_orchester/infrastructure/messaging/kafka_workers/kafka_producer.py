import asyncio
import json
import logging
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from resources_orchester.application.ports.outbound.resource_event_publisher_port_out import ResourceEventPublisherPortOut
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.exception.infrastructure_exceptions import MessagingException
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.bootstrap.settings import KafkaSettings
from ..dtos.message_resource_payload import MessageResourcePayload

logger = logging.getLogger(__name__)


def _serialize(value: Any) -> bytes:
    return json.dumps(value).encode("utf-8")


class WorkerKafkaProducer(ResourceEventPublisherPortOut):
    """
    Implements ResourceEventPublisherPortOut: adapter that publishes messages to Kafka with aiokafka.

    Application-scoped: the composition root creates ONE instance, starts it in the lifespan
    and closes it on shutdown, so every request reuses the same producer (and broker
    connections) instead of opening a new one.

    If the broker is unreachable at startup the application still starts (read endpoints do
    not need Kafka); the connection is retried lazily on the next publish, and while Kafka
    stays down publishing raises MessagingException.
    """

    def __init__(self, settings: KafkaSettings):
        self._settings = settings
        self._producer: Optional[AIOKafkaProducer] = None
        self._start_lock = asyncio.Lock()

    async def start(self) -> bool:
        """Connects to the broker. Non-fatal: returns False (and logs) if Kafka is unreachable."""
        try:
            await self._ensure_started()
            return True
        except MessagingException as error:
            logger.warning("Kafka producer not started, will retry on the next publish: %s", error)
            return False

    async def _ensure_started(self) -> AIOKafkaProducer:
        if self._producer is not None:
            return self._producer
        async with self._start_lock:
            if self._producer is None:  # another coroutine may have started it while we waited
                producer = AIOKafkaProducer(
                    bootstrap_servers=self._settings.bootstrap_servers,
                    value_serializer=_serialize,
                    request_timeout_ms=self._settings.request_timeout_ms,
                )
                try:
                    await producer.start()
                except KafkaError as error:
                    await producer.stop()
                    raise MessagingException(
                        f"Kafka no disponible en '{self._settings.bootstrap_servers}': {error}") from error
                self._producer = producer
                logger.info("Kafka producer connected to %s", self._settings.bootstrap_servers)
        return self._producer

    async def publish_resource_updated(self, resource: DataResources, family: FamilyResource, data_use: DataUse) -> None:
        """Publishes the resource to the configured update topic and waits for the broker ack."""
        destination = {"family_destination": family, "resource_value": data_use}

        payload = MessageResourcePayload.from_data_resources(destination, resource).model_dump(mode="json")
        message: dict = {"destination": destination, "message_data": payload}
        await self._send(self._settings.update_topic, message)

    async def send_message_to_request(self, destiny_message: Dict[str, Any], message: Dict[str, Any]) -> None:
        """Sends a request/command message to ``destiny_message['topic']`` (must be a producer topic)."""
        topic = destiny_message.get("topic")
        if not topic or topic not in self._settings.producer_topics:
            raise MessagingException(
                f"Tópico '{topic}' no válido: debe ser uno de {self._settings.producer_topics}.")
        await self._send(topic, message)

    async def _send(self, topic: str, payload: Dict[str, Any]) -> None:
        producer = await self._ensure_started()
        try:
            await producer.send_and_wait(topic, value=payload)
        except KafkaError as error:
            raise MessagingException(f"No se pudo publicar en el tópico '{topic}': {error}") from error

    async def close(self) -> None:
        """Flushes pending messages and closes the broker connections (application shutdown)."""
        async with self._start_lock:
            if self._producer is not None:
                producer, self._producer = self._producer, None
                await producer.stop()


