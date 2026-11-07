"""
Integration tests (Kafka Testcontainer) for the messaging adapters.

Each test uses its own topics (``kafka_settings`` / ``unique_topic`` fixtures in conftest),
so tests sharing the session-wide Kafka container never read each other's messages.
Configuration is injected through KafkaSettings: no environment variables or env files.
"""
import asyncio
import contextlib
import socket
import time
from typing import AsyncGenerator, Generator, List, Optional

import pytest
import pytest_asyncio
from kafka import KafkaConsumer

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.exception.domain_exceptions import NotFoundException
from resources_orchester.domain.exception.infrastructure_exceptions import MessagingException
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.domain.model.resource_update import ResourceUpdate
from resources_orchester.domain.model.stored_data_resource import StoredDataResource
from resources_orchester.infrastructure.bootstrap.settings import KafkaSettings
from resources_orchester.infrastructure.messaging.dtos.message_resource_payload import MessageResourcePayload
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_admin import WorkerKafkaAdmin
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_consumer_update import KafkaUpdateConsumerService
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_producer import WorkerKafkaProducer
from resources_orchester.application.ports.outbound.resource_event_publisher_port_out import ResourceEventPublisherPortOut

from test.infraestructure.kafka_container_tester import consumer_tester, producer_tester, tcp_forwarder
from test.infraestructure.utils_tester import create_random_data_resources


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest_asyncio.fixture(scope="function")
async def producer_port_out(kafka_settings: KafkaSettings) -> AsyncGenerator[ResourceEventPublisherPortOut]:
    producer = WorkerKafkaProducer(kafka_settings)
    assert await producer.start(), "the producer must connect to the Kafka test container"
    yield producer
    await producer.close()


@pytest.fixture(scope="function")
def consumer_container(kafka_container, kafka_settings: KafkaSettings) -> Generator[KafkaConsumer]:
    consumer = consumer_tester(kafka_container, kafka_settings.update_topic)
    yield consumer
    consumer.close()


@pytest.mark.asyncio
async def test_simply_kafka_producer(producer_port_out: ResourceEventPublisherPortOut, consumer_container: KafkaConsumer):
    """Resources published through the port really reach the broker (review D4)."""
    message_processed: int = 0
    data_message: DataResources = create_random_data_resources()
    await producer_port_out.publish_resource_updated(data_message, FamilyResource.DATAS, DataUse.ASYNC_MESSAGE)

    data_message_2: DataResources = create_random_data_resources()
    await producer_port_out.publish_resource_updated(data_message_2, FamilyResource.DATAS, DataUse.ASYNC_MESSAGE)

    list_names: list[str] = [data_message.name, data_message_2.name]

    for message in consumer_container:
        message_consumed: MessageResourcePayload = MessageResourcePayload.from_message_resources(message.value)
        assert message_consumed.source in list_names
        message_processed += 1

    assert 2 == message_processed


@pytest.mark.asyncio
async def test_published_payload_contract(producer_port_out: ResourceEventPublisherPortOut, consumer_container: KafkaConsumer):
    """The JSON on the wire carries the family, the type and the full resource content."""
    resource = create_random_data_resources()
    await producer_port_out.publish_resource_updated(resource, FamilyResource.DATAS, DataUse.DB_TABLE)

    values = [message.value for message in consumer_container]

    assert len(values) == 1
    assert values[0]["family_resource"] == "datas"
    assert values[0]["type_resource"] == "DB_TABLE"
    assert values[0]["source"] == resource.name
    assert values[0]["content"]["description"] == resource.description
    assert values[0]["content"]["fields"] == [field.model_dump() for field in resource.fields]


@pytest.mark.asyncio
async def test_send_message_to_request(kafka_container, kafka_settings: KafkaSettings, producer_port_out: ResourceEventPublisherPortOut):
    request_topic = next(t for t in kafka_settings.producer_topics if t != kafka_settings.update_topic)
    await producer_port_out.send_message_to_request({"topic": request_topic}, {"action": "reindex", "id": 7})

    consumer = consumer_tester(kafka_container, request_topic)
    try:
        assert [message.value for message in consumer] == [{"action": "reindex", "id": 7}]
    finally:
        consumer.close()


@pytest.mark.asyncio
async def test_send_message_to_request_rejects_unknown_topics(producer_port_out: ResourceEventPublisherPortOut):
    with pytest.raises(MessagingException):
        await producer_port_out.send_message_to_request({"topic": "not-configured"}, {"x": 1})


@pytest.mark.asyncio
async def test_publish_with_broker_unreachable_raises_messaging_exception():
    """No broker: start() is non-fatal, and publishing fails fast with a domain exception (no silent print)."""
    producer = WorkerKafkaProducer(KafkaSettings(bootstrap_servers=f"127.0.0.1:{_free_port()}"))
    assert await producer.start() is False

    started = time.perf_counter()
    with pytest.raises(MessagingException):
        await producer.publish_resource_updated(create_random_data_resources(), FamilyResource.DATAS, DataUse.ASYNC_MESSAGE)
    assert time.perf_counter() - started < 10

    await producer.close()


@pytest.mark.asyncio
async def test_send_failures_reported_by_kafka_raise_messaging_exception(producer_port_out: ResourceEventPublisherPortOut):
    """
    Errors raised by Kafka while SENDING (connection already up) are not swallowed either.
    A message above max_request_size (1 MiB) is a deterministic example (MessageSizeTooLargeError).
    """
    resource = create_random_data_resources()
    resource.description = {"es_ES": "x" * (2 * 1024 * 1024)}

    with pytest.raises(MessagingException):
        await producer_port_out.publish_resource_updated(resource, FamilyResource.DATAS, DataUse.ASYNC_MESSAGE)


@pytest.mark.asyncio
async def test_producer_reconnects_lazily_once_the_broker_is_reachable(kafka_container, kafka_settings: KafkaSettings):
    """
    Kafka down at startup, up later: the application-scoped producer connects on the next publish
    (no restart needed). "Up later" is simulated by opening a TCP forwarder to the test broker on
    the bootstrap port only after the failed start.
    """
    port = _free_port()
    settings = kafka_settings.model_copy(update={"bootstrap_servers": f"127.0.0.1:{port}"})
    producer = WorkerKafkaProducer(settings)
    assert await producer.start() is False

    async with tcp_forwarder(port, kafka_container):
        consumer = consumer_tester(kafka_container, settings.update_topic)
        try:
            resource = create_random_data_resources()
            await producer.publish_resource_updated(resource, FamilyResource.DATAS, DataUse.ASYNC_MESSAGE)
            assert [message.value["source"] for message in consumer] == [resource.name]
        finally:
            consumer.close()
            await producer.close()


class RecordingApplyResourceUpdate:
    """Fake ApplyResourceUpdatePortIn: records the updates it receives (or fails for a given name)."""

    def __init__(self, fail_for: Optional[str] = None):
        self.applied: List[ResourceUpdate] = []
        self.factory_calls = 0
        self._fail_for = fail_for

    async def apply(self, update: ResourceUpdate) -> StoredDataResource:
        if update.name == self._fail_for:
            raise NotFoundException(f"no resource {update.name}")
        self.applied.append(update)
        resource = DataResources(name=update.name, description=update.description, competence="messaging",
                                 type_data="json", fields=update.fields)
        return StoredDataResource(dictionary_id=1, data_use=update.data_use, resource=resource)

    def factory(self):
        """Stands in for the composition-root factory: one use case (session / UoW) per message."""
        recorder = self

        @contextlib.asynccontextmanager
        async def open_use_case():
            recorder.factory_calls += 1
            yield recorder
        return open_use_case


def _update_message(resource: DataResources) -> dict:
    return MessageResourcePayload.from_data_resources(
        {"family_destination": FamilyResource.DATAS, "resource_value": DataUse.ASYNC_MESSAGE}, resource).model_dump(mode="json")


@pytest.mark.asyncio
async def test_consumer_applies_updates_through_the_use_case_and_stops_on_cancel(kafka_container, kafka_settings: KafkaSettings):
    """
    Each message is validated, converted to a ResourceUpdate and handed to ApplyResourceUpdatePortIn
    obtained from the factory (one per message). Invalid messages and updates the use case rejects are
    skipped without stopping the consumer; cancelling the task stops it cleanly.
    """
    use_case = RecordingApplyResourceUpdate(fail_for="rejected_by_use_case")
    task = asyncio.create_task(KafkaUpdateConsumerService(kafka_settings, use_case.factory()).run())
    producer = producer_tester(kafka_container)
    valid = create_random_data_resources()
    rejected = create_random_data_resources()
    rejected.name = "rejected_by_use_case"
    try:
        # Without a consumer group the consumer starts at the latest offset, so keep producing the
        # sequence (poison, rejected, valid) until it is positioned and the valid update arrives.
        deadline = time.monotonic() + 30
        while not use_case.applied and time.monotonic() < deadline:
            for payload in ({"not": "an update"}, _update_message(rejected), _update_message(valid)):
                producer.send(kafka_settings.consumer_topics[0], payload)
            producer.flush()
            await asyncio.sleep(0.5)

        assert use_case.applied, "the valid update must reach the use case"
        update = use_case.applied[0]
        assert (update.name, update.data_use, update.description, update.fields) == (
            valid.name, DataUse.ASYNC_MESSAGE, valid.description, valid.fields)
        assert all(u.name == valid.name for u in use_case.applied)  # poison/rejected never applied
        assert use_case.factory_calls >= 2  # one use case (session / UoW) per applicable message
        assert not task.done(), "poison and rejected messages must not stop the consumer"
    finally:
        producer.close()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)

    assert task.done()


@pytest.mark.asyncio
async def test_consumer_service_keeps_retrying_while_the_broker_is_down():
    """Kafka down: run() does not crash (it retries with backoff) and cancellation still stops it."""
    settings = KafkaSettings(bootstrap_servers=f"127.0.0.1:{_free_port()}", consumer_retry_backoff_seconds=0.1)
    task = asyncio.create_task(KafkaUpdateConsumerService(settings, RecordingApplyResourceUpdate().factory()).run())

    await asyncio.sleep(1)
    assert not task.done(), "the consumer must keep retrying instead of failing"

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    assert task.cancelled()


def test_admin_creates_the_configured_topics(kafka_settings: KafkaSettings):
    admin = WorkerKafkaAdmin(kafka_settings)

    created = admin.ensure_topics()

    expected = set(kafka_settings.producer_topics + kafka_settings.consumer_topics)
    assert set(created) == expected
    assert admin.ensure_topics() == []  # idempotent
