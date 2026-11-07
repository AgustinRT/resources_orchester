"""
OutboxRelay against the PostgreSQL Testcontainer with a fake publisher (no Kafka needed).
The relay commits for real, so the outbox/resource tables are truncated after each test.
"""
import asyncio
import time
import uuid
from typing import List, Optional

import pytest
from sqlalchemy import select

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.events.resource_updated_event import ResourceUpdatedEvent
from resources_orchester.domain.exception.infrastructure_exceptions import MessagingException
from resources_orchester.infrastructure.bootstrap.settings import KafkaSettings
from resources_orchester.infrastructure.messaging.outbox_relay import OutboxRelay
from resources_orchester.infrastructure.persistence.entities.outbox_event import OutboxEvent
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from resources_orchester.infrastructure.persistence.repositories.outbox_repository import OutboxRepository

from test.infraestructure.utils_tester import create_random_data_resources

pytestmark = pytest.mark.usefixtures("truncate_resource_tables")


class FakePublisher:
    """ResourceEventPublisherPortOut double: records names; can fail for some names or be slow."""

    def __init__(self, fail_for: Optional[set] = None, delay: float = 0.0):
        self.published: List[str] = []
        self.fail_for = fail_for or set()
        self.delay = delay

    async def publish_resource_updated(self, resource, family, data_use) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if resource.name in self.fail_for:
            raise MessagingException(f"Kafka down for {resource.name}")
        self.published.append(resource.name)

    async def send_message_to_request(self, destiny_message, message) -> None:
        raise NotImplementedError


def _settings(batch_size: int = 100, poll: float = 30.0) -> KafkaSettings:
    return KafkaSettings(outbox_batch_size=batch_size, outbox_poll_interval_seconds=poll)


async def _add_events(database: DatabaseManager, names: List[str]) -> None:
    async with database.session() as session:
        outbox = OutboxRepository(session)
        for name in names:
            resource = create_random_data_resources()
            resource.name = name
            await outbox.add(ResourceUpdatedEvent(resource=resource, family=FamilyResource.DATAS, data_use=DataUse.ASYNC_MESSAGE))


async def _rows(database: DatabaseManager) -> List[OutboxEvent]:
    async with database.session() as session:
        return list((await session.execute(select(OutboxEvent).order_by(OutboxEvent.event_id))).scalars().all())


def _names(count: int) -> List[str]:
    token = uuid.uuid4().hex[:6]
    return [f"evt_{token}_{i}" for i in range(count)]


@pytest.mark.asyncio
async def test_relay_publishes_pending_events_in_order_once(database_manager: DatabaseManager):
    names = _names(5)
    await _add_events(database_manager, names)
    publisher = FakePublisher()
    relay = OutboxRelay(database_manager, publisher, _settings(batch_size=2))

    assert await relay.relay_pending() == 5
    assert await relay.relay_pending() == 0  # already published: never sent twice

    assert publisher.published == names
    assert all(row.published_at is not None and row.failed_at is None for row in await _rows(database_manager))


@pytest.mark.asyncio
async def test_relay_keeps_order_and_retries_when_kafka_fails(database_manager: DatabaseManager):
    first, second, third = names = _names(3)
    await _add_events(database_manager, names)
    publisher = FakePublisher(fail_for={second})
    relay = OutboxRelay(database_manager, publisher, _settings())

    assert await relay.relay_pending() == 1
    rows = await _rows(database_manager)
    assert publisher.published == [first]  # stops at the failure: `third` is not sent before `second`
    assert [(r.published_at is not None, r.attempts) for r in rows] == [(True, 0), (False, 1), (False, 0)]
    assert "Kafka down" in rows[1].last_error

    publisher.fail_for.clear()  # Kafka is back
    assert await relay.relay_pending() == 2
    assert publisher.published == names
    assert all(r.published_at is not None for r in await _rows(database_manager))


@pytest.mark.asyncio
async def test_poison_events_are_dead_lettered_and_do_not_block_the_queue(database_manager: DatabaseManager):
    """A full batch of unpublishable rows ahead of a good event: the cycle terminates and the good event goes out."""
    async with database_manager.session() as session:
        session.add_all([OutboxEvent(event_type="unknown_type", payload={}, attempts=0),
                         OutboxEvent(event_type="resource_updated", payload={"not": "an event"}, attempts=0),
                         OutboxEvent(event_type="resource_updated", payload={"resource": {}}, attempts=0)])
    good = _names(1)
    await _add_events(database_manager, good)
    publisher = FakePublisher()
    relay = OutboxRelay(database_manager, publisher, _settings(batch_size=2))

    assert await asyncio.wait_for(relay.relay_pending(), timeout=10) == 1

    rows = await _rows(database_manager)
    assert publisher.published == good
    assert [(r.failed_at is not None, r.published_at is not None) for r in rows] == [(True, False)] * 3 + [(False, True)]
    assert "Unknown outbox event type" in rows[0].last_error


@pytest.mark.asyncio
async def test_notify_wakes_the_relay_without_waiting_for_the_poll_interval(database_manager: DatabaseManager):
    publisher = FakePublisher()
    relay = OutboxRelay(database_manager, publisher, _settings(poll=30.0))
    task = asyncio.create_task(relay.run())
    try:
        await asyncio.sleep(0.3)  # first cycle done: now waiting up to 30 s
        names = _names(1)
        await _add_events(database_manager, names)
        started = time.monotonic()
        relay.notify()
        while not publisher.published and time.monotonic() - started < 5:
            await asyncio.sleep(0.05)
        assert publisher.published == names
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_concurrent_relays_publish_each_event_exactly_once(database_manager: DatabaseManager):
    """Several instances (relays) can run at the same time: FOR UPDATE SKIP LOCKED avoids double publishing."""
    names = _names(12)
    await _add_events(database_manager, names)
    publisher = FakePublisher(delay=0.02)
    relays = [OutboxRelay(database_manager, publisher, _settings(batch_size=3)) for _ in range(3)]

    counts = await asyncio.gather(*(relay.relay_pending() for relay in relays))

    assert sum(counts) == 12
    assert sorted(publisher.published) == sorted(names)  # no duplicates, none missing
