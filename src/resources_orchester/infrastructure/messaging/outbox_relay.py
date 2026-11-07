import asyncio
import contextlib
import logging
from typing import Tuple

from resources_orchester.application.ports.outbound.resource_event_publisher_port_out import ResourceEventPublisherPortOut
from resources_orchester.domain.events.resource_updated_event import ResourceUpdatedEvent
from resources_orchester.domain.exception.infrastructure_exceptions import MessagingException
from resources_orchester.infrastructure.bootstrap.settings import KafkaSettings
from resources_orchester.infrastructure.persistence.entities.outbox_event import OutboxEvent
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from resources_orchester.infrastructure.persistence.repositories.outbox_repository import RESOURCE_UPDATED, OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    """
    Publishes the committed events of the transactional outbox to Kafka (at-least-once).

    * Events are published in ``event_id`` order. If Kafka fails, the batch stops (order is kept),
      the attempt is recorded (``attempts``, ``last_error``) and it is retried on the next cycle.
    * A row that can never be published (unknown type / invalid payload) is dead-lettered
      (``failed_at``) and leaves the queue, so a poison row cannot block every later event.
      Every batch either stops or shrinks the pending set, so a relay cycle always terminates.
    * At-least-once: if the process dies after Kafka acknowledged an event but before it was
      marked as published, the event is sent again; consumers must be idempotent.
    * Runs as one background task (``run``): it wakes up on ``notify()`` (called after each
      commit by the unit of work) and, as a fallback, every ``outbox_poll_interval_seconds``.
    """

    def __init__(self, database: DatabaseManager, publisher: ResourceEventPublisherPortOut, settings: KafkaSettings):
        self._database = database
        self._publisher = publisher
        self._poll_interval = settings.outbox_poll_interval_seconds
        self._batch_size = settings.outbox_batch_size
        self._wake_up = asyncio.Event()

    def notify(self) -> None:
        """Something was committed: publish pending events now instead of waiting for the next poll."""
        self._wake_up.set()

    async def run(self) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake_up.wait(), timeout=self._poll_interval)
            self._wake_up.clear()
            try:
                await self.relay_pending()
            except Exception:
                # e.g. database unreachable: keep the relay alive and retry on the next cycle
                logger.exception("Outbox relay cycle failed; retrying in %.1fs", self._poll_interval)

    async def relay_pending(self) -> int:
        """Publishes every pending event it can. Returns how many were published."""
        total = 0
        while True:
            published, stop = await self._relay_batch()
            total += published
            if stop:
                return total

    async def _relay_batch(self) -> Tuple[int, bool]:
        """One transaction: lock a batch, publish it, record the outcome. Returns (published, stop)."""
        published = 0
        async with self._database.session() as session:  # commits the marks when the block ends
            outbox = OutboxRepository(session)
            events = await outbox.lock_pending(self._batch_size)
            for event in events:
                try:
                    await self._dispatch(event)
                except MessagingException as error:
                    outbox.mark_failed(event, error)
                    logger.warning("Outbox event %s not published yet (Kafka): %s", event.event_id, error)
                    return published, True
                except Exception as error:
                    outbox.mark_dead(event, error)
                    logger.error("Outbox event %s cannot be published; dead-lettered: %s", event.event_id, error)
                    continue
                outbox.mark_published(event)
                published += 1
            return published, len(events) < self._batch_size

    async def _dispatch(self, event: OutboxEvent) -> None:
        if event.event_type != RESOURCE_UPDATED:
            raise ValueError(f"Unknown outbox event type '{event.event_type}'")
        domain_event = ResourceUpdatedEvent.model_validate(event.payload)
        await self._publisher.publish_resource_updated(domain_event.resource, domain_event.family, domain_event.data_use)
