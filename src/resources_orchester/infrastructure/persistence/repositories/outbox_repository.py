from datetime import datetime, timezone
from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.application.ports.outbound.event_outbox_port_out import EventOutboxPortOut
from resources_orchester.domain.events.resource_updated_event import ResourceUpdatedEvent
from resources_orchester.infrastructure.persistence.entities.outbox_event import OutboxEvent

RESOURCE_UPDATED = "resource_updated"


class OutboxRepository(EventOutboxPortOut):
    """
    Transactional outbox on PostgreSQL.

    * ``add`` (EventOutboxPortOut) is used by use cases: it only flushes, so the event becomes
      visible exactly when the surrounding unit of work commits.
    * The other methods are used by the outbox relay (infrastructure only).
    """

    def __init__(self, db_session: AsyncSession):
        self._db_session = db_session

    async def add(self, event: ResourceUpdatedEvent) -> None:
        self._db_session.add(OutboxEvent(event_type=RESOURCE_UPDATED, payload=event.model_dump(mode="json"), attempts=0))
        await self._db_session.flush()

    async def lock_pending(self, limit: int) -> List[OutboxEvent]:
        """
        Oldest unpublished events, locked until the relay's transaction ends. SKIP LOCKED lets
        several relays (instances) run concurrently without publishing the same event twice at once.
        """
        stmt = (select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None), OutboxEvent.failed_at.is_(None))
                .order_by(OutboxEvent.event_id)
                .limit(limit)
                .with_for_update(skip_locked=True))
        result = await self._db_session.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    def mark_published(event: OutboxEvent) -> None:
        event.published_at = datetime.now(timezone.utc)
        event.last_error = None

    @staticmethod
    def mark_failed(event: OutboxEvent, error: Exception) -> None:
        """Retryable failure (e.g. Kafka down): the event stays pending."""
        event.attempts += 1
        event.last_error = f"{type(error).__name__}: {error}"[:2000]

    @classmethod
    def mark_dead(cls, event: OutboxEvent, error: Exception) -> None:
        """Permanent failure (unknown type / invalid payload): removed from the pending queue."""
        cls.mark_failed(event, error)
        event.failed_at = datetime.now(timezone.utc)
