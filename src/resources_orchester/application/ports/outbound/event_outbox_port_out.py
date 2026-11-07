from typing import Protocol

from resources_orchester.domain.events.resource_updated_event import ResourceUpdatedEvent


class EventOutboxPortOut(Protocol):
    """
    Transactional outbox (driven port): records an event in the SAME unit of work as the data
    change. It is published after the commit by the outbox relay (at-least-once delivery), so a
    rolled-back or failed transaction never publishes anything.
    """

    async def add(self, event: ResourceUpdatedEvent) -> None:
        ...
