from typing import Any, Dict, Protocol

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.model.data_resources import DataResources


class ResourceEventPublisherPortOut(Protocol):
    """
    Driven port for publishing events about resources to other systems (implemented with Kafka).

    Both operations only return once the broker has acknowledged the message. If it could not be
    delivered they raise ``MessagingException``: failures are never silently swallowed.
    """

    async def publish_resource_updated(self, resource: DataResources, family: FamilyResource, data_use: DataUse) -> None:
        """
        Publishes that ``resource`` was created/updated. The destination topic is part of the
        adapter's configuration, not of the business logic.
        """
        ...

    async def send_message_to_request(self, destiny_message: Dict[str, Any], message: Dict[str, Any]) -> None:
        """
        Sends a message intended to trigger an action in a downstream service.

        Args:
            destiny_message: must contain 'topic', one of the configured producer topics.
            message: JSON-serializable payload.
        """
        ...
