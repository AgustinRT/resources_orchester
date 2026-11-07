from typing import Protocol

from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.model.data_resources import DataResources


class CreateMessageResourcePortIn(Protocol):
    """Use case: register a messaging resource, link it to its application and notify other systems."""

    async def create_message_resource(self, payload_message: MessagesRequestDTO) -> DataResources: ...
