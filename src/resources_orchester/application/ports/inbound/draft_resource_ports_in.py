"""
Inbound ports of the DRAFT operations: they only build the domain model from the DTO
(no persistence, no events) and no adapter exposes them yet.
"""
from typing import Protocol

from resources_orchester.domain.api_dtos.database_request_dto import DatabaseRequestDTO
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.api_dtos.rest_request_dto import RestRequestDTO
from resources_orchester.domain.model.data_resources import DataResources


class UpdateMessageResourcePortIn(Protocol):
    async def update_message_resource(self, resource_name: str, payload_message: MessagesRequestDTO) -> DataResources: ...


class CreateDatabaseResourcePortIn(Protocol):
    async def create_database_resource(self, payload_database: DatabaseRequestDTO) -> DataResources: ...


class UpdateDatabaseResourcePortIn(Protocol):
    async def update_database_resource(self, resource_name: str, payload_database: DatabaseRequestDTO) -> DataResources: ...


class CreateRestResourcePortIn(Protocol):
    async def create_rest_resource(self, payload_rest: RestRequestDTO) -> DataResources: ...


class UpdateRestResourcePortIn(Protocol):
    async def update_rest_resource(self, resource_name: str, payload_rest: RestRequestDTO) -> DataResources: ...
