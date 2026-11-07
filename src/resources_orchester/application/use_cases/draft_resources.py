"""
DRAFT operations carried over from ResourceManagerGeneral: they only transform the DTO into the
domain model. They do not persist or publish anything and no endpoint exposes them yet; when one
is implemented it should get its own module, ports and provider like CreateMessageResource.
"""
from resources_orchester.application.ports.inbound.draft_resource_ports_in import (
    CreateDatabaseResourcePortIn,
    CreateRestResourcePortIn,
    UpdateDatabaseResourcePortIn,
    UpdateMessageResourcePortIn,
    UpdateRestResourcePortIn,
)
from resources_orchester.application.use_cases.mappers import descriptions_to_dict, fields_to_domain
from resources_orchester.domain.api_dtos.database_request_dto import DatabaseRequestDTO
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.api_dtos.rest_request_dto import RestRequestDTO
from resources_orchester.domain.model.data_resources import DataResources


class UpdateMessageResource(UpdateMessageResourcePortIn):
    async def update_message_resource(self, resource_name: str, payload_message: MessagesRequestDTO) -> DataResources:
        return DataResources(name=resource_name, description=descriptions_to_dict(payload_message.description),
                             competence="messaging", process_apply=[f"updated-{resource_name}"], type_data="json", fields=[])


class CreateDatabaseResource(CreateDatabaseResourcePortIn):
    async def create_database_resource(self, payload_database: DatabaseRequestDTO) -> DataResources:
        return DataResources(name=payload_database.name or f"DB Resource {payload_database.schema_use}",
                             description=descriptions_to_dict(payload_database.description),
                             competence="database", process_apply=None, type_data="dbRow",
                             fields=fields_to_domain(payload_database.fields_add))


class UpdateDatabaseResource(UpdateDatabaseResourcePortIn):
    async def update_database_resource(self, resource_name: str, payload_database: DatabaseRequestDTO) -> DataResources:
        return DataResources(name=resource_name, description=descriptions_to_dict(payload_database.description),
                             competence="database", process_apply=None, type_data="dbRow", fields=[])


class CreateRestResource(CreateRestResourcePortIn):
    async def create_rest_resource(self, payload_rest: RestRequestDTO) -> DataResources:
        return DataResources(name=payload_rest.name or "Unknown API Resource",
                             description=descriptions_to_dict(payload_rest.description),
                             competence="api_gateway", process_apply=None, type_data="http/json",
                             fields=fields_to_domain(payload_rest.fields_add))


class UpdateRestResource(UpdateRestResourcePortIn):
    async def update_rest_resource(self, resource_name: str, payload_rest: RestRequestDTO) -> DataResources:
        return DataResources(name=resource_name, description=descriptions_to_dict(payload_rest.description),
                             competence="api_gateway", process_apply=None, type_data="http/json", fields=[])
