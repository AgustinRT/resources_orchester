from typing import Protocol
from resources_orchester.infrastructure.api_router.dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.infrastructure.api_router.dtos.database_request_dto import DatabaseRequestDTO
from resources_orchester.infrastructure.api_router.dtos.rest_request_dto import RestRequestDTO

from src.resources_orchester.domain.model.data_resources import DataResources


class ResourcePortIn(Protocol):
    """Puerto de Entrada (Input Port) para la lógica de negocio de los recursos."""
    
    def create_message_resource(self, payload_message: MessagesRequestDTO) -> DataResources: ...
    def update_message_resource(self, resource_name: str, payload_message: MessagesRequestDTO) -> DataResources: ...

    def create_database_resource(self, payload_database: DatabaseRequestDTO) -> DataResources: ...
    def update_database_resource(self, resource_name: str, payload_database: DatabaseRequestDTO) -> DataResources: ...

    def create_rest_resource(self, payload_rest: RestRequestDTO) -> DataResources: ...
    def update_rest_resource(self, resource_name: str, payload_rest: RestRequestDTO) -> DataResources: ...

