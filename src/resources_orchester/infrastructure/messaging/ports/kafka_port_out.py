from typing import Protocol

from resources_orchester.domain.model.data_resources import DataResources
# Importación necesaria para tipar correctamente el payload del mensaje
from src.resources_orchester.infrastructure.messaging.dtos.message_resource_payload import MessageResourcePayload

class KafkaPortOut(Protocol):
    """
    Defines the outgoing communication port (Producer Port) for sending messages 
    via Kafka, adhering to the hexagonal architecture principles.
    """
    
    def send_message_to_update(self, destiny_message: dict, message: DataResources) -> None:
        """
        Sends a message intended to update a resource or state in a downstream service.
        
        Args:
            destiny_message (dict): The target destination identifier (e.g., topic name).
            message (MessageResourcePayload): The payload data containing the updated information.
        """
        ...  # Implementación concreta en la capa de infraestructura

    def send_message_to_request(self, destiny_message: dict, message: dict) -> None:
        """
        Sends a message intended to trigger an action or request processing 
        from a downstream service.
        
        Args:
            destiny_message (dict): The target destination identifier (e.g., topic name).
            message (dict): The payload data containing the required actions/parameters.
        """
        ... # Implementación concreta en la capa de infraestructura
