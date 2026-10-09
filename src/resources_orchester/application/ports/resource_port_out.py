from typing import Protocol

from src.resources_orchester.domain.model.data_resources import DataResources
from src.resources_orchester.domain.model.context_resource import ContextResource

class ResourcePortOut(Protocol):
    """Puerto de Salida (Output Port) que define las interacciones con sistemas externos."""
    def save_data_resource(self, adapter: PersistenceAdapter, data: DataResources) -> None: ...
    def publish_context_resource(self, adapter: PersistenceAdapter, context: ContextResource) -> bool: ...



