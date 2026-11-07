from typing import Protocol

from resources_orchester.domain.model.application import Application
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.domain.model.stored_data_resource import ResourceApplicationLink, StoredDataResource


class ReferenceCommandPortOut(Protocol):
    """Driven port (write side) over the data dictionary. Changes become durable when the unit of work commits."""

    async def save_data_dictionary_reference(self, data: DataResources) -> StoredDataResource:
        """Registers a new resource and returns it with its identity."""
        ...

    async def update_data_dictionary_reference(self, id_dictionary: int, data: DataResources) -> StoredDataResource:
        """Replaces the registered resource. Raises NotFoundException if ``id_dictionary`` does not exist."""
        ...

    async def delete_data_dictionary_reference(self, id_dictionary: int) -> bool:
        """Removes the registered resource; True if it existed."""
        ...

    async def save_data_reference_application(self, data_dictionary: StoredDataResource, application: Application) -> ResourceApplicationLink:
        """Links a registered resource to an application."""
        ...
