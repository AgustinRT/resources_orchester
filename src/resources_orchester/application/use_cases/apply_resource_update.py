from resources_orchester.application.ports.inbound.apply_resource_update_port_in import ApplyResourceUpdatePortIn
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut
from resources_orchester.application.ports.outbound.reference_query_port_out import ReferenceQueryPortOut
from resources_orchester.application.ports.outbound.unit_of_work_port import UnitOfWorkPort
from resources_orchester.domain.exception.domain_exceptions import NotFoundException, ValidationException
from resources_orchester.domain.model.resource_update import ResourceUpdate
from resources_orchester.domain.model.stored_data_resource import StoredDataResource


class ApplyResourceUpdate(ApplyResourceUpdatePortIn):
    """
    Applies an update received from another system (Kafka) to the registered resource with the
    same name and type: its description and fields are replaced, in one committed unit of work.

    Raises NotFoundException if no resource matches and ValidationException if several do
    (names are not unique, so an ambiguous update is rejected instead of guessing).
    It does not publish an event: the sender already knows about the change, and re-publishing
    could create update loops between systems.
    """

    def __init__(self, unit_of_work: UnitOfWorkPort, reference_query_port_out: ReferenceQueryPortOut,
                 reference_command_port_out: ReferenceCommandPortOut):
        self._uow = unit_of_work
        self._reference_query_port_out = reference_query_port_out
        self._reference_command_port_out = reference_command_port_out

    async def apply(self, update: ResourceUpdate) -> StoredDataResource:
        async with self._uow:
            matches = await self._reference_query_port_out.get_data_dictionary_by_name(update.name, update.data_use)
            if not matches:
                raise NotFoundException(f"No existe el recurso '{update.name}' de tipo {update.data_use.value}.")
            if len(matches) > 1:
                raise ValidationException(
                    f"Actualización ambigua: {len(matches)} recursos se llaman '{update.name}' ({update.data_use.value}).")

            current = matches[0]
            updated_resource = current.resource.model_copy(update={"description": update.description, "fields": update.fields})
            stored = await self._reference_command_port_out.update_data_dictionary_reference(current.dictionary_id, updated_resource)
            await self._uow.commit()

        return stored
