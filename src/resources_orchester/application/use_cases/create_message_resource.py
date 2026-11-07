from resources_orchester.application.ports.inbound.create_message_resource_port_in import CreateMessageResourcePortIn
from resources_orchester.application.ports.outbound.application_query_port_out import ApplicationQueryPortOut
from resources_orchester.application.ports.outbound.event_outbox_port_out import EventOutboxPortOut
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut
from resources_orchester.application.ports.outbound.unit_of_work_port import UnitOfWorkPort
from resources_orchester.application.use_cases.mappers import descriptions_to_dict, fields_to_domain
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.events.resource_updated_event import ResourceUpdatedEvent
from resources_orchester.domain.exception.domain_exceptions import NotFoundException
from resources_orchester.domain.model.data_resources import DataResources


class CreateMessageResource(CreateMessageResourcePortIn):
    """
    Registers a messaging resource, links it to its application and records the
    ResourceUpdatedEvent in the outbox, all in ONE unit of work committed before returning.

    * If anything fails (including the commit) nothing is stored and nothing is published.
    * Publishing to Kafka happens after the commit (outbox relay), so a broker outage does not
      prevent creating resources: the event is delivered when Kafka is reachable again.
    """

    def __init__(self, unit_of_work: UnitOfWorkPort, application_query_port_out: ApplicationQueryPortOut,
                 reference_command_port_out: ReferenceCommandPortOut, event_outbox_port_out: EventOutboxPortOut):
        self._uow = unit_of_work
        self._application_query_port_out = application_query_port_out
        self._reference_command_port_out = reference_command_port_out
        self._event_outbox_port_out = event_outbox_port_out

    async def create_message_resource(self, payload_message: MessagesRequestDTO) -> DataResources:
        data_resources = DataResources(
            name=payload_message.name or "Unknown Message Resource",
            description=descriptions_to_dict(payload_message.description),
            competence="messaging",
            process_apply=payload_message.process_apply_add if payload_message.process_apply_add else None,
            type_data="json",
            fields=fields_to_domain(payload_message.fields_add),
        )
        name_application = "MS_producer_question" if data_resources.process_apply and data_resources.process_apply[0] == "some_ms" else "MS_listener_question"

        async with self._uow:
            application = await self._application_query_port_out.get_application_by_name(name_application)
            if application is None:
                raise NotFoundException(f"La aplicación '{name_application}' no está registrada.")

            stored = await self._reference_command_port_out.save_data_dictionary_reference(data_resources)
            await self._reference_command_port_out.save_data_reference_application(stored, application)
            await self._event_outbox_port_out.add(ResourceUpdatedEvent(
                resource=data_resources,
                family=FamilyResource.DATAS,
                data_use=DataUse.ASYNC_MESSAGE if data_resources.competence == "messaging" else DataUse.DB_TABLE,
            ))
            await self._uow.commit()

        return data_resources
