"""
Integration tests (Testcontainers / PostgreSQL) of the use cases wired with the REAL persistence
adapters and the REAL SqlAlchemyUnitOfWork over the Postgres test container (fixtures in
test/conftest.py). The test session runs inside an outer transaction: a use case's commit only
releases a savepoint, and everything is rolled back when the test ends.
"""
import uuid
from typing import List

import pytest
import pytest_asyncio

from resources_orchester.application.ports.outbound.application_query_port_out import ApplicationQueryPortOut
from resources_orchester.application.ports.outbound.event_outbox_port_out import EventOutboxPortOut
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut
from resources_orchester.application.ports.outbound.reference_query_port_out import ReferenceQueryPortOut
from resources_orchester.application.ports.outbound.resource_query_port_out import ResourceQueryPortOut
from resources_orchester.application.ports.outbound.unit_of_work_port import UnitOfWorkPort
from resources_orchester.application.use_cases.apply_resource_update import ApplyResourceUpdate
from resources_orchester.application.use_cases.create_message_resource import CreateMessageResource
from resources_orchester.application.use_cases.search_resources import SearchResources
from resources_orchester.domain.api_dtos.data_field import DataField
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.events.resource_updated_event import ResourceUpdatedEvent
from resources_orchester.domain.exception.domain_exceptions import NotFoundException, ValidationException
from resources_orchester.domain.model.data_field_resource import DataFieldResource
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.domain.model.resource_update import ResourceUpdate
from resources_orchester.infrastructure.persistence.repositories.outbox_repository import OutboxRepository

from test.infraestructure.utils_tester import create_random_data_resources


@pytest_asyncio.fixture
async def search_resources(resource_query_port_out: ResourceQueryPortOut) -> SearchResources:
    return SearchResources(resource_query_port_out=resource_query_port_out)


@pytest_asyncio.fixture
async def create_message_resource(unit_of_work: UnitOfWorkPort, application_query_port_out: ApplicationQueryPortOut,
                                  reference_command_port_out: ReferenceCommandPortOut,
                                  event_outbox_port_out: EventOutboxPortOut) -> CreateMessageResource:
    return CreateMessageResource(unit_of_work=unit_of_work, application_query_port_out=application_query_port_out,
                                 reference_command_port_out=reference_command_port_out, event_outbox_port_out=event_outbox_port_out)


@pytest_asyncio.fixture
async def apply_resource_update(unit_of_work: UnitOfWorkPort, reference_query_port_out: ReferenceQueryPortOut,
                                reference_command_port_out: ReferenceCommandPortOut) -> ApplyResourceUpdate:
    return ApplyResourceUpdate(unit_of_work=unit_of_work, reference_query_port_out=reference_query_port_out,
                               reference_command_port_out=reference_command_port_out)


async def _pending_events(session_connect) -> List[ResourceUpdatedEvent]:
    rows = await OutboxRepository(session_connect).lock_pending(1000)
    return [ResourceUpdatedEvent.model_validate(row.payload) for row in rows]


async def _seed(reference_command_port_out: ReferenceCommandPortOut, name: str, competence: str) -> DataResources:
    resource = create_random_data_resources()
    resource.name = name
    resource.competence = competence
    await reference_command_port_out.save_data_dictionary_reference(resource)
    return resource


@pytest.mark.asyncio
async def test_search_resources_by_name_returns_partial_matches(search_resources: SearchResources, reference_command_port_out: ReferenceCommandPortOut, session_connect):
    token = uuid.uuid4().hex
    await _seed(reference_command_port_out, f"customer_{token}_created", "messaging")
    await _seed(reference_command_port_out, f"customer_{token}_deleted", "database")
    await _seed(reference_command_port_out, f"invoice_{uuid.uuid4().hex}", "messaging")

    results: List[DataResources] = await search_resources.search_resources_by_name(f"CUSTOMER_{token}")

    assert [r.name for r in results] == [f"customer_{token}_created", f"customer_{token}_deleted"]
    assert all(isinstance(r, DataResources) for r in results)
    assert await _pending_events(session_connect) == []  # read-only use case: no event recorded


@pytest.mark.asyncio
async def test_search_resources_by_name_and_type(search_resources: SearchResources, reference_command_port_out: ReferenceCommandPortOut):
    token = uuid.uuid4().hex
    await _seed(reference_command_port_out, f"stock_{token}", "messaging")
    await _seed(reference_command_port_out, f"stock_{token}_table", "database")

    results = await search_resources.search_resources_by_name(token, DataUse.DB_TABLE)

    assert [(r.name, r.competence) for r in results] == [(f"stock_{token}_table", "database")]


@pytest.mark.asyncio
async def test_search_resources_by_name_trims_the_search_term(search_resources: SearchResources, reference_command_port_out: ReferenceCommandPortOut):
    token = uuid.uuid4().hex
    await _seed(reference_command_port_out, f"payment_{token}", "messaging")

    results = await search_resources.search_resources_by_name(f"   {token}  ")

    assert [r.name for r in results] == [f"payment_{token}"]


@pytest.mark.asyncio
async def test_search_resources_by_name_without_matches(search_resources: SearchResources):
    assert await search_resources.search_resources_by_name(f"nothing_{uuid.uuid4().hex}") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("blank_name", ["", "   ", None])
async def test_search_resources_by_name_rejects_blank_names(search_resources: SearchResources, blank_name):
    """A blank term would match every row; the use case rejects it instead of dumping the table."""
    with pytest.raises(ValidationException):
        await search_resources.search_resources_by_name(blank_name)


@pytest.mark.asyncio
async def test_resource_created_by_the_use_case_can_be_found_by_name(create_message_resource: CreateMessageResource,
                                                                   search_resources: SearchResources, session_connect):
    """Round trip: CreateMessageResource commits the name (and the event in the outbox), SearchResources finds it."""
    token = uuid.uuid4().hex
    payload = MessagesRequestDTO(
        name=f"question_asked_{token}",
        description=["es_ES pregunta emitida", "en_US question emitted"],
        fields_add=[DataField(name="question_id", type="uuid", description=["identificador"], validations=["mandatory"])],
    )

    created = await create_message_resource.create_message_resource(payload)
    results = await search_resources.search_resources_by_name(f"ASKED_{token}", DataUse.ASYNC_MESSAGE)

    assert len(results) == 1
    found = results[0]
    assert found.name == created.name
    assert found.description == created.description
    assert found.fields == created.fields
    assert (found.competence, found.type_data) == (created.competence, created.type_data)
    # the event was recorded in the outbox in the same transaction (published later by the relay)
    assert await _pending_events(session_connect) == [
        ResourceUpdatedEvent(resource=created, family=FamilyResource.DATAS, data_use=DataUse.ASYNC_MESSAGE)]


@pytest.mark.asyncio
async def test_failed_creation_leaves_neither_resource_nor_event(create_message_resource: CreateMessageResource,
                                                                 search_resources: SearchResources, session_connect,
                                                                 application_query_port_out: ApplicationQueryPortOut, monkeypatch):
    """Everything happens in one unit of work: if it fails before the commit, nothing remains."""
    async def unknown_application(name):
        return None
    monkeypatch.setattr(application_query_port_out, "get_application_by_name", unknown_application)

    with pytest.raises(NotFoundException):
        await create_message_resource.create_message_resource(
            MessagesRequestDTO(name=f"orphan_{uuid.uuid4().hex}", description=["d"]))

    assert await _pending_events(session_connect) == []


@pytest.mark.asyncio
async def test_apply_resource_update_replaces_the_stored_resource(apply_resource_update: ApplyResourceUpdate,
                                                                  reference_command_port_out: ReferenceCommandPortOut,
                                                                  search_resources: SearchResources, session_connect):
    token = uuid.uuid4().hex
    await _seed(reference_command_port_out, f"ticket_{token}", "messaging")
    await _seed(reference_command_port_out, f"ticket_{token}", "database")  # same name, other type: untouched
    update = ResourceUpdate(name=f"ticket_{token}", data_use=DataUse.ASYNC_MESSAGE, description={"es_ES": "actualizado"},
                            fields=[DataFieldResource(name="ticket_id", type="uuid", description={"es_ES": "pk"})])

    stored = await apply_resource_update.apply(update)
    results = {r.competence: r for r in await search_resources.search_resources_by_name(token)}

    assert stored.resource.description == {"es_ES": "actualizado"}
    assert (results["messaging"].description, results["messaging"].fields) == (update.description, update.fields)
    assert results["database"].description != update.description
    assert await _pending_events(session_connect) == []  # applying an external update does not publish


@pytest.mark.asyncio
async def test_apply_resource_update_unknown_or_ambiguous(apply_resource_update: ApplyResourceUpdate,
                                                         reference_command_port_out: ReferenceCommandPortOut, session_connect):
    token = uuid.uuid4().hex
    await _seed(reference_command_port_out, f"dup_{token}", "messaging")
    await _seed(reference_command_port_out, f"dup_{token}", "messaging")
    # Commit the seed data (releases a savepoint here): a failed unit of work rolls back EVERYTHING
    # uncommitted in its session, which would otherwise include these seeds.
    await session_connect.commit()

    with pytest.raises(NotFoundException):
        await apply_resource_update.apply(ResourceUpdate(name=f"missing_{token}", data_use=DataUse.ASYNC_MESSAGE, description={}, fields=[]))
    with pytest.raises(ValidationException):
        await apply_resource_update.apply(ResourceUpdate(name=f"dup_{token}", data_use=DataUse.ASYNC_MESSAGE, description={}, fields=[]))
