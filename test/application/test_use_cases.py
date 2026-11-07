"""
Unit tests of the use cases with ALL their ports faked (no database, no Kafka).

* Port mocks use MagicMock(spec=<Protocol>): only methods declared by the port are accepted,
  and ``async def`` methods become AsyncMock.
* FakeUnitOfWork records the transaction calls in the same ``calls`` list as the ports, so the
  tests can assert the ORDER (event recorded in the outbox BEFORE commit, nothing after a failure).
"""
from typing import List, Optional
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from resources_orchester.application.ports.outbound.application_query_port_out import ApplicationQueryPortOut
from resources_orchester.application.ports.outbound.event_outbox_port_out import EventOutboxPortOut
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut
from resources_orchester.application.ports.outbound.reference_query_port_out import ReferenceQueryPortOut
from resources_orchester.application.use_cases.apply_resource_update import ApplyResourceUpdate
from resources_orchester.application.use_cases.create_message_resource import CreateMessageResource
from resources_orchester.application.use_cases.draft_resources import CreateDatabaseResource, CreateRestResource, UpdateMessageResource
from resources_orchester.domain.api_dtos.data_field import DataField
from resources_orchester.domain.api_dtos.database_request_dto import DatabaseRequestDTO
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.api_dtos.rest_request_dto import RestRequestDTO
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.events.resource_updated_event import ResourceUpdatedEvent
from resources_orchester.domain.exception.domain_exceptions import NotFoundException, ValidationException
from resources_orchester.domain.exception.infrastructure_exceptions import PersistenceException
from resources_orchester.domain.model.application import Application
from resources_orchester.domain.model.data_field_resource import DataFieldResource
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.domain.model.resource_update import ResourceUpdate
from resources_orchester.domain.model.stored_data_resource import StoredDataResource


class FakeUnitOfWork:
    """In-memory UnitOfWorkPort that records its calls; ``fail_commit_with`` simulates a commit failure."""

    def __init__(self, calls: List[str], fail_commit_with: Optional[Exception] = None):
        self.calls = calls
        self.fail_commit_with = fail_commit_with
        self.committed = False

    async def __aenter__(self):
        self.calls.append("uow.begin")
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.calls.append("uow.end")

    async def commit(self):
        self.calls.append("uow.commit")
        if self.fail_commit_with is not None:
            raise self.fail_commit_with
        self.committed = True

    async def rollback(self):
        self.calls.append("uow.rollback")


APPLICATION = Application(application_id=2, name="MS_listener_question", description="d", type_application_id=2)


def _recording(mock, calls: List[str], name: str, return_value=None):
    async def side_effect(*args, **kwargs):
        calls.append(name)
        return return_value
    mock.side_effect = side_effect
    return mock


def _stored(resource: DataResources, dictionary_id: int = 10) -> StoredDataResource:
    return StoredDataResource(dictionary_id=dictionary_id, data_use=DataUse.ASYNC_MESSAGE, resource=resource)


@pytest.fixture
def calls() -> List[str]:
    return []


@pytest.fixture
def ports(calls):
    application_query = MagicMock(spec=ApplicationQueryPortOut)
    reference_command = MagicMock(spec=ReferenceCommandPortOut)
    event_outbox = MagicMock(spec=EventOutboxPortOut)
    _recording(application_query.get_application_by_name, calls, "get_application", APPLICATION)
    _recording(reference_command.save_data_dictionary_reference, calls, "save_reference",
               _stored(DataResources(name="x", description={}, competence="messaging", type_data="json", fields=[])))
    _recording(reference_command.save_data_reference_application, calls, "link_application")
    _recording(event_outbox.add, calls, "outbox.add")
    return application_query, reference_command, event_outbox


def _create_message_resource(calls, ports, fail_commit_with=None):
    application_query, reference_command, event_outbox = ports
    uow = FakeUnitOfWork(calls, fail_commit_with)
    use_case = CreateMessageResource(unit_of_work=uow, application_query_port_out=application_query,
                                     reference_command_port_out=reference_command, event_outbox_port_out=event_outbox)
    return use_case, uow


# =======================================================================
# MOCK HELPERS: DTOs y Modelos (Para simular datos de prueba sin crear archivos temporales)
# =======================================================================

def create_mock_data_field() -> DataField:
    """Crea un DataField mock para usar en los payloads."""
    mockedFields: DataField = DataField(name="test", description=["lorem Ipson Desc"], type="string", validations=["not null", "not blanck"])
    return mockedFields

def create_mock_messages_dto() -> MessagesRequestDTO:
    """Crea un payload de mensajes básico y válido."""
    # Usamos el modelo real, pero llenando con datos mockeados.
    return MessagesRequestDTO(
                                name="test_resource",
                                description=["es_ES example", "en_US example"],
                                process_apply_add=[],
                                process_apply_del=[],
                                fields_add=[],
                                fields_del=[]
                            )


# =======================================================================
# CreateMessageResource
# =======================================================================

@pytest.mark.asyncio
async def test_create_message_resource_happy_path(calls, ports):
    """
    TEST HAPPY PATH: Verifica que un DTO de mensaje se transforma correctamente en DataResources
    y que todo (registro, asociación y evento en el outbox) ocurre dentro de UNA unidad de trabajo
    confirmada al final.
    """
    use_case, uow = _create_message_resource(calls, ports)
    payload = create_mock_messages_dto()
    payload.fields_add.append(create_mock_data_field())

    result = await use_case.create_message_resource(payload)

    assert isinstance(result, DataResources)
    assert result.name == "test_resource"
    assert result.competence == "messaging"
    assert result.type_data == "json"
    assert len(result.fields) > 0
    assert calls == ["uow.begin", "get_application", "save_reference", "link_application", "outbox.add", "uow.commit", "uow.end"]
    assert uow.committed


@pytest.mark.asyncio
async def test_create_message_resource_records_the_event_in_the_outbox(calls, ports):
    """The use case does not publish: it records the domain event for the relay (transactional outbox)."""
    use_case, _ = _create_message_resource(calls, ports)

    result = await use_case.create_message_resource(create_mock_messages_dto())

    _, _, event_outbox = ports
    event_outbox.add.assert_awaited_once_with(
        ResourceUpdatedEvent(resource=result, family=FamilyResource.DATAS, data_use=DataUse.ASYNC_MESSAGE))


@pytest.mark.asyncio
async def test_message_create_empty_fields_and_optional_data(calls, ports):
    """TEST EDGE CASE: Prueba que el servicio maneje correctamente DTOs con campos opcionales vacíos."""
    use_case, _ = _create_message_resource(calls, ports)
    payload = create_mock_messages_dto()
    payload.process_apply_add = None

    result = await use_case.create_message_resource(payload)

    assert isinstance(result, DataResources)
    assert result.process_apply is None


@pytest.mark.asyncio
async def test_create_message_resource_without_fields(calls, ports):
    """fields_add es opcional: sin campos el recurso se crea igualmente (antes fallaba: 'NoneType' not iterable)."""
    use_case, uow = _create_message_resource(calls, ports)
    payload = create_mock_messages_dto()
    payload.fields_add = None

    result = await use_case.create_message_resource(payload)

    assert result.fields == []
    assert uow.committed


@pytest.mark.asyncio
async def test_create_message_resource_propagates_commit_failures(calls, ports):
    """
    TEST ERROR HANDLING (review D2): si el COMMIT falla, el caso de uso no informa de éxito: propaga
    PersistenceException. El evento solo se había registrado en el outbox de esa misma transacción,
    así que tampoco se publicará nada.
    """
    use_case, uow = _create_message_resource(calls, ports, fail_commit_with=PersistenceException("commit rejected"))

    with pytest.raises(PersistenceException):
        await use_case.create_message_resource(create_mock_messages_dto())

    assert calls[-3:] == ["outbox.add", "uow.commit", "uow.end"]
    assert not uow.committed


@pytest.mark.asyncio
async def test_create_message_resource_fails_if_the_application_is_not_registered(calls, ports):
    """
    El puerto devuelve None: el caso de uso lanza una excepción de dominio antes de escribir nada,
    no registra el evento y no confirma la transacción.
    """
    application_query, _, _ = ports
    _recording(application_query.get_application_by_name, calls, "get_application", None)
    use_case, uow = _create_message_resource(calls, ports)

    with pytest.raises(NotFoundException):
        await use_case.create_message_resource(create_mock_messages_dto())

    assert calls == ["uow.begin", "get_application", "uow.end"]
    assert not uow.committed


@pytest.mark.asyncio
async def test_create_message_resource_does_not_commit_if_a_write_fails(calls, ports):
    _, reference_command, _ = ports
    reference_command.save_data_reference_application.side_effect = PersistenceException("FK violation")
    use_case, uow = _create_message_resource(calls, ports)

    with pytest.raises(PersistenceException):
        await use_case.create_message_resource(create_mock_messages_dto())

    assert "outbox.add" not in calls and "uow.commit" not in calls
    assert calls[-1] == "uow.end"


# =======================================================================
# ApplyResourceUpdate (driven by Kafka)
# =======================================================================

def _apply_resource_update(calls, matches):
    reference_query = MagicMock(spec=ReferenceQueryPortOut)
    reference_command = MagicMock(spec=ReferenceCommandPortOut)
    _recording(reference_query.get_data_dictionary_by_name, calls, "find_by_name", matches)

    async def update(dictionary_id, resource):
        calls.append("update_reference")
        return StoredDataResource(dictionary_id=dictionary_id, data_use=DataUse.ASYNC_MESSAGE, resource=resource)
    reference_command.update_data_dictionary_reference.side_effect = update

    uow = FakeUnitOfWork(calls)
    use_case = ApplyResourceUpdate(unit_of_work=uow, reference_query_port_out=reference_query,
                                   reference_command_port_out=reference_command)
    return use_case, uow, reference_query


UPDATE = ResourceUpdate(name="orders", data_use=DataUse.ASYNC_MESSAGE, description={"es_ES": "nueva"},
                        fields=[DataFieldResource(name="id", type="uuid", description={"es_ES": "pk"})])
CURRENT = DataResources(name="orders", description={"es_ES": "vieja"}, competence="messaging", type_data="json", fields=[])


@pytest.mark.asyncio
async def test_apply_resource_update_replaces_description_and_fields(calls):
    use_case, uow, reference_query = _apply_resource_update(calls, [_stored(CURRENT, 7)])

    stored = await use_case.apply(UPDATE)

    reference_query.get_data_dictionary_by_name.assert_awaited_once_with("orders", DataUse.ASYNC_MESSAGE)
    assert stored.dictionary_id == 7
    assert stored.resource.name == "orders" and stored.resource.competence == "messaging"
    assert (stored.resource.description, stored.resource.fields) == (UPDATE.description, UPDATE.fields)
    assert calls == ["uow.begin", "find_by_name", "update_reference", "uow.commit", "uow.end"]


@pytest.mark.asyncio
async def test_apply_resource_update_for_an_unknown_resource_raises_not_found(calls):
    use_case, uow, _ = _apply_resource_update(calls, [])

    with pytest.raises(NotFoundException):
        await use_case.apply(UPDATE)
    assert not uow.committed


@pytest.mark.asyncio
async def test_apply_resource_update_rejects_ambiguous_names(calls):
    use_case, uow, _ = _apply_resource_update(calls, [_stored(CURRENT, 7), _stored(CURRENT, 8)])

    with pytest.raises(ValidationException):
        await use_case.apply(UPDATE)
    assert "update_reference" not in calls and not uow.committed


# =======================================================================
# Draft operations (no persistence yet)
# =======================================================================

@pytest.mark.asyncio
async def test_update_message_resource_happy_path():
    """
    TEST HAPPY PATH: Verifica que la actualización de un mensaje es procesada y devuelve el dominio actualizado.
    """
    payload = create_mock_messages_dto()
    payload.fields_add.append(create_mock_data_field())
    resource_name = "test_resource"

    result = await UpdateMessageResource().update_message_resource(resource_name, payload)

    assert isinstance(result, DataResources)
    # Verificamos que el nombre sigue siendo correcto y la competencia se mantuvo (sin cambiarlo).
    assert result.name == resource_name
    assert "updated-" in str(result.process_apply[0]) # Verifica la lógica de proceso en update


@pytest.mark.asyncio
async def test_create_database_and_rest_drafts_accept_fields():
    """They used to crash when fields were given (description built as str instead of a dict)."""
    field = create_mock_data_field()
    database = await CreateDatabaseResource().create_database_resource(
        DatabaseRequestDTO(description=["d"], schema_use="public", fields_add=[field]))
    rest = await CreateRestResource().create_rest_resource(
        RestRequestDTO(name="api", description=["d"], endpoints_request=[], endpoints_response=[], fields_add=[field]))

    assert database.name == "DB Resource public"  # it used to embed "<bound method BaseModel.schema ...>"
    assert database.fields[0].description == {"es_ES": "lorem Ipson Desc"}
    assert rest.fields[0].description == {"es_ES": "lorem Ipson Desc"}


def test_database_create_missing_description_fails():
    """
    TEST ERROR HANDLING: la descripción es obligatoria en DatabaseRequestDTO.
    Su ausencia se detecta en la validación del DTO (FastAPI responde 422) antes de llegar
    al servicio, por lo que el caso de uso nunca recibe un payload sin descripción.
    """
    with pytest.raises(ValidationError) as excinfo:
        DatabaseRequestDTO(
            name="fail",
            schema_use="test_schema",
            # description omitida a propósito
        )

    errors = excinfo.value.errors()
    assert [(e["loc"], e["type"]) for e in errors] == [(("description",), "missing")]
