import pytest
import pytest_asyncio
import uuid

from resources_orchester.application.ports.outbound.resource_query_port_out import ResourceQueryPortOut
from resources_orchester.domain.model.data_resources import DataResources

from resources_orchester.domain.exception.domain_exceptions import NotFoundException
from resources_orchester.domain.model.application import Application
from resources_orchester.domain.model.stored_data_resource import ResourceApplicationLink, StoredDataResource
from resources_orchester.application.ports.outbound.application_query_port_out import ApplicationQueryPortOut
from resources_orchester.application.ports.outbound.reference_query_port_out import ReferenceQueryPortOut
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut

from resources_orchester.domain.enum.data_use import DataUse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator, List


from test.infraestructure.utils_tester import create_random_data_resources

@pytest.mark.asyncio
async def test_create_generic_data_resource(reference_command_port_out: ReferenceCommandPortOut):
    """
    Tests the creation and management of a generic data resource object 
    using the DataResources model, demonstrating domain layer usage within tests.
    """
    generic_resource = create_random_data_resources()

    response: StoredDataResource = await reference_command_port_out.save_data_dictionary_reference(generic_resource)

    assert response is not None, "Should successfully save resource data dictionary reference without fields."
    assert response.dictionary_id is not None


@pytest.mark.asyncio
async def test_save_data_dictionary_reference_no_fields(reference_command_port_out: ReferenceCommandPortOut):
    """
    Tests saving a resource when the 'fields' list is empty or missing, 
    ensuring graceful handling if fields are optional in this scenario.
    """
    # Create a resource without fields (or with an explicitly empty field list)
    generic_resource = create_random_data_resources()
    generic_resource.fields = []
    # Expect the save operation to succeed even without field definitions.
    response = await reference_command_port_out.save_data_dictionary_reference(generic_resource)
    assert response is not None, "Should successfully save resource data dictionary reference without fields."
    assert response.dictionary_id is not None

@pytest.mark.asyncio
async def test_create_data_reference_application(reference_command_port_out: ReferenceCommandPortOut, application_query_port_out: ApplicationQueryPortOut):
    """
    Tests the creation and management of a generic data resource object 
    using the DataResources model, demonstrating domain layer usage within tests.
    """    
    usable_apply: Application = await application_query_port_out.get_application(1)
    generic_resource = create_random_data_resources()
    new_data_dict: StoredDataResource = await reference_command_port_out.save_data_dictionary_reference(generic_resource)

    response: ResourceApplicationLink = await reference_command_port_out.save_data_reference_application(new_data_dict, usable_apply)

    assert response is not None, "Should successfully save resource data dictionary reference without fields."
    assert new_data_dict.dictionary_id == response.dictionary_id
    assert usable_apply.application_id == response.application_id

@pytest.mark.asyncio
async def test_get_data_dictionary_by_use(reference_command_port_out: ReferenceCommandPortOut, reference_query_port_out: ReferenceQueryPortOut):
    """
    Tests fetching data dictionaries based on a specific DataUse enum.
    Requires seeding multiple records with different uses.
    """
    # Sembrar registros en la base de datos simulando diferentes usos
    resource_db = create_random_data_resources()
    resource_rest = create_random_data_resources()
    
    resource_db.competence ="database"
    await reference_command_port_out.save_data_dictionary_reference(resource_db)

    resource_rest.competence ="rest"
    await reference_command_port_out.save_data_dictionary_reference(resource_rest)

    # 1. Test con DataUse.DB_TABLE
    results_db = await reference_query_port_out.get_data_dictionary_by_use(DataUse.DB_TABLE)
    assert len(results_db) == 1, f"Expected one result for DB_TABLE, but got {len(results_db)}"
    assert results_db[0].resource.description == resource_db.description

    # 2. Test con DataUse.REST_OBJ
    results_rest = await reference_query_port_out.get_data_dictionary_by_use(DataUse.REST_OBJ)
    assert len(results_rest) == 1, f"Expected one result for REST_OBJ, but got {len(results_rest)}"
    assert results_rest[0].resource.description == resource_rest.description

    # 3. Test con un DataUse que no tiene registros (limpieza de prueba)
    dummy_use = DataUse.ASYNC_MESSAGE  # Esto requiere manejo avanzado del Enum o asumir el error esperado
    results_none = await reference_query_port_out.get_data_dictionary_by_use(dummy_use)
    assert len(results_none) == 0, "Expected no results for a non-existent data use."

@pytest.mark.asyncio
async def test_get_data_dictionary_by_applications_list(reference_command_port_out: ReferenceCommandPortOut, reference_query_port_out: ReferenceQueryPortOut, application_query_port_out: ApplicationQueryPortOut):
    """
    Tests fetching data dictionaries based on multiple application names and a type definition.
    Requires seeding applications and associated links (ResourceApplicationLink).
    """
    # Setup: Sembrar datos para la prueba
    usable_apply: Application = await application_query_port_out.get_application(1)
    usable_apply_2: Application = await application_query_port_out.get_application(2)
    
    generic_resource_1 = create_random_data_resources()
    new_data_dict_1: StoredDataResource = await reference_command_port_out.save_data_dictionary_reference(generic_resource_1)
    dic_ref_1: ResourceApplicationLink = await reference_command_port_out.save_data_reference_application(new_data_dict_1, usable_apply)

    generic_resource_2 = create_random_data_resources()
    new_data_dict_2: StoredDataResource = await reference_command_port_out.save_data_dictionary_reference(generic_resource_2)
    dic_ref_2: ResourceApplicationLink = await reference_command_port_out.save_data_reference_application(new_data_dict_2, usable_apply_2)

    generic_resource_3 = create_random_data_resources()
    new_data_dict_3: StoredDataResource = await reference_command_port_out.save_data_dictionary_reference(generic_resource_3)
    dic_ref_3: ResourceApplicationLink = await reference_command_port_out.save_data_reference_application(new_data_dict_3, usable_apply)

    list_names_applications: List[str] = [usable_apply.name, usable_apply_2.name]
    dict_ref_id_list: list = [dic_ref_1.dictionary_id, dic_ref_3.dictionary_id]

    results_name_type:List[StoredDataResource] = await reference_query_port_out.get_data_dictionary_by_applications(applications_name=list_names_applications, type_applications_id=usable_apply_2.type_application_id)

    assert len(results_name_type) == 1, f"Expected two unique dictionaries (A and B), but found {len(results_name_type)}. Check joins/WHERE clauses."
    assert results_name_type[0].dictionary_id == dic_ref_2.dictionary_id

    results_name:List[StoredDataResource] = await reference_query_port_out.get_data_dictionary_by_applications(applications_name=[usable_apply.name])

    assert len(results_name) == 2, f"Expected two unique dictionaries (A and B), but found {len(results_name)}. Check joins/WHERE clauses."
    assert results_name[0].dictionary_id in dict_ref_id_list
    assert results_name[1].dictionary_id in dict_ref_id_list

    results_type:List[StoredDataResource] = await reference_query_port_out.get_data_dictionary_by_applications(type_applications_id=usable_apply_2.type_application_id)

    assert len(results_type) == 1, f"Expected two unique dictionaries (A and B), but found {len(results_type)}. Check joins/WHERE clauses."
    assert results_type[0].dictionary_id == dic_ref_2.dictionary_id

    results_all:List[StoredDataResource] = await reference_query_port_out.get_data_dictionary_by_applications()

    assert len(results_all) == 3, f"Expected two unique dictionaries (A and B), but found {len(results_all)}. Check joins/WHERE clauses."


# =======================================================================
# ResourceQueryPortOut: búsqueda de recursos por nombre (lectura)
# =======================================================================

async def _save_named(reference_command_port_out: ReferenceCommandPortOut, name: str, competence: str = "messaging") -> DataResources:
    """Persiste un recurso aleatorio con nombre y competencia controlados."""
    resource = create_random_data_resources()
    resource.name = name
    resource.competence = competence
    await reference_command_port_out.save_data_dictionary_reference(resource)
    return resource


@pytest.mark.asyncio
async def test_save_data_dictionary_reference_persists_name(reference_command_port_out: ReferenceCommandPortOut, resource_query_port_out: ResourceQueryPortOut):
    """El nombre del recurso (DataResources.name) debe persistirse para poder buscarlo después."""
    token = uuid.uuid4().hex
    saved = await _save_named(reference_command_port_out, f"orders_{token}")

    results = await resource_query_port_out.search_data_resources_by_name(f"orders_{token}")

    assert [r.name for r in results] == [saved.name]


@pytest.mark.asyncio
async def test_search_by_name_partial_and_case_insensitive(reference_command_port_out: ReferenceCommandPortOut, resource_query_port_out: ResourceQueryPortOut):
    """Coincidencia parcial en cualquier posición del nombre, sin distinguir mayúsculas/minúsculas, ordenado por nombre."""
    token = uuid.uuid4().hex[:12]
    await _save_named(reference_command_port_out, f"beta_{token.upper()}_users")
    await _save_named(reference_command_port_out, f"alpha_{token}_orders")
    await _save_named(reference_command_port_out, f"unrelated_{uuid.uuid4().hex[:12]}")

    # fragmento central, con mayúsculas/minúsculas mezcladas
    fragment = token[2:10].swapcase()
    results = await resource_query_port_out.search_data_resources_by_name(fragment)

    assert [r.name for r in results] == [f"alpha_{token}_orders", f"beta_{token.upper()}_users"]


@pytest.mark.asyncio
async def test_search_by_name_filters_by_data_use(reference_command_port_out: ReferenceCommandPortOut, resource_query_port_out: ResourceQueryPortOut):
    """El filtro por tipo (DataUse) restringe los resultados; sin filtro se devuelven todos los tipos."""
    token = uuid.uuid4().hex
    await _save_named(reference_command_port_out, f"msg_{token}", competence="messaging")   # ASYNC_MESSAGE
    await _save_named(reference_command_port_out, f"tbl_{token}", competence="database")    # DB_TABLE
    await _save_named(reference_command_port_out, f"api_{token}", competence="rest")        # REST_OBJ

    only_tables = await resource_query_port_out.search_data_resources_by_name(token, DataUse.DB_TABLE)
    only_messages = await resource_query_port_out.search_data_resources_by_name(token, DataUse.ASYNC_MESSAGE)
    all_types = await resource_query_port_out.search_data_resources_by_name(token)

    assert [r.name for r in only_tables] == [f"tbl_{token}"]
    assert [r.name for r in only_messages] == [f"msg_{token}"]
    assert len(all_types) == 3


@pytest.mark.asyncio
async def test_search_by_name_without_matches_returns_empty_list(reference_command_port_out: ReferenceCommandPortOut, resource_query_port_out: ResourceQueryPortOut):
    """Sin coincidencias se devuelve una lista vacía (no None ni excepción)."""
    await _save_named(reference_command_port_out, f"present_{uuid.uuid4().hex}")

    results = await resource_query_port_out.search_data_resources_by_name(f"absent_{uuid.uuid4().hex}")

    assert results == []


@pytest.mark.asyncio
async def test_search_by_name_treats_like_wildcards_literally(reference_command_port_out: ReferenceCommandPortOut, resource_query_port_out: ResourceQueryPortOut):
    """'_' y '%' en el término de búsqueda son literales, no comodines de LIKE."""
    token = uuid.uuid4().hex
    await _save_named(reference_command_port_out, f"a_{token}")
    await _save_named(reference_command_port_out, f"aX{token}")    # '_' como comodín coincidiría también con este
    await _save_named(reference_command_port_out, f"100%_{token}")

    underscore = await resource_query_port_out.search_data_resources_by_name(f"a_{token}")
    percent = await resource_query_port_out.search_data_resources_by_name(f"%_{token}")

    assert [r.name for r in underscore] == [f"a_{token}"]
    assert [r.name for r in percent] == [f"100%_{token}"]


@pytest.mark.asyncio
async def test_search_by_name_maps_record_to_domain_model(reference_command_port_out: ReferenceCommandPortOut, resource_query_port_out: ResourceQueryPortOut):
    """El puerto devuelve modelos de dominio completos (no entidades ORM), reconstruidos a partir del registro."""
    token = uuid.uuid4().hex
    saved = await _save_named(reference_command_port_out, f"db_{token}", competence="database")

    results = await resource_query_port_out.search_data_resources_by_name(token)

    assert len(results) == 1
    found = results[0]
    assert isinstance(found, DataResources)
    assert found.name == saved.name
    assert found.description == saved.description
    assert found.fields == saved.fields
    assert (found.competence, found.type_data) == ("database", "dbRow")



# =======================================================================
# Domain-typed contracts (review Phase 2, D3/D12): adapters return domain models
# =======================================================================

@pytest.mark.asyncio
async def test_reference_adapter_returns_domain_models(reference_command_port_out: ReferenceCommandPortOut,
                                                       reference_query_port_out: ReferenceQueryPortOut,
                                                       application_query_port_out: ApplicationQueryPortOut):
    """No ORM entity crosses the port: every result is a domain model."""
    resource = create_random_data_resources()
    resource.competence = "database"

    stored = await reference_command_port_out.save_data_dictionary_reference(resource)
    application = await application_query_port_out.get_application(1)
    assert application is not None
    link = await reference_command_port_out.save_data_reference_application(stored, application)
    by_use = await reference_query_port_out.get_data_dictionary_by_use(DataUse.DB_TABLE)

    assert isinstance(stored, StoredDataResource) and isinstance(application, Application)
    assert isinstance(link, ResourceApplicationLink)
    assert (link.dictionary_id, link.application_id) == (stored.dictionary_id, application.application_id)
    assert stored.data_use == DataUse.DB_TABLE
    assert (stored.resource.name, stored.resource.description, stored.resource.fields) == (resource.name, resource.description, resource.fields)
    assert [s.dictionary_id for s in by_use] == [stored.dictionary_id]


@pytest.mark.asyncio
async def test_update_data_dictionary_reference_replaces_the_resource(reference_command_port_out: ReferenceCommandPortOut,
                                                                     reference_query_port_out: ReferenceQueryPortOut):
    """It used to crash (``data.items()`` on a Pydantic model); it now replaces name, description, type and fields."""
    stored = await reference_command_port_out.save_data_dictionary_reference(create_random_data_resources())
    replacement = create_random_data_resources()
    replacement.competence = "database"

    updated = await reference_command_port_out.update_data_dictionary_reference(stored.dictionary_id, replacement)
    by_use = await reference_query_port_out.get_data_dictionary_by_use(DataUse.DB_TABLE)

    assert updated.dictionary_id == stored.dictionary_id
    assert updated.data_use == DataUse.DB_TABLE
    assert (updated.resource.name, updated.resource.description, updated.resource.fields) == (
        replacement.name, replacement.description, replacement.fields)
    assert [s.resource.name for s in by_use] == [replacement.name]


@pytest.mark.asyncio
async def test_update_unknown_reference_raises_not_found(reference_command_port_out: ReferenceCommandPortOut):
    with pytest.raises(NotFoundException):
        await reference_command_port_out.update_data_dictionary_reference(999_999, create_random_data_resources())


@pytest.mark.asyncio
async def test_delete_data_dictionary_reference_reports_whether_it_existed(reference_command_port_out: ReferenceCommandPortOut,
                                                                          resource_query_port_out: ResourceQueryPortOut):
    stored = await reference_command_port_out.save_data_dictionary_reference(create_random_data_resources())

    assert await reference_command_port_out.delete_data_dictionary_reference(stored.dictionary_id) is True
    assert await resource_query_port_out.search_data_resources_by_name(stored.resource.name) == []
    assert await reference_command_port_out.delete_data_dictionary_reference(stored.dictionary_id) is False


@pytest.mark.asyncio
async def test_application_adapter_returns_domain_models_or_none(application_query_port_out: ApplicationQueryPortOut):
    by_name = await application_query_port_out.get_application_by_name("MS_listener_question")
    all_applications = await application_query_port_out.get_all_applications()

    assert isinstance(by_name, Application)
    assert by_name.location == {"region": "eu-west-1", "group": "question_procesor", "familly": "ms_procesor"}
    assert [a.name for a in all_applications] == ["MS_listener_question", "MS_producer_question"]
    assert await application_query_port_out.get_application_by_name("unknown_application") is None
    assert await application_query_port_out.get_application(999_999) is None
