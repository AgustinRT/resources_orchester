import pytest
import pytest_asyncio
from resources_orchester.infrastructure.persistence.base_adapter import get_application_port_in, get_reference_port_in, get_reference_port_out, get_reference_repository_test
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
from resources_orchester.infrastructure.persistence.entities.link_dict_application import DataDictionaryAppRef
from resources_orchester.infrastructure.persistence.entities.registry_applications import RegistryApplications
from resources_orchester.infrastructure.persistence.ports.applications_port_in import ApplicationsPortIn
from resources_orchester.infrastructure.persistence.ports.reference_port_in import ReferencePortIn
from resources_orchester.infrastructure.persistence.ports.reference_port_out import ReferencePortOut

from resources_orchester.domain.enum.data_use import DataUse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import AsyncGenerator, List

from test.infraestructure.postgres_container_tester import rise_postgres_container, set_session_to_container
from test.infraestructure.utils_tester import create_random_data_resources

@pytest_asyncio.fixture(scope="session")
async def postgres_container():
    """Start a Postgres container with SQL initialization."""
    async with rise_postgres_container() as container:
        yield container

@pytest_asyncio.fixture(scope="function")
async def session_connect(postgres_container) -> AsyncGenerator[AsyncSession]:
    """Provide a psycopg3 connection that rolls back after each test."""
    url = postgres_container.get_connection_url()
    async with set_session_to_container(url) as session:
        yield session

@pytest_asyncio.fixture(scope="function")
async def reference_port_out(session_connect) -> ReferencePortOut:
    return  get_reference_port_out(session_connect)


@pytest_asyncio.fixture(scope="function")
async def reference_port_in(session_connect) -> ReferencePortIn:
    return  get_reference_port_in(session_connect)

@pytest_asyncio.fixture(scope="function")
async def application_port_in(session_connect) -> ApplicationsPortIn:
    return  get_application_port_in(session_connect)

@pytest.mark.asyncio
async def test_create_generic_data_resource(reference_port_out: ReferencePortOut):
    """
    Tests the creation and management of a generic data resource object 
    using the DataResources model, demonstrating domain layer usage within tests.
    """
    generic_resource = create_random_data_resources()

    response: DataRefDict = await reference_port_out.save_data_dictionary_reference(generic_resource)

    assert response is not None, "Should successfully save resource data dictionary reference without fields."
    assert response.dictionary_id is not None


@pytest.mark.asyncio
async def test_save_data_dictionary_reference_no_fields(reference_port_out: ReferencePortOut):
    """
    Tests saving a resource when the 'fields' list is empty or missing, 
    ensuring graceful handling if fields are optional in this scenario.
    """
    # Create a resource without fields (or with an explicitly empty field list)
    generic_resource = create_random_data_resources()
    generic_resource.fields = []
    # Expect the save operation to succeed even without field definitions.
    response = await reference_port_out.save_data_dictionary_reference(generic_resource)
    assert response is not None, "Should successfully save resource data dictionary reference without fields."
    assert response.dictionary_id is not None

@pytest.mark.asyncio
async def test_create_data_reference_application(reference_port_out: ReferencePortOut, application_port_in: ApplicationsPortIn):
    """
    Tests the creation and management of a generic data resource object 
    using the DataResources model, demonstrating domain layer usage within tests.
    """    
    usable_apply: RegistryApplications = await application_port_in.get_application(1)
    generic_resource = create_random_data_resources()
    new_data_dict: DataRefDict = await reference_port_out.save_data_dictionary_reference(generic_resource)

    response: DataDictionaryAppRef = await reference_port_out.save_data_reference_application(new_data_dict, usable_apply)

    assert response is not None, "Should successfully save resource data dictionary reference without fields."
    assert new_data_dict.dictionary_id == response.dictionary_id
    assert usable_apply.application_id == response.application_id

@pytest.mark.asyncio
async def test_get_data_dictionary_by_use(reference_port_out: ReferencePortOut, reference_port_in: ReferencePortIn):
    """
    Tests fetching data dictionaries based on a specific DataUse enum.
    Requires seeding multiple records with different uses.
    """
    # Sembrar registros en la base de datos simulando diferentes usos
    resource_db = create_random_data_resources()
    resource_rest = create_random_data_resources()
    
    resource_db.competence ="database"
    await reference_port_out.save_data_dictionary_reference(resource_db)

    resource_rest.competence ="rest"
    await reference_port_out.save_data_dictionary_reference(resource_rest)

    # 1. Test con DataUse.DB_TABLE
    results_db = await reference_port_in.get_data_dictionary_by_use(DataUse.DB_TABLE)
    assert len(results_db) == 1, f"Expected one result for DB_TABLE, but got {len(results_db)}"
    assert results_db[0].description_data == resource_db.description

    # 2. Test con DataUse.REST_OBJ
    results_rest = await reference_port_in.get_data_dictionary_by_use(DataUse.REST_OBJ)
    assert len(results_rest) == 1, f"Expected one result for REST_OBJ, but got {len(results_rest)}"
    assert results_rest[0].description_data == resource_rest.description

    # 3. Test con un DataUse que no tiene registros (limpieza de prueba)
    dummy_use = DataUse.ASYNC_MESSAGE  # Esto requiere manejo avanzado del Enum o asumir el error esperado
    results_none = await reference_port_in.get_data_dictionary_by_use(dummy_use)
    assert len(results_none) == 0, "Expected no results for a non-existent data use."

@pytest.mark.asyncio
async def test_get_data_dictionary_by_applications_list(reference_port_out: ReferencePortOut, reference_port_in: ReferencePortIn, application_port_in: ApplicationsPortIn):
    """
    Tests fetching data dictionaries based on multiple application names and a type definition.
    Requires seeding applications and associated links (DataDictionaryAppRef).
    """
    # Setup: Sembrar datos para la prueba
    usable_apply: RegistryApplications = await application_port_in.get_application(1)
    usable_apply_2: RegistryApplications = await application_port_in.get_application(2)
    
    generic_resource_1 = create_random_data_resources()
    new_data_dict_1: DataRefDict = await reference_port_out.save_data_dictionary_reference(generic_resource_1)
    dic_ref_1: DataDictionaryAppRef = await reference_port_out.save_data_reference_application(new_data_dict_1, usable_apply)

    generic_resource_2 = create_random_data_resources()
    new_data_dict_2: DataRefDict = await reference_port_out.save_data_dictionary_reference(generic_resource_2)
    dic_ref_2: DataDictionaryAppRef = await reference_port_out.save_data_reference_application(new_data_dict_2, usable_apply_2)

    generic_resource_3 = create_random_data_resources()
    new_data_dict_3: DataRefDict = await reference_port_out.save_data_dictionary_reference(generic_resource_3)
    dic_ref_3: DataDictionaryAppRef = await reference_port_out.save_data_reference_application(new_data_dict_3, usable_apply)

    list_names_applications: List[str] = [usable_apply.name_application, usable_apply_2.name_application]
    dict_ref_id_list: list = [dic_ref_1.dictionary_id, dic_ref_3.dictionary_id]

    results_name_type:List[DataRefDict] = await reference_port_in.get_data_dictionary_by_applications(applications_name=list_names_applications, type_applications_id=usable_apply_2.type_application_id)

    assert len(results_name_type) == 1, f"Expected two unique dictionaries (A and B), but found {len(results_name_type)}. Check joins/WHERE clauses."
    assert results_name_type[0].dictionary_id == dic_ref_2.dictionary_id

    results_name:List[DataRefDict] = await reference_port_in.get_data_dictionary_by_applications(applications_name=[usable_apply.name_application])

    assert len(results_name) == 2, f"Expected two unique dictionaries (A and B), but found {len(results_name)}. Check joins/WHERE clauses."
    assert results_name[0].dictionary_id in dict_ref_id_list
    assert results_name[1].dictionary_id in dict_ref_id_list

    results_type:List[DataRefDict] = await reference_port_in.get_data_dictionary_by_applications(type_applications_id=usable_apply_2.type_application_id)

    assert len(results_type) == 1, f"Expected two unique dictionaries (A and B), but found {len(results_type)}. Check joins/WHERE clauses."
    assert results_type[0].dictionary_id == dic_ref_2.dictionary_id

    results_all:List[DataRefDict] = await reference_port_in.get_data_dictionary_by_applications()

    assert len(results_all) == 3, f"Expected two unique dictionaries (A and B), but found {len(results_all)}. Check joins/WHERE clauses."
