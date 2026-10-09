import json

from kafka import KafkaConsumer
import pytest
import pytest_asyncio
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.domain.model.data_resources import DataResources
from resources_orchester.infrastructure.messaging.dtos.message_resource_payload import MessageResourcePayload
from resources_orchester.infrastructure.messaging.messaging_adapter import get_messaging_port_out
from resources_orchester.infrastructure.messaging.ports.kafka_port_out import KafkaPortOut

from test.infraestructure.kafka_container_tester import consumer_tester, rise_kafka_container
from test.infraestructure.utils_tester import create_random_data_resources

@pytest_asyncio.fixture(scope="session")
async def kafka_container():
    """Start a kafka container."""
    async with rise_kafka_container() as kafka:
        yield kafka.get_bootstrap_server()

@pytest_asyncio.fixture(scope="function")
async def producer_port_out(kafka_container) -> KafkaPortOut:
    return get_messaging_port_out({
        "server": kafka_container,
        "group": "mcps"
    })

@pytest_asyncio.fixture(scope="function")
async def consumer_container(kafka_container) -> KafkaConsumer:
    return consumer_tester(kafka_container, "update")

@pytest.mark.asyncio
async def test_simply_kafka_producer(producer_port_out: KafkaPortOut, consumer_container: KafkaConsumer):
    """
    Tests the creation and management of a generic data resource object 
    using the DataResources model, demonstrating domain layer usage within tests.
    """
    destiny_message = {
                        "topic": f"update",
                        "family_destination": FamilyResource.DATAS,
                        "type_destination": DataUse.ASYNC_MESSAGE
                    }
    message_processed: int = 0
    data_message: DataResources = create_random_data_resources()
    producer_port_out.send_message_to_update(destiny_message, data_message)

    data_message_2: DataResources = create_random_data_resources()
    producer_port_out.send_message_to_update(destiny_message, data_message_2)

    list_names: list[str] = [data_message.name, data_message_2.name]

    for message in consumer_container:
        message_consumed: MessageResourcePayload = MessageResourcePayload.from_message_resources(message.value)
        assert message_consumed.name_resource in list_names
        message_processed += 1

    assert 2 == message_processed
