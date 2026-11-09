"""
Application tests with the REAL lifespan and DI graph against the PostgreSQL and Kafka
Testcontainers (review Phase 1: D1/D4/D6, Phase 3: D2/D9/D11). Nothing is mocked: settings are
injected into create_app, and the lifespan builds the app-scoped engine/pool, producer, outbox
relay and consumer.

These tests commit for real (the application owns its transactions), so the resource and
outbox tables are truncated afterwards (``truncate_resource_tables``).
"""
import asyncio
import json
import socket
import time
import uuid
from typing import Awaitable, Callable, List

import pytest
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from sqlalchemy import event, func, select

from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.enum.family_resources import FamilyResource
from resources_orchester.infrastructure.bootstrap.settings import DatabaseSettings, KafkaSettings
from resources_orchester.infrastructure.messaging.dtos.message_resource_payload import MessageResourcePayload
from resources_orchester.infrastructure.messaging.kafka_workers import kafka_producer as kafka_producer_module
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_producer import WorkerKafkaProducer
from resources_orchester.infrastructure.persistence.entities.data_dict_registry import DataRefDict
from resources_orchester.infrastructure.persistence.entities.outbox_event import OutboxEvent
from resources_orchester.infrastructure.persistence.repositories.reference_repository import ReferenceRepository

from test.infraestructure.app_tester import build_test_app, client_for
from test.infraestructure.kafka_container_tester import consumer_tester, producer_tester, tcp_forwarder
from test.infraestructure.utils_tester import create_random_data_resources

pytestmark = pytest.mark.usefixtures("truncate_resource_tables")


def _message_payload(name: str) -> dict:
    return {"name": name, "description": ["es_ES d", "en_US d"],
            "fields_add": [{"name": "id", "type": "uuid", "description": ["pk"]}]}


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _published_names(kafka_container: str, topic: str) -> List[str]:
    """
    Every resource name published on ``topic`` (reads until the topic is idle for 5 s).
    The producer wraps the payload in an envelope: {"destination": ..., "message_data": MessageResourcePayload}.
    """
    consumer = consumer_tester(kafka_container, topic)
    try:
        return [json.loads(json.dumps(message.value))["message_data"]["source"] for message in consumer]
    finally:
        consumer.close()


async def _eventually(check: Callable[[], Awaitable[bool]], timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await check():
            return True
        await asyncio.sleep(0.2)
    return False


async def _count(app: FastAPI, stmt) -> int:
    async with app.state.database.session() as session:
        return (await session.execute(stmt)).scalar_one()


@pytest.fixture
def producer_instances(monkeypatch) -> list:
    """Records every AIOKafkaProducer the adapter creates (i.e. every producer connection set)."""
    created = []

    class CountingProducer(AIOKafkaProducer):
        def __init__(self, *args, **kwargs):
            created.append(self)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(kafka_producer_module, "AIOKafkaProducer", CountingProducer)
    return created


# =======================================================================
# Phase 1: app-scoped resources
# =======================================================================

@pytest.mark.asyncio
async def test_resources_are_app_scoped_and_reused_under_load(database_settings: DatabaseSettings, kafka_settings: KafkaSettings,
                                                            kafka_container: str, producer_instances: list):
    app: FastAPI = build_test_app(database=database_settings, kafka=kafka_settings)
    token = uuid.uuid4().hex[:8]
    names = [f"load_{token}_{i:02d}" for i in range(20)]
    db_connections_opened = []

    async with app.router.lifespan_context(app):
        producer_at_startup = app.state.kafka_producer
        event.listen(app.state.database.engine.sync_engine, "connect", lambda *_: db_connections_opened.append(1))

        async with client_for(app) as client:
            create_url = app.url_path_for("create_message_resource_endpoint", id_user="load")
            search_url = app.url_path_for("search_resources_endpoint")
            responses = await asyncio.gather(
                *[client.post(create_url, json=_message_payload(name)) for name in names],
                *[client.get(search_url, params={"name": token}) for _ in names],
            )
            found = await client.get(search_url, params={"name": token})

        assert [r.status_code for r in responses] == [201] * len(names) + [200] * len(names)
        assert sorted(r["name"] for r in found.json()) == names
        pending = select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_(None))
        assert await _eventually(lambda: _count_is_zero(app, pending)), "the relay publishes every committed event"
        # D1: one producer for the whole application, never one per request.
        assert app.state.kafka_producer is producer_at_startup
        assert len(producer_instances) == 1
        # D6: DB connections come from the pool and are bounded by it (was one per request).
        assert 1 <= len(db_connections_opened) <= database_settings.pool_size + database_settings.max_overflow

    # D4: every created resource was really published (through the outbox relay).
    assert sorted(_published_names(kafka_container, kafka_settings.update_topic)) == names


async def _count_is_zero(app: FastAPI, stmt) -> bool:
    return await _count(app, stmt) == 0


@pytest.mark.asyncio
async def test_shutdown_releases_the_app_scoped_resources(database_settings: DatabaseSettings, kafka_settings: KafkaSettings):
    app: FastAPI = build_test_app(database=database_settings, kafka=kafka_settings)

    async with app.router.lifespan_context(app):
        producer: WorkerKafkaProducer = app.state.kafka_producer
        background_tasks = [app.state.kafka_consumer_task, app.state.outbox_relay_task]
        async with client_for(app) as client:
            assert (await client.get(app.url_path_for("search_resources_endpoint"), params={"name": "x"})).status_code == 200
        assert producer._producer is not None
        assert app.state.database.engine.pool.checkedin() >= 1

    assert all(task.done() for task in background_tasks)
    assert producer._producer is None
    assert app.state.database.engine.pool.checkedin() == 0


# =======================================================================
# Phase 3 (D2): transactions and the outbox
# =======================================================================

@pytest.mark.asyncio
async def test_commit_failure_returns_5xx_and_publishes_nothing(database_settings: DatabaseSettings, kafka_settings: KafkaSettings,
                                                                kafka_container: str, fail_on_commit_trigger: str):
    """
    DONE-WHEN (Phase 3): the database rejects the transaction at COMMIT (deferred trigger: INSERT and
    flush succeed). The client gets a 5xx (it used to get 201), nothing is stored, no event is
    recorded and nothing reaches Kafka (it used to publish an event for a resource that did not exist).
    """
    app: FastAPI = build_test_app(database=database_settings, kafka=kafka_settings)
    failing_name, good_name = f"{fail_on_commit_trigger}_{uuid.uuid4().hex[:8]}", f"ok_{uuid.uuid4().hex[:8]}"

    async with app.router.lifespan_context(app):
        async with client_for(app) as client:
            create_url = app.url_path_for("create_message_resource_endpoint", id_user="u")
            failed = await client.post(create_url, json=_message_payload(failing_name))
            succeeded = await client.post(create_url, json=_message_payload(good_name))  # the relay is alive

        assert failed.status_code == 500
        assert succeeded.status_code == 201
        assert await _count(app, select(func.count()).select_from(DataRefDict).where(DataRefDict.name_data == failing_name)) == 0
        assert await _count(app, select(func.count()).select_from(OutboxEvent)
                            .where(OutboxEvent.payload["resource"]["name"].astext == failing_name)) == 0
        published = select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_not(None))
        assert await _eventually(lambda: _count_equals(app, published, 1))

    assert _published_names(kafka_container, kafka_settings.update_topic) == [good_name]


async def _count_equals(app: FastAPI, stmt, expected: int) -> bool:
    return await _count(app, stmt) == expected


@pytest.mark.asyncio
async def test_kafka_down_resources_are_created_and_events_delivered_when_it_returns(database_settings: DatabaseSettings,
                                                                                    kafka_settings: KafkaSettings, kafka_container: str):
    """
    Broker unreachable from startup: the app starts, reads work, creating a resource succeeds (201) and
    its event waits in the outbox (attempts recorded). When Kafka becomes reachable the relay delivers
    it without a restart. (Before the outbox, the POST answered 503 and the resource was not created.)
    """
    port = _free_port()
    kafka_down = kafka_settings.model_copy(update={"bootstrap_servers": f"127.0.0.1:{port}",
                                                   "outbox_poll_interval_seconds": 0.2, "consumer_retry_backoff_seconds": 0.2})
    app: FastAPI = build_test_app(database=database_settings, kafka=kafka_down)
    name = f"while_down_{uuid.uuid4().hex[:8]}"

    started = time.perf_counter()
    async with app.router.lifespan_context(app):
        assert time.perf_counter() - started < 10, "startup must not block on Kafka"
        async with client_for(app) as client:
            created = await client.post(app.url_path_for("create_message_resource_endpoint", id_user="u"), json=_message_payload(name))
            found = await client.get(app.url_path_for("search_resources_endpoint"), params={"name": name})

        assert created.status_code == 201
        assert [r["name"] for r in found.json()] == [name]
        retried = select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_(None), OutboxEvent.attempts > 0)
        assert await _eventually(lambda: _count_equals(app, retried, 1)), "the relay tried and kept the event pending"

        async with tcp_forwarder(port, kafka_container):  # Kafka becomes reachable
            published = select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_not(None))
            assert await _eventually(lambda: _count_equals(app, published, 1), timeout=30)

    assert _published_names(kafka_container, kafka_settings.update_topic) == [name]


# =======================================================================
# Phase 3 (D11): Kafka updates applied through a use case
# =======================================================================

@pytest.mark.asyncio
async def test_consumer_updates_persisted_state_through_the_use_case(database_settings: DatabaseSettings, kafka_settings: KafkaSettings,
                                                                     kafka_container: str):
    """
    DONE-WHEN (Phase 3): an update message on the consumer topic changes the persisted resource.
    Path: Kafka -> KafkaUpdateConsumerService -> ApplyResourceUpdatePortIn (one session / UoW per
    message, from the composition root) -> PostgreSQL. A poison message before it is skipped.
    """
    app: FastAPI = build_test_app(database=database_settings, kafka=kafka_settings.model_copy(update={"consumer_retry_backoff_seconds": 0.2}))
    resource = create_random_data_resources()
    resource.name, resource.competence = f"external_{uuid.uuid4().hex[:8]}", "messaging"
    updated = resource.model_copy(update={"description": {"es_ES": "actualizado desde Kafka"},
                                          "fields": create_random_data_resources().fields})
    update_message = MessageResourcePayload.from_data_resources(
        {"family_destination": FamilyResource.DATAS, "resource_value": DataUse.ASYNC_MESSAGE}, updated).model_dump(mode="json")

    async with app.router.lifespan_context(app):
        async with app.state.database.session() as session:
            await ReferenceRepository(session).save_data_dictionary_reference(resource)

        async with client_for(app) as client:
            search_url = app.url_path_for("search_resources_endpoint")

            async def applied() -> bool:
                response = await client.get(search_url, params={"name": resource.name})
                return response.json()[0]["description"] == updated.description

            producer = producer_tester(kafka_container)
            try:
                # No consumer group: the consumer starts at the latest offset, so keep sending
                # (a poison message, then the update; re-applying it is idempotent) until applied.
                deadline = time.monotonic() + 30
                while not await applied() and time.monotonic() < deadline:
                    producer.send(kafka_settings.consumer_topics[0], {"garbage": True})
                    producer.send(kafka_settings.consumer_topics[0], update_message)
                    producer.flush()
                    await asyncio.sleep(0.5)
            finally:
                producer.close()

            final = (await client.get(search_url, params={"name": resource.name})).json()

        assert [(r["name"], r["description"]) for r in final] == [(resource.name, updated.description)]
        assert final[0]["fields"] == [f.model_dump() for f in updated.fields]
        assert not app.state.kafka_consumer_task.done(), "the poison message did not stop the consumer"
