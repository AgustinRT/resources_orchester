"""
Composition root: the ONLY place where ports are bound to their adapters.

Application-scoped resources (engine/pool, Kafka producer, outbox relay) are created once by
the lifespan (bootstrap/lifespan.py) and stored in ``app.state``; the providers below only READ
them.

Request-scoped objects are built per request, and each use case only receives the ports it
needs (review D9):

* ``get_db_session`` opens ONE session per request, with ``scope="function"`` so it is closed
  before the response is sent. It never commits by itself: only a unit of work commits,
  explicitly, inside the use case (review D2). Every port of a request shares the same session
  (FastAPI caches the dependency per request), so they all take part in the same transaction.
  Always depend on it through the ``DbSession`` alias, so every provider uses the same cached
  dependency.
* ``build_apply_resource_update_factory`` does the same for Kafka messages: one session and
  unit of work per message, from the shared session factory.
"""
import contextlib
from typing import Annotated, AsyncGenerator, AsyncIterator

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from resources_orchester.application.ports.inbound.apply_resource_update_port_in import ApplyResourceUpdatePortIn
from resources_orchester.application.ports.inbound.create_message_resource_port_in import CreateMessageResourcePortIn
from resources_orchester.application.ports.inbound.resource_access_port_in import ResourceAccessPortIn
from resources_orchester.application.ports.inbound.search_resources_port_in import SearchResourcesPortIn
from resources_orchester.application.ports.outbound.application_query_port_out import ApplicationQueryPortOut
from resources_orchester.application.ports.outbound.event_outbox_port_out import EventOutboxPortOut
from resources_orchester.application.ports.outbound.reference_command_port_out import ReferenceCommandPortOut
from resources_orchester.application.ports.outbound.reference_query_port_out import ReferenceQueryPortOut
from resources_orchester.application.ports.outbound.resource_access_port_out import ResourceAccessPortOut
from resources_orchester.application.ports.outbound.resource_query_port_out import ResourceQueryPortOut
from resources_orchester.application.ports.outbound.unit_of_work_port import UnitOfWorkPort
from resources_orchester.application.use_cases.apply_resource_update import ApplyResourceUpdate
from resources_orchester.application.use_cases.create_message_resource import CreateMessageResource
from resources_orchester.application.use_cases.manage_resource_access import ManageResourceAccess
from resources_orchester.application.use_cases.search_resources import SearchResources
from resources_orchester.infrastructure.bootstrap.settings import Settings
from resources_orchester.infrastructure.messaging.kafka_workers.kafka_consumer_update import ApplyResourceUpdateFactory
from resources_orchester.infrastructure.persistence.embeddings.embedding_client import EmbeddingClient
from resources_orchester.infrastructure.persistence.postgre_database import DatabaseManager
from resources_orchester.infrastructure.persistence.repositories.application_repository import ApplicationRepository
from resources_orchester.infrastructure.persistence.repositories.outbox_repository import OutboxRepository
from resources_orchester.infrastructure.persistence.repositories.reference_repository import ReferenceRepository
from resources_orchester.infrastructure.persistence.repositories.resource_access_repository import ResourceAccessRepository
from resources_orchester.infrastructure.persistence.unit_of_work import SqlAlchemyUnitOfWork


# --- Application-scoped resources (created by the lifespan) ---------------------------

def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_database(request: Request) -> DatabaseManager:
    return request.app.state.database


def get_embedding_client(request: Request) -> EmbeddingClient:
    """AI model client chosen by EMBEDDING_* configuration (created once by the lifespan)."""
    return request.app.state.embedding_client


def notify_outbox_relay(request: Request) -> None:
    """Wakes the outbox relay up so the events just committed are published immediately."""
    request.app.state.outbox_relay.notify()


# --- Request-scoped: session, unit of work and outbound ports -------------------------

async def get_db_session(database: Annotated[DatabaseManager, Depends(get_database)]) -> AsyncGenerator[AsyncSession]:
    async with database.open_session() as session:
        yield session


DbSession = Annotated[AsyncSession, Depends(get_db_session, scope="function")]


def get_unit_of_work(session: DbSession, request: Request) -> UnitOfWorkPort:
    return SqlAlchemyUnitOfWork(session, after_commit=lambda: notify_outbox_relay(request))


def get_reference_query_port_out(session: DbSession) -> ReferenceQueryPortOut:
    return ReferenceRepository(session)


def get_reference_command_port_out(session: DbSession) -> ReferenceCommandPortOut:
    return ReferenceRepository(session)


def get_resource_query_port_out(session: DbSession) -> ResourceQueryPortOut:
    """Adaptador de lectura (ReferenceRepository) expuesto tras el puerto de salida de consultas."""
    return ReferenceRepository(session)


def get_application_query_port_out(session: DbSession) -> ApplicationQueryPortOut:
    return ApplicationRepository(session)


def get_event_outbox_port_out(session: DbSession) -> EventOutboxPortOut:
    return OutboxRepository(session)


def get_resource_access_port_out(session: DbSession,
                                 embedding_client: Annotated[EmbeddingClient, Depends(get_embedding_client)]) -> ResourceAccessPortOut:
    return ResourceAccessRepository(session, embedding_client)


UnitOfWork = Annotated[UnitOfWorkPort, Depends(get_unit_of_work)]
ReferenceQueryPort = Annotated[ReferenceQueryPortOut, Depends(get_reference_query_port_out)]
ReferenceCommandPort = Annotated[ReferenceCommandPortOut, Depends(get_reference_command_port_out)]
ResourceQueryPort = Annotated[ResourceQueryPortOut, Depends(get_resource_query_port_out)]
ApplicationQueryPort = Annotated[ApplicationQueryPortOut, Depends(get_application_query_port_out)]
EventOutboxPort = Annotated[EventOutboxPortOut, Depends(get_event_outbox_port_out)]
ResourceAccessPort = Annotated[ResourceAccessPortOut, Depends(get_resource_access_port_out)]


# --- Use cases: one provider each, with only the ports it needs -----------------------

def get_create_message_resource(unit_of_work: UnitOfWork, application_query: ApplicationQueryPort,
                                reference_command: ReferenceCommandPort, event_outbox: EventOutboxPort) -> CreateMessageResourcePortIn:
    return CreateMessageResource(unit_of_work=unit_of_work, application_query_port_out=application_query,
                                 reference_command_port_out=reference_command, event_outbox_port_out=event_outbox)


def get_search_resources(resource_query: ResourceQueryPort) -> SearchResourcesPortIn:
    return SearchResources(resource_query_port_out=resource_query)


def get_resource_access_service(unit_of_work: UnitOfWork, resource_access: ResourceAccessPort,
                                settings: Annotated[Settings, Depends(get_settings)]) -> ResourceAccessPortIn:
    """The search values come from configuration (ResourceAccessSettings), never from the use case."""
    search = settings.resource_access
    return ManageResourceAccess(unit_of_work=unit_of_work, resource_access_port_out=resource_access,
                                list_limit=search.list_limit, similarity_limit=search.similarity_limit,
                                min_similarity=search.min_similarity)


CreateMessageResourceService = Annotated[CreateMessageResourcePortIn, Depends(get_create_message_resource)]
SearchResourcesService = Annotated[SearchResourcesPortIn, Depends(get_search_resources)]
ResourceAccessService = Annotated[ResourceAccessPortIn, Depends(get_resource_access_service)]


# --- Non-HTTP driving adapters (Kafka consumer) ---------------------------------------

def build_apply_resource_update_factory(database: DatabaseManager) -> ApplyResourceUpdateFactory:
    """For the Kafka consumer: each call opens one session / unit of work for one message."""

    @contextlib.asynccontextmanager
    async def apply_resource_update() -> AsyncIterator[ApplyResourceUpdatePortIn]:
        async with database.open_session() as session:
            references = ReferenceRepository(session)
            yield ApplyResourceUpdate(unit_of_work=SqlAlchemyUnitOfWork(session), reference_query_port_out=references,
                                      reference_command_port_out=references)

    return apply_resource_update
