import logging
from typing import Annotated, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.exception.domain_exceptions import ValidationException
from resources_orchester.domain.exception.infrastructure_exceptions import MessagingException, PersistenceException
from resources_orchester.infrastructure.bootstrap.dependencies import CreateMessageResourceService, SearchResourcesService
from resources_orchester.infrastructure.bootstrap.settings import ApiSettings
from resources_orchester.domain.model.data_resources import DataResources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

async def create_message_resource_endpoint(
    payload_message: MessagesRequestDTO,
    id_user: str,
    service: CreateMessageResourceService
):
    """
    Endpoint que gestiona la creación de recursos de Mensajería.
    Flujo: DTO (HTTP) -> Service Layer (Caso de Uso) -> Domain Model.
    """
    try:
        logger.error("************************control logging info")
        created_resource = await service.create_message_resource(payload_message)
        return created_resource

    except PersistenceException as e:
        # La transacción no se ha podido confirmar (p. ej. fallo en el COMMIT): nada se ha guardado
        # ni se publicará. Es un error del servidor, no del cliente.
        logger.error(f"Error de persistencia al crear mensaje para usuario {id_user}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "No se ha podido guardar el recurso; no se ha creado ni notificado.",
                "detalle": "Error de persistencia"
            }
        )
    except MessagingException as e:
        # El broker no está disponible: no es un error del cliente. La transacción se revierte.
        logger.error(f"Kafka no disponible al crear mensaje para usuario {id_user}: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message": "Servicio de mensajería no disponible; el recurso no se ha creado.",
                "detalle": e.message
            }
        )
    except Exception as e:
        
        logger.error(f"Error al crear mensaje para usuario {id_user}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail={
                "message": "Fallo en el proceso de creación de recurso.",
                "detalle": str(e) 
            }
        )


async def search_resources_endpoint(
    service: SearchResourcesService,
    name: Annotated[str, Query(min_length=1, max_length=150, description="Fragmento del nombre del recurso (coincidencia parcial, sin distinguir mayúsculas/minúsculas).")],
    data_use: Annotated[Optional[DataUse], Query(alias="type", description="Tipo de recurso por el que filtrar. Si se omite se buscan todos los tipos.")] = None,
):
    """
    Endpoint de consulta de recursos persistidos por nombre y, opcionalmente, por tipo.
    Flujo: Query params (HTTP) -> Service Layer (Caso de Uso) -> Puerto de lectura -> Domain Models.
    Devuelve una lista (vacía si no hay coincidencias).
    """
    try:
        return await service.search_resources_by_name(name, data_use)
    except ValidationException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "message": "Parámetros de búsqueda no válidos.",
                "detalle": e.message
            }
        )


def build_router(api_settings: ApiSettings) -> APIRouter:
    """Registers the endpoints under the paths configured in ApiSettings (loaded once at startup)."""
    router = APIRouter(prefix=api_settings.prefix)
    router.add_api_route(api_settings.new_resource, create_message_resource_endpoint, methods=["POST"],
                         response_model=DataResources, status_code=status.HTTP_201_CREATED)
    router.add_api_route(api_settings.search_resource, search_resources_endpoint, methods=["GET"],
                         response_model=List[DataResources], status_code=status.HTTP_200_OK)
    return router

