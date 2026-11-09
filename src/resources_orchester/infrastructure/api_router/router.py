import logging
from typing import Annotated, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from resources_orchester.domain.api_dtos.resource_access_dto import (
    ResourceAccessDTO,
    ResourceAccessListResponseDTO,
    ResourceAccessMatchDTO,
    ResourceAccessRegisteredResponseDTO,
    ResourceAccessRequestDTO,
    ResourceAccessSimilarResponseDTO,
)
from resources_orchester.domain.enum.data_use import DataUse
from resources_orchester.domain.exception.domain_exceptions import NotFoundException, ValidationException
from resources_orchester.domain.exception.infrastructure_exceptions import (
    EmbeddingException,
    MessagingException,
    PersistenceException,
)
from resources_orchester.infrastructure.bootstrap.dependencies import (
    CreateMessageResourceService,
    ResourceAccessService,
    SearchResourcesService,
)
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


def _access_failure(status_code: int, message: str, detail: str) -> HTTPException:
    """Error response of the resource-access endpoints: {"success": false, "message", "detalle"}."""
    return HTTPException(status_code=status_code, detail={"success": False, "message": message, "detalle": detail})


def _embedding_unavailable(error: EmbeddingException, action: str) -> HTTPException:
    logger.error("Modelo de embeddings no disponible al %s: %s", action, error.message)
    return _access_failure(status.HTTP_503_SERVICE_UNAVAILABLE,
                           f"Modelo de IA no disponible; no se ha podido {action}.", error.message)


async def create_resource_access_endpoint(
    payload_access: ResourceAccessRequestDTO,
    service: ResourceAccessService,
) -> ResourceAccessRegisteredResponseDTO:
    """
    Registra el acceso de un usuario a un recurso, con el embedding de su prompt.
    Flujo: DTO (HTTP, 422 si la estructura no es válida) -> ResourceAccessPortIn -> ResourceAccess (dominio)
    -> ResourceAccessPortOut (embedding + PostgreSQL). Si algo falla no se guarda nada.
    """
    try:
        access = await service.register_access(payload_access)
    except ValidationException as e:
        raise _access_failure(status.HTTP_400_BAD_REQUEST, "Acceso no válido; no se ha registrado.", e.message)
    except NotFoundException as e:
        raise _access_failure(status.HTTP_404_NOT_FOUND, "Recurso no registrado; no se ha registrado el acceso.", e.message)
    except EmbeddingException as e:
        raise _embedding_unavailable(e, "registrar el acceso")
    except PersistenceException as e:
        logger.error("Error de persistencia al registrar acceso de %s al recurso %s: %s",
                     payload_access.username, payload_access.resource_id, e)
        raise _access_failure(status.HTTP_500_INTERNAL_SERVER_ERROR, "No se ha podido registrar el acceso.",
                              "Error de persistencia")

    logger.info("Acceso registrado: usuario=%s recurso=%s modelo=%s", access.username, access.resource_id, access.model_name)
    return ResourceAccessRegisteredResponseDTO(success=True, message="Acceso al recurso registrado.",
                                               data=ResourceAccessDTO.from_domain(access))


async def search_resource_access_endpoint(
    service: ResourceAccessService,
    username: Annotated[Optional[str], Query(min_length=1, max_length=150, description="Usuario (coincidencia exacta).")] = None,
    resource: Annotated[Optional[int], Query(gt=0, description="Recurso (data_dict_registry.dictionary_id).")] = None,
) -> ResourceAccessListResponseDTO:
    """
    Accesos de un usuario y/o de un recurso (coincidencia exacta; al menos uno de los dos), más recientes primero.
    Flujo: Query params -> ResourceAccessPortIn -> ResourceAccessPortOut.
    """
    try:
        accesses = await service.search_accesses(username=username, resource_id=resource)
    except ValidationException as e:
        raise _access_failure(status.HTTP_400_BAD_REQUEST, "Parámetros de consulta no válidos.", e.message)
    except PersistenceException as e:
        logger.error("Error de persistencia al consultar accesos (usuario=%s recurso=%s): %s", username, resource, e)
        raise _access_failure(status.HTTP_500_INTERNAL_SERVER_ERROR, "No se ha podido consultar los accesos.",
                              "Error de persistencia")

    return ResourceAccessListResponseDTO(success=True, message=f"{len(accesses)} acceso(s) encontrado(s).",
                                         results=[ResourceAccessDTO.from_domain(a) for a in accesses])


async def similar_resource_access_endpoint(
    service: ResourceAccessService,
    predicate: Annotated[str, Query(min_length=1, description="Texto a comparar con los prompts almacenados.")],
    resource: Annotated[Optional[int], Query(gt=0, description="Limitar la búsqueda a este recurso.")] = None,
) -> ResourceAccessSimilarResponseDTO:
    """
    Accesos cuyo prompt es similar a ``predicate`` (límite y similitud mínima configurados), más similares primero.
    Flujo: Query params -> ResourceAccessPortIn -> ResourceAccessPortOut (embedding + búsqueda vectorial).
    """
    try:
        matches = await service.search_similar_accesses(predicate, resource_id=resource)
    except ValidationException as e:
        raise _access_failure(status.HTTP_400_BAD_REQUEST, "Parámetros de consulta no válidos.", e.message)
    except EmbeddingException as e:
        raise _embedding_unavailable(e, "buscar accesos similares")
    except PersistenceException as e:
        logger.error("Error de persistencia en la búsqueda por similitud (recurso=%s): %s", resource, e)
        raise _access_failure(status.HTTP_500_INTERNAL_SERVER_ERROR, "No se ha podido consultar los accesos.",
                              "Error de persistencia")

    return ResourceAccessSimilarResponseDTO(success=True, message=f"{len(matches)} acceso(s) similar(es) encontrado(s).",
                                            results=[ResourceAccessMatchDTO.from_match(m) for m in matches])


def build_router(api_settings: ApiSettings) -> APIRouter:
    """Registers the endpoints under the paths configured in ApiSettings (loaded once at startup)."""
    router = APIRouter(prefix=api_settings.prefix)
    router.add_api_route(api_settings.new_resource, create_message_resource_endpoint, methods=["POST"],
                         response_model=DataResources, status_code=status.HTTP_201_CREATED)
    router.add_api_route(api_settings.search_resource, search_resources_endpoint, methods=["GET"],
                         response_model=List[DataResources], status_code=status.HTTP_200_OK)
    router.add_api_route(api_settings.new_resource_access, create_resource_access_endpoint, methods=["POST"],
                         response_model=ResourceAccessRegisteredResponseDTO, status_code=status.HTTP_201_CREATED)
    router.add_api_route(api_settings.search_resource_access, search_resource_access_endpoint, methods=["GET"],
                         response_model=ResourceAccessListResponseDTO, status_code=status.HTTP_200_OK)
    router.add_api_route(api_settings.similar_resource_access, similar_resource_access_endpoint, methods=["GET"],
                         response_model=ResourceAccessSimilarResponseDTO, status_code=status.HTTP_200_OK)
    return router

