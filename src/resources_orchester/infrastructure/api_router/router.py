from fastapi import APIRouter, HTTPException, status
from resources_orchester.application.adapters.resources_adapter import ResourceServicePortIn
from src.resources_orchester.infrastructure.api_router.dtos.messages_request_dto import MessagesRequestDTO
from src.resources_orchester.domain.model.data_resources import DataResources


router = APIRouter(prefix="/v1/resources")

@router.post("/{id_user}/messages/create", response_model=DataResources, status_code=status.HTTP_201_CREATED)
async def create_message_resource_endpoint(
    payload_message: MessagesRequestDTO,
    id_user: str,
    service: ResourceServicePortIn
):
    """
    Endpoint que gestiona la creación de recursos de Mensajería.
    Flujo: DTO (HTTP) -> Service Layer (Caso de Uso) -> Domain Model.
    """
    try:
        created_resource = await service.create_message_resource(payload_message)
        return created_resource

    except Exception as e:
        
        print(f"Error al crear mensaje para usuario {id_user}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail={
                "message": "Fallo en el proceso de creación de recurso.",
                "detalle": str(e) # Devolvemos un detalle técnico para debugging
            }
        )