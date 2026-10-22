import logging
from typing import Dict
import os
from dotenv import dotenv_values
from fastapi import APIRouter, HTTPException, status
from resources_orchester.application.adapters.resources_adapter import ResourceServicePortIn
from resources_orchester.domain.api_dtos.messages_request_dto import MessagesRequestDTO
from src.resources_orchester.domain.model.data_resources import DataResources

# Load environment variables from the local .env.api file
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, ".env.api")
api_config: Dict[str, str] = dotenv_values(env_path)

# Configuration for API routes
API_PREFIX = api_config.get("API_PREFIX", "/v1/resources")
API_NEW_RESOURCE = api_config.get("API_NEW_RESOURCE", "/{id_user}/messages/create")

router = APIRouter(prefix=API_PREFIX)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

@router.post(API_NEW_RESOURCE, response_model=DataResources, status_code=status.HTTP_201_CREATED)
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
        logger.error("************************control logging info")
        created_resource = await service.create_message_resource(payload_message)
        return created_resource

    except Exception as e:
        
        logger.error(f"Error al crear mensaje para usuario {id_user}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail={
                "message": "Fallo en el proceso de creación de recurso.",
                "detalle": str(e) 
            }
        )
