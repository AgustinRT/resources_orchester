
from typing import Annotated

from fastapi import Depends
from resources_orchester.application.ports.resource_port_in import ResourcePortIn
from resources_orchester.application.resource_services.resource_manager_general import ResourceManagerGeneral
from resources_orchester.infrastructure.messaging.messaging_adapter import MessagingPortOut
from resources_orchester.infrastructure.persistence.base_adapter import AppliPortInt, RefPortIn, RefPortOut

def get_message_resource_service(port_in: RefPortIn, port_out: RefPortOut, appli_in: AppliPortInt, message_producer: MessagingPortOut) -> ResourcePortIn:
    """Dependency Provider para obtener la instancia del servicio de recursos, usando el adaptador base."""
    return ResourceManagerGeneral(reference_port_in=port_in, reference_port_out=port_out, application_port_in=appli_in, messaging_port_out=message_producer)

ResourceServicePortIn = Annotated[ResourcePortIn, Depends(get_message_resource_service)]
