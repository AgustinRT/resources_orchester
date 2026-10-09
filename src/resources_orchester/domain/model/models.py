# Este archivo está vacío o puede ser usado para un índice de modelos.

from .data_resources import DataResources
from .context_resource import ContextResource
from .communications_resource import CommunicationsResource

class ModelContainer():
    """Contenedor principal que agrupa todos los modelos del dominio."""
    data_resources: DataResources
    context_resource: ContextResource
    comunications_resource: CommunicationsResource