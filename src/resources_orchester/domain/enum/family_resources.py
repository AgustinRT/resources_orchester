from enum import Enum

class FamilyResource(str, Enum):
    """
    Define las familias de recursos manejadas por el sistema.
    """
    COMUNICATION = "communication"
    CONTEXT = "context"
    DATAS = "datas"
