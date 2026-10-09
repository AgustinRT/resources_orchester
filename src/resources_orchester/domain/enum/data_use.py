from enum import Enum

class DataUse(str, Enum):
    """
    Define los tipos de recursos o contextos operativos utilizados en el sistema 
    para diferenciar su naturaleza (Mensajería, Base de Datos, API).
    Debe reflejar los valores definidos en el dominio.
    """
    ASYNC_MESSAGE = "ASYNC_MESSAGE"
    DB_TABLE = "DB_TABLE"
    REST_OBJ = "REST_OBJ"