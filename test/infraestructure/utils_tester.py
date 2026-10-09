


from random import choice
import random
import uuid

from resources_orchester.domain.model.data_field_resource import DataFieldResource
from resources_orchester.domain.model.data_resources import DataResources


def create_random_data_resources() -> DataResources:
    """
    Factory function to generate and return a semi-randomly populated 
    DataResources object for testing purposes.
    """
    # Define potential values for semi-realistic randomization
    possible_types = ["json", "db_table", "MD_info"]
    possible_process = ["api_MS", "listener_MS", "MCP_MS"]
    descriptions = {
        "es_ES": f"Descripción aleatoria generada para el dato {str(uuid.uuid4())[:8]}.",
        "en_US": f"Random description generated for the data {str(uuid.uuid4())[:8]}."
    }
    fields_usable = [create_random_data_field_resources() for _ in range(3)]

    resource = DataResources(
        name = f"data_{choice(['foo', 'bar', 'spanish_inquisition'])}_{str(uuid.uuid4())[:8]}",
        type_data = choice(possible_types),
        description = descriptions,
        competence = choice(["messaging", "restAPI", "persistence", "servers"]),
        process_apply = [choice(possible_process) for _ in range(random.randint(0, 2))],
        fields = fields_usable
    )
    return resource

def create_random_data_field_resources() -> DataFieldResource:
    """
    Factory function to generate and return a semi-randomly populated 
    DataResources object for testing purposes.
    """
    possible_types = ["string", "integer", "date", "boolean"]
    descriptions = {
        "es_ES": f"Descripción aleatoria generada para el campo {str(uuid.uuid4())[:8]}.",
        "en_US": f"Random description generated for the field {str(uuid.uuid4())[:8]}."
    }
    
    data_field = DataFieldResource(
        name=f"field_{choice(['reference', 'name', 'description'])}_{str(uuid.uuid4())[:8]}",
        type = choice(possible_types),
        description = descriptions,
        validations = [choice(["mandatory", "unique", "no inject"]) for _ in range(random.randint(0, 2))]
    )

    return data_field