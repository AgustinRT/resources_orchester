from pydantic import BaseModel, Field
from typing import Dict, List, Optional

class DataFieldEntity(BaseModel):
    """
    Pydantic model representing the structure of a data field within the system.
    Manages metadata for a data attribute, including multilingual descriptions 
    and validation rules.
    """
    name: str = Field(..., description="The unique name identifier for this data field.")
    type: str = Field(..., description="The data type of the field (e.g., 'string', 'integer', 'date').")
    description: Dict[str, str] = Field(..., description="Multilingual description dictionary (e.g., {'es_ES': 'Descripción en español', 'en_US': 'Description in English'}). Must contain at least one language.")
    validations: Optional[List[str]] = Field(None, description="A list of string rules or validators applicable to this field's data.")

# Ejemplo de uso (Opcional: Para verificar la estructura)
# try:
#     example_data = {
#         "name": "user_email",
#         "type": "string",
#         "description": {"es_ES": "Correo electrónico del usuario.", "en_US": "User's email address."},
#         "validations": ["must_be_email", "max_length:254"]
#     }
#     field = DataFieldResource(**example_data)
#     print(f"DataFieldResource model created successfully for {field.name}")

# except Exception as e:
#     print(f"Error creating DataFieldResource model instance: {e}")