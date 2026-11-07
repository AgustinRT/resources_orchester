"""DTO -> domain conversions shared by the use cases."""
from typing import Dict, List, Optional

from resources_orchester.domain.api_dtos.data_field import DataField
from resources_orchester.domain.model.data_field_resource import DataFieldResource


def descriptions_to_dict(descriptions: List[str]) -> Dict[str, str]:
    """Multilingual description from the DTO list: first item -> es_ES, last item -> en_US."""
    if not descriptions:
        return {}
    return {"es_ES": descriptions[0], "en_US": descriptions[-1]}


def fields_to_domain(fields: Optional[List[DataField]]) -> List[DataFieldResource]:
    """DTO fields -> domain fields (the DTO description list becomes {'es_ES': first item})."""
    return [
        DataFieldResource(name=field.name, type=field.type, description={"es_ES": field.description[0]},
                          validations=field.validations)
        for field in fields or []
    ]
