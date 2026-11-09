"""Domain rules of ResourceAccess (the record stored in the vector table)."""
import pytest
from pydantic import ValidationError

from resources_orchester.domain.model.resource_access import ResourceAccess

VALID = {"username": "jdoe", "resource_id": 7, "predicate": "¿Qué campos tiene el topic de pedidos?"}
MODEL = "text-embedding-3-small"


def test_an_access_without_embedding_is_valid():
    access = ResourceAccess(**VALID)

    assert (access.username, access.resource_id, access.predicate) == ("jdoe", 7, VALID["predicate"])
    assert access.predicate_embed is None and access.model_name is None


def test_an_access_with_embedding_keeps_vector_and_model():
    access = ResourceAccess(**VALID, predicate_embed=[0.1, -0.2, 3], model_name=MODEL)

    assert access.predicate_embed == [0.1, -0.2, 3.0]
    assert access.model_name == MODEL


def test_text_fields_are_trimmed():
    access = ResourceAccess(username=" jdoe ", resource_id=7, predicate=" hola ", predicate_embed=[1.0], model_name=f" {MODEL} ")

    assert (access.username, access.predicate, access.model_name) == ("jdoe", "hola", MODEL)


def test_the_same_username_can_have_several_accesses():
    first = ResourceAccess(**VALID)
    second = ResourceAccess(**{**VALID, "resource_id": 8, "predicate": "otra pregunta"})

    assert first.username == second.username


def test_access_is_immutable():
    access = ResourceAccess(**VALID)
    with pytest.raises(ValidationError):
        access.username = "other"  # type: ignore[misc]


@pytest.mark.parametrize("changes, bad_field", [
    ({"username": None}, "username"),            # not null
    ({"username": "   "}, "username"),
    ({"username": "x" * 151}, "username"),
    ({"predicate": None}, "predicate"),          # not null
    ({"predicate": ""}, "predicate"),
    ({"resource_id": 0}, "resource_id"),
    ({"resource_id": None}, "resource_id"),
    ({"predicate_embed": [], "model_name": MODEL}, "predicate_embed"),               # empty vector
    ({"predicate_embed": [0.1, float("nan")], "model_name": MODEL}, "predicate_embed"),
    ({"predicate_embed": [float("inf")], "model_name": MODEL}, "predicate_embed"),
    ({"predicate_embed": [0.1], "model_name": "  "}, "model_name"),
])
def test_invalid_fields_are_rejected(changes, bad_field):
    with pytest.raises(ValidationError) as error:
        ResourceAccess(**{**VALID, **changes})

    assert bad_field in {str(e["loc"][0]) for e in error.value.errors()}


@pytest.mark.parametrize("changes", [
    {"predicate_embed": [0.1, 0.2]},   # vector without the model that built it
    {"model_name": MODEL},             # model without a vector
])
def test_embedding_and_model_name_go_together(changes):
    with pytest.raises(ValidationError, match="predicate_embed and model_name must be provided together"):
        ResourceAccess(**{**VALID, **changes})
