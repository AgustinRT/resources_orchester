"""
Architecture guard (review D5): the application layer must not depend on the web framework.
Dependency wiring lives in infrastructure/bootstrap (composition root).
Phase 2 replaces this with import-linter contracts for every layer boundary.
"""
import ast
from pathlib import Path

import resources_orchester

PACKAGE_DIR = Path(resources_orchester.__file__).resolve().parent
FORBIDDEN_IN_APPLICATION = ("fastapi", "starlette")


def _imported_modules(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            yield node.module


def test_application_layer_does_not_import_the_web_framework():
    application_files = sorted((PACKAGE_DIR / "application").rglob("*.py"))
    assert application_files, "application layer not found"

    offenders = [
        f"{path.relative_to(PACKAGE_DIR)} imports {module}"
        for path in application_files
        for module in _imported_modules(path)
        if module.split(".")[0] in FORBIDDEN_IN_APPLICATION
    ]

    assert offenders == []


def test_composition_root_is_the_only_place_that_binds_ports_to_adapters():
    """The service provider lives in infrastructure/bootstrap; application/adapters is gone."""
    assert not (PACKAGE_DIR / "application" / "adapters").exists()
    assert (PACKAGE_DIR / "infrastructure" / "bootstrap" / "dependencies.py").is_file()


def test_ports_are_owned_by_the_application_layer():
    """Review Phase 2 (D3/D10): every port lives in application/ports/{inbound,outbound}; infrastructure defines none."""
    assert not any(path.name == "ports" for path in (PACKAGE_DIR / "infrastructure").rglob("*") if path.is_dir())
    assert sorted(p.name for p in (PACKAGE_DIR / "application" / "ports").iterdir() if p.is_dir() and p.name != "__pycache__") == ["inbound", "outbound"]



def _session_dependencies(function):
    """Yields (parameter, Depends) for every parameter of ``function`` that depends on get_db_session."""
    import inspect
    import typing

    from fastapi.params import Depends as DependsParam

    from resources_orchester.infrastructure.bootstrap.dependencies import get_db_session

    for parameter in inspect.signature(function).parameters.values():
        candidates = list(getattr(parameter.annotation, "__metadata__", ()))
        if isinstance(parameter.default, DependsParam):
            candidates.append(parameter.default)
        for candidate in candidates:
            if isinstance(candidate, DependsParam) and candidate.dependency is get_db_session:
                yield parameter, candidate


def test_request_session_is_function_scoped_and_shared_through_one_alias():
    """
    Review D2 quick fix + transactionality guard:
    * the request session uses scope="function", so it is closed before the response is sent;
    * every provider must depend on it through the SAME ``DbSession`` alias. FastAPI caches
      ``Depends(get_db_session)`` and ``Depends(get_db_session, scope="function")`` separately:
      mixing them opens a SECOND session, whose writes would escape the unit of work's transaction.
    """
    import inspect

    from resources_orchester.infrastructure.bootstrap import dependencies

    assert dependencies.DbSession.__metadata__[0].scope == "function"
    providers = [f for _, f in inspect.getmembers(dependencies, inspect.isfunction) if f.__module__ == dependencies.__name__]
    session_users = {f.__name__: [p for p, _ in _session_dependencies(f)] for f in providers}
    session_users = {name: params for name, params in session_users.items() if params}

    assert {"get_unit_of_work", "get_reference_command_port_out", "get_event_outbox_port_out"} <= session_users.keys()
    offenders = [name for name, params in session_users.items() if any(p.annotation is not dependencies.DbSession for p in params)]
    assert offenders == []
