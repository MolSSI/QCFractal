from __future__ import annotations

import inspect
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, get_type_hints, TYPE_CHECKING

if TYPE_CHECKING:
    from flask import Flask

import pydantic

# Matches a single flask/werkzeug path parameter such as <molecule_id> or <int:molecule_id>.
# The character classes must not span '<'/'>' so that a rule with multiple parameters
# (e.g. /users/<username_or_id>/tokens/<int:token_id>) is matched one parameter at a time.
_PATH_PARAM_RE = re.compile(r"<(?:(?P<converter>[^<>:]+):)?(?P<name>[^<>]+)>")


class SchemaCollector:
    """
    Collects all types used in routes, then generates JSON schemas for them in a single pass

    Generating all schemas in one batch lets pydantic disambiguate models from different
    modules that share a class name (for example, qcportal and qcelemental both define
    an OptimizationSpecification). Colliding names get module-qualified component names.
    Generating them one type at a time would instead silently associate all references
    with whichever definition happened to be generated first.
    """

    def __init__(self) -> None:
        self._types: Dict[Any, int] = {}
        self._schemas: Optional[Dict[Any, Dict[str, Any]]] = None
        self.components: Dict[str, Dict[str, Any]] = {}

    def register(self, tp: Any) -> None:
        """
        Mark a type as needing a schema. Must be called for every type before generate()
        """

        if tp is None or tp is type(None):
            return
        if tp not in self._types:
            self._types[tp] = len(self._types)

    def generate(self) -> None:
        """
        Generate schemas for all registered types, populating self.components
        """

        # Request data would ideally use mode="validation", but qcelemental's Array type
        # raises KeyError when generating a validation-mode json schema (its
        # __get_pydantic_json_schema__ only finds its dtype metadata in serialization mode).
        # So serialization mode is used for everything. In practice the two modes produce
        # the same schemas for the models used in routes.
        inputs = [(key, "serialization", pydantic.TypeAdapter(tp)) for tp, key in self._types.items()]
        schemas_by_key, defs = pydantic.TypeAdapter.json_schemas(
            inputs,
            ref_template="#/components/schemas/{model}",
        )

        self._schemas = {tp: schemas_by_key[(key, "serialization")] for tp, key in self._types.items()}
        self.components = defs.get("$defs", {})

    def schema_for_type(self, tp: Any) -> Dict[str, Any]:
        if tp is None or tp is type(None):
            return {"type": "null"}

        assert self._schemas is not None, "SchemaCollector.generate() must be called first"
        return self._schemas[tp]


def _normalize_path(rule: str) -> Tuple[str, List[Dict[str, Any]]]:
    params: List[Dict[str, Any]] = []

    def replace(match: re.Match[str]) -> str:
        converter = match.group("converter") or "string"
        name = match.group("name")
        schema = {"type": "string"}

        if converter in {"int", "integer"}:
            schema = {"type": "integer"}
        elif converter in {"float", "number"}:
            schema = {"type": "number"}

        params.append(
            {
                "name": name,
                "in": "path",
                "required": True,
                "schema": schema,
            }
        )
        return "{" + name + "}"

    normalized = _PATH_PARAM_RE.sub(replace, rule)
    return normalized, params


def _url_params_fields(model: Any) -> Dict[str, pydantic.fields.FieldInfo]:
    if not isinstance(model, type) or not issubclass(model, pydantic.BaseModel):
        return {}
    return model.model_fields


def _query_parameters(model: Any, schema_for_type: Callable[[Any], Dict[str, Any]]) -> List[Dict[str, Any]]:
    params: List[Dict[str, Any]] = []
    for name, field in _url_params_fields(model).items():
        params.append(
            {
                "name": name,
                "in": "query",
                "required": field.is_required(),
                "schema": schema_for_type(field.annotation),
            }
        )
    return params


def _request_body(
    body_model: Any,
    allowed_file_extensions: Optional[Iterable[str]],
    schema_for_type: Callable[[Any], Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if body_model is None and not allowed_file_extensions:
        return None

    if allowed_file_extensions:
        properties: Dict[str, Any] = {
            "files": {
                "type": "array",
                "items": {"type": "string", "format": "binary"},
            }
        }
        if body_model is not None:
            properties["body_data"] = schema_for_type(body_model)

        required_properties = ["files"]
        if body_model is not None:
            required_properties.append("body_data")

        return {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": properties,
                        "required": required_properties,
                    }
                }
            },
        }

    return {
        "required": True,
        "content": {
            "application/json": {
                "schema": schema_for_type(body_model),
            }
        },
    }


def generate_openapi_spec(
    app: Flask,
    title: str = "QCFractal API",
    version: str = "0.0.0",
) -> Dict[str, Any]:
    collector = SchemaCollector()

    # First pass: gather route information and register every type that needs a schema
    route_entries = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue

        view_func = app.view_functions[rule.endpoint]

        # Use inspect.unwrap to get the original function that has the metadata and annotations
        unwrapped_view_func = inspect.unwrap(view_func)
        meta = getattr(unwrapped_view_func, "__openapi_meta__", {})

        # If this raises, a route has an annotation that cannot be resolved - a developer error
        hints = get_type_hints(unwrapped_view_func)

        body_model = hints.get("body_data")
        url_params_model = hints.get("url_params")
        response_model = hints.get("return")

        collector.register(body_model)
        collector.register(response_model)
        for field in _url_params_fields(url_params_model).values():
            collector.register(field.annotation)

        route_entries.append((rule, unwrapped_view_func, meta, body_model, url_params_model, response_model))

    collector.generate()

    # Second pass: assemble the operations, with all schemas now available
    paths: Dict[str, Any] = {}
    security_required = False

    for rule, unwrapped_view_func, meta, body_model, url_params_model, response_model in route_entries:
        requested_resource = meta.get("requested_resource")
        require_security = meta.get("require_security", False)
        allowed_file_extensions = meta.get("allowed_file_extensions")

        path, path_params = _normalize_path(rule.rule)
        query_params = _query_parameters(url_params_model, collector.schema_for_type)
        request_body = _request_body(body_model, allowed_file_extensions, collector.schema_for_type)

        for method in rule.methods:
            if method in {"HEAD", "OPTIONS"}:
                continue

            operation = {
                "summary": unwrapped_view_func.__name__,
                "parameters": [*path_params, *query_params],
                "responses": {
                    "200": {
                        "description": "Success",
                    }
                },
            }

            if requested_resource:
                operation["tags"] = [requested_resource]

            if request_body is not None:
                operation["requestBody"] = request_body

            if response_model is not None:
                operation["responses"]["200"]["content"] = {
                    "application/json": {
                        "schema": collector.schema_for_type(response_model),
                    }
                }

            if require_security:
                operation["security"] = [{"bearerAuth": []}]
                security_required = True

            paths.setdefault(path, {})[method.lower()] = operation

    # Schemas generated by pydantic v2 use JSON Schema 2020-12 features
    # (type: "null", prefixItems, const), which require OpenAPI 3.1
    spec: Dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {"title": title, "version": version},
        "paths": paths,
    }

    if collector.components or security_required:
        components: Dict[str, Any] = {}
        if collector.components:
            components["schemas"] = collector.components

        if security_required:
            components["securitySchemes"] = {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                }
            }

        spec["components"] = components

    return spec
