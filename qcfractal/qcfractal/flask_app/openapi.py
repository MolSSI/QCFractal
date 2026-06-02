from __future__ import annotations

import re
import inspect
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple, get_type_hints, TYPE_CHECKING

if TYPE_CHECKING:
    from flask import Flask

import pydantic



_PATH_PARAM_RE = re.compile(r"<(?:(?P<converter>[^:]+):)?(?P<name>[^>]+)>")


class SchemaRegistry:
    def __init__(self) -> None:
        self.components: Dict[str, Dict[str, Any]] = {}
        self._root_counter = 0

    def schema_for_type(self, tp: Any) -> Dict[str, Any]:
        if tp is None or tp is type(None):
            return {"type": "null"}

        root_name = f"OpenAPIRoot{self._root_counter}"
        self._root_counter += 1

        root_adapter = pydantic.TypeAdapter(tp)
        schema = root_adapter.json_schema(
            mode="serialization",
            ref_template="#/components/schemas/{model}",
        )
        definitions = schema.pop("$defs", {})

        for name, definition in definitions.items():
            if name not in self.components:
                self.components[name] = definition

        return schema


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


def _query_parameters(model: Any, registry: SchemaRegistry) -> List[Dict[str, Any]]:
    if model is None:
        return []

    if not isinstance(model, type) or not issubclass(model, pydantic.BaseModel):
        return []

    params: List[Dict[str, Any]] = []
    for name, field in model.model_fields.items():
        schema = registry.schema_for_type(field.annotation)
        params.append(
            {
                "name": name,
                "in": "query",
                "required": field.is_required(),
                "schema": schema,
            }
        )
    return params


def _request_body(
    body_model: Any,
    allowed_file_extensions: Optional[Iterable[str]],
    registry: SchemaRegistry,
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
            properties["body_data"] = registry.schema_for_type(body_model)

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

    schema = registry.schema_for_type(body_model)
    return {
        "required": True,
        "content": {
            "application/json": {
                "schema": schema,
            }
        },
    }


def generate_openapi_spec(
    app: Flask,
    title: str = "QCFractal API",
    version: str = "0.0.0",
) -> Dict[str, Any]:
    registry = SchemaRegistry()
    paths: Dict[str, Any] = {}
    security_required = False

    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue

        view_func = app.view_functions[rule.endpoint]

        # Use inspect.unwrap to get the original function that has the metadata and annotations
        unwrapped_view_func = inspect.unwrap(view_func)
        meta = getattr(unwrapped_view_func, "__openapi_meta__", {})

        requested_resource = meta.get("requested_resource")
        require_security = meta.get("require_security", False)
        allowed_file_extensions = meta.get("allowed_file_extensions")

        path, path_params = _normalize_path(rule.rule)
        try:
            hints = get_type_hints(unwrapped_view_func)
        except Exception:
            hints = getattr(unwrapped_view_func, "__annotations__", {})

        body_model = hints.get("body_data")
        url_params_model = hints.get("url_params")
        response_model = hints.get("return")

        query_params = _query_parameters(url_params_model, registry)
        request_body = _request_body(body_model, allowed_file_extensions, registry)

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
                        "schema": registry.schema_for_type(response_model),
                    }
                }

            if require_security:
                operation["security"] = [{"bearerAuth": []}]
                security_required = True

            paths.setdefault(path, {})[method.lower()] = operation

    spec: Dict[str, Any] = {
        "openapi": "3.0.3",
        "info": {"title": title, "version": version},
        "paths": paths,
    }

    if registry.components or security_required:
        components: Dict[str, Any] = {}
        if registry.components:
            components["schemas"] = deepcopy(registry.components)

        if security_required:
            components["securitySchemes"] = {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                }
            }

        spec["components"] = components

    return spec
