from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, get_type_hints

import pydantic.v1 as pydantic


@dataclass(frozen=True)
class OpenAPIRouteInfo:
    path: str
    methods: Tuple[str, ...]
    view_func: Any
    requested_resource: str
    requested_action: str
    require_security: bool
    allowed_file_extensions: Optional[Tuple[str, ...]]


_ROUTES: List[OpenAPIRouteInfo] = []


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

        root_model = pydantic.create_model(root_name, __root__=(tp, ...))
        schema = pydantic.schema.schema([root_model], ref_prefix="#/components/schemas/")
        definitions = schema.get("definitions", {})
        root_schema = definitions.pop(root_name, None) or {}

        for name, definition in definitions.items():
            if name not in self.components:
                self.components[name] = definition

        return root_schema


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


def _join_paths(prefix: Optional[str], rule: str) -> str:
    if not prefix:
        return rule
    if rule == "/":
        return prefix
    return prefix.rstrip("/") + "/" + rule.lstrip("/")


def register_route(
    rule: str,
    methods: Iterable[str],
    view_func: Any,
    url_prefix: Optional[str] = None,
) -> None:
    meta = getattr(view_func, "__openapi_meta__", None)
    if not meta:
        return

    full_rule = _join_paths(url_prefix, rule)

    info = OpenAPIRouteInfo(
        path=full_rule,
        methods=tuple(sorted({m.upper() for m in methods if m})),
        view_func=view_func,
        requested_resource=meta["requested_resource"],
        requested_action=meta["requested_action"],
        require_security=meta["require_security"],
        allowed_file_extensions=meta["allowed_file_extensions"],
    )
    _ROUTES.append(info)


def _query_parameters(model: Any, registry: SchemaRegistry) -> List[Dict[str, Any]]:
    if model is None:
        return []

    if not isinstance(model, type) or not issubclass(model, pydantic.BaseModel):
        return []

    params: List[Dict[str, Any]] = []
    for name, field in model.__fields__.items():
        schema = registry.schema_for_type(field.outer_type_)
        params.append(
            {
                "name": name,
                "in": "query",
                "required": field.required,
                "schema": schema,
            }
        )
    return params


def _request_body(
    body_model: Any,
    allowed_file_extensions: Optional[Tuple[str, ...]],
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

        return {
            "required": True,
            "content": {
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": properties,
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
    title: str = "QCFractal API",
    version: str = "0.0.0",
) -> Dict[str, Any]:
    registry = SchemaRegistry()
    paths: Dict[str, Any] = {}

    for route in _ROUTES:
        path, path_params = _normalize_path(route.path)
        try:
            hints = get_type_hints(route.view_func)
        except Exception:
            hints = route.view_func.__annotations__

        body_model = hints.get("body_data")
        url_params_model = hints.get("url_params")
        response_model = hints.get("return")

        query_params = _query_parameters(url_params_model, registry)
        request_body = _request_body(body_model, route.allowed_file_extensions, registry)

        for method in route.methods:
            if method in {"HEAD", "OPTIONS"}:
                continue

            operation = {
                "summary": route.view_func.__name__,
                "tags": [route.requested_resource],
                "parameters": [*path_params, *query_params],
                "responses": {
                    "200": {
                        "description": "Success",
                    }
                },
            }

            if request_body is not None:
                operation["requestBody"] = request_body

            if response_model is not None:
                operation["responses"]["200"]["content"] = {
                    "application/json": {
                        "schema": registry.schema_for_type(response_model),
                    }
                }

            if route.require_security:
                operation["security"] = [{"bearerAuth": []}]

            paths.setdefault(path, {})[method.lower()] = operation

    spec: Dict[str, Any] = {
        "openapi": "3.0.3",
        "info": {"title": title, "version": version},
        "paths": paths,
    }

    if registry.components:
        spec["components"] = {"schemas": registry.components}
        spec["components"]["securitySchemes"] = {
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer",
            }
        }

    return spec
