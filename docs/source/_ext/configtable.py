"""
A ``config-table`` directive for documenting pydantic settings in prose.

The server and manager configuration pages are organised into thematic groups
("Logging", "Heartbeats", ...) that do not correspond to pydantic classes. This
directive lets a prose section render a table of just the options it is talking
about, with the type, default, and summary read out of the model at build time.

With no body, every field of the model is rendered, in declaration order::

    .. config-table:: qcfractal.config.CORSconfig

That is what you want whenever a model is documented by a single table. Give a body only
to split a model across several prose sections - ``FractalConfig`` is spread over eight -
in which case the body lists the fields for that one table, in the order they appear::

    .. config-table:: qcfractal.config.FractalConfig

       service_frequency
       max_active_services

A field's summary is the first paragraph of its description, so a description can
be as long as it needs to be: the table stays scannable and the full text shows up
in the generated reference section further down the page.

Two checks make the tables self-maintaining, which is the point of the whole
exercise - a hand-written option list drifts silently, and this cannot:

* Naming a field the model does not have is an error, so a renamed or removed option
  breaks the build rather than rotting in the prose.
* Every model referenced by a ``config-table`` must be *fully* covered: any field not in
  one of its tables is reported at the end of the build, so a newly added option cannot
  go undocumented. A body-less table satisfies this by construction.

Two escape hatches, both declared at the table rather than in ``conf.py`` so they sit
next to the thing they describe:

``:omit:``
    Fields deliberately left undocumented - typically ones a user cannot usefully set.
    Omissions are pooled across all of a model's tables, so it does not matter which one
    carries the option. Naming a field the model does not have is reported, so an
    omission cannot outlive the field it refers to.

``:partial:``
    Skip the completeness check for this model. For a page mid-conversion, so that
    unfinished work does not fail the build.

Defaults come from the model. Where the real default is computed at runtime (the
paths that default to ``[base_folder]/...``), override it with ``=``::

    .. config-table:: qcfractal.config.FractalConfig

       geoip2_dir = [base_folder]/geoip2
       geoip2_filename
"""

from __future__ import annotations

import importlib
from typing import Any

from docutils import nodes
from docutils.parsers.rst import Directive
from docutils.statemachine import StringList
from sphinx.util import logging

logger = logging.getLogger(__name__)

# What each document contributed, kept on the build environment rather than in module
# state so that it survives an incremental build. With module state, a rebuild that
# re-reads only some of the documents sees only their tables, and a model whose tables are
# split across pages gets falsely reported as incompletely documented.
#
#   env.configtable_data = {docname: {model path: {"seen", "omitted", "partial"}}}
_ENV_ATTR = "configtable_data"


def _doc_data(env, docname: str) -> dict:
    store = getattr(env, _ENV_ATTR, None)
    if store is None:
        store = {}
        setattr(env, _ENV_ATTR, store)
    return store.setdefault(docname, {})

_SCALAR_NAMES = {
    "str": "string",
    "int": "int",
    "bool": "bool",
    "float": "float",
    "NoneType": "null",
}


def _import_model(path: str) -> Any:
    module_name, _, class_name = path.rpartition(".")
    if not module_name:
        raise ValueError(f"'{path}' is not a dotted path to a class")
    return getattr(importlib.import_module(module_name), class_name)


def _type_name(annotation: Any) -> str:
    text = str(annotation)
    text = text.replace("typing.", "")
    # str(int) is "<class 'int'>"
    text = text.replace("<class '", "").replace("'>", "")
    for long, short in _SCALAR_NAMES.items():
        text = text.replace(long, short)
    text = text.replace(" | null", " or null")
    # Annotated[...] reprs are unreadable in a table; the bare type is enough here
    # and the reference section shows the full annotation.
    if text.startswith("Annotated["):
        text = text[len("Annotated[") :].split(",", 1)[0]
    if "." in text and "[" not in text:
        text = text.rsplit(".", 1)[-1]
    return text


def _default_text(field: Any) -> str:
    if field.is_required():
        return "*required*"
    # A default_factory means the real default is built at runtime. Pydantic leaves
    # .default as PydanticUndefined (not None) for these, so this has to be checked
    # before looking at .default at all.
    if getattr(field, "default_factory", None) is not None:
        return "*see below*"
    default = field.default
    if default is None:
        return "``null``"
    if isinstance(default, bool):
        return f"``{str(default).lower()}``"
    if isinstance(default, str):
        return '``""``' if default == "" else f'``"{default}"``'
    if isinstance(default, (list, dict)) and not default:
        return f"``{default!r}``"
    return f"``{default!r}``"


def _summary(field: Any, name: str, model_path: str) -> str:
    description = (field.description or "").strip()
    if not description:
        logger.warning(
            f"config-table: {model_path}.{name} has no description. Add an attribute "
            f"docstring (or Field(description=...)) in the model.",
            type="configtable",
            subtype="nodescription",
        )
        return "\\-"
    # First paragraph only. The generated reference section carries the whole thing.
    summary = description.split("\n\n")[0].strip()
    return " ".join(summary.split())


class ConfigTable(Directive):
    required_arguments = 1
    has_content = True
    option_spec = {
        "omit": lambda arg: [n.strip() for n in (arg or "").replace(",", " ").split()],
        "partial": lambda arg: True,
    }

    def run(self):
        model_path = self.arguments[0].strip()
        try:
            model = _import_model(model_path)
        except Exception as exc:
            raise self.error(f"config-table: cannot import '{model_path}': {exc}")

        fields = getattr(model, "model_fields", None)
        if not fields:
            raise self.error(f"config-table: '{model_path}' has no model_fields")

        names: list[str] = []
        overrides: dict[str, str] = {}
        for line in self.content:
            line = line.strip()
            if not line:
                continue
            name, sep, override = line.partition("=")
            name = name.strip()
            names.append(name)
            if sep:
                overrides[name] = override.strip()

        # No body means the whole model, in declaration order.
        if not names:
            names = list(fields)

        omitted = self.options.get("omit", [])

        unknown = [n for n in names + omitted if n not in fields]
        if unknown:
            raise self.error(
                f"config-table: {model_path} has no field(s) {', '.join(unknown)}. "
                f"Available: {', '.join(sorted(fields))}"
            )

        # An omitted field should not also be rendered; that would be contradictory.
        both = sorted(set(names) & set(omitted))
        if both:
            raise self.error(
                f"config-table: {model_path} field(s) {', '.join(both)} are listed in "
                f"the table and in :omit: at the same time"
            )
        names = [n for n in names if n not in omitted]

        env = self.state.document.settings.env
        entry = _doc_data(env, env.docname).setdefault(
            model_path, {"seen": set(), "omitted": set(), "partial": False}
        )
        entry["seen"].update(names)
        entry["omitted"].update(omitted)
        entry["partial"] = entry["partial"] or ("partial" in self.options)

        rows = [
            ".. list-table::",
            "   :header-rows: 1",
            "   :widths: 30 20 50",
            "",
            "   * - Option",
            "     - Default",
            "     - Description",
        ]
        for name in names:
            field = fields[name]
            default = overrides.get(name)
            default = f"``{default}``" if default else _default_text(field)
            rows.append(f"   * - ``{name}``")
            rows.append(f"       *({_type_name(field.annotation)})*")
            rows.append(f"     - {default}")
            rows.append(f"     - {_summary(field, name, model_path)}")

        node = nodes.container()
        source = self.state_machine.get_source_and_line(self.lineno)[0]
        self.state.nested_parse(StringList(rows, source=source), self.content_offset, node)
        return node.children


def _purge_doc(app, env, docname: str) -> None:
    """Drop a document's contribution before it is re-read."""
    getattr(env, _ENV_ATTR, {}).pop(docname, None)


def _merge_info(app, env, docnames, other) -> None:
    """Merge contributions gathered by a parallel-read worker."""
    getattr(env, _ENV_ATTR, {}).update(getattr(other, _ENV_ATTR, {}))


def _check_coverage(app, env) -> None:
    """Report options of a documented model that appear in none of its tables.

    Every model referenced by a config-table is checked - referencing a model is the
    claim that the documentation covers it. Coverage is pooled across every document, so
    a model may legitimately be split over several pages. Use ``:partial:`` to waive the
    check while converting.
    """

    pooled: dict[str, dict] = {}
    for per_model in getattr(env, _ENV_ATTR, {}).values():
        for model_path, entry in per_model.items():
            acc = pooled.setdefault(model_path, {"seen": set(), "omitted": set(), "partial": False})
            acc["seen"] |= entry["seen"]
            acc["omitted"] |= entry["omitted"]
            acc["partial"] = acc["partial"] or entry["partial"]

    for model_path, entry in sorted(pooled.items()):
        if entry["partial"]:
            continue
        try:
            model = _import_model(model_path)
        except Exception:  # pragma: no cover - the directive already errored
            continue

        missing = sorted(set(model.model_fields) - entry["seen"] - entry["omitted"])
        if missing:
            logger.warning(
                f"config-table: these options of {model_path} are in no table: "
                f"{', '.join(missing)}. Add them to a table, list them in :omit:, or "
                f"mark the table :partial: while converting.",
                type="configtable",
                subtype="coverage",
            )


def setup(app):
    app.add_directive("config-table", ConfigTable)
    app.connect("env-purge-doc", _purge_doc)
    app.connect("env-merge-info", _merge_info)
    app.connect("env-check-consistency", _check_coverage)
    return {
        "version": "1.0",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
