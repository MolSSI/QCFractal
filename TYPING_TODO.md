# Typing / correctness follow-ups

Findings from the type-hint pass over `qcportal/client.py` and `qcportal/client_base.py`
(2026-08-13). Nothing in this file was changed by that pass — these are all items that were
deliberately left alone because they change runtime behavior, are outside qcportal, or are
larger than a hint fix.

Reproduce the surveys below with:

```
mypy --config-file mypy.ini qcportal/qcportal/client.py qcportal/qcportal/client_base.py
MYPYPATH=$PWD/qcportal mypy --config-file mypy.ini qcfractal/qcfractal
```

---

## A. qcportal — behavior bugs found while fixing hints

### A1. `PortalClientBase.__init__`: the http/SSL special case is dead code
`qcportal/qcportal/client_base.py` (~line 110)

```python
# If we are `http`, ignore all SSL directives
if not address.startswith("https"):
    self._verify = True
...
self._verify = verify   # <- a few lines later, unconditional
```

The `verify` argument always wins, so the comment describes something that does not happen.
Either drop the first branch or move the assignment above it. Decide which behavior is
intended before changing it — `verify=False` over http currently still disables verification
(harmless) but also still triggers the `urllib3.disable_warnings` call.

### A2. `QueryIteratorBase.reset()` does not reset the pagination cursor
`qcportal/qcportal/base_models.py`

`reset()` clears `_current_batch` and `_fetched`, but not `_query_filters.cursor`.
`_fetch_batch()` only writes the cursor when the previous batch was non-empty, so after an
iterator has been exhausted, `reset()` leaves the old cursor in place and re-iteration yields
nothing. Fix is one line (`self._query_filters.cursor = None` in `reset()`), but it is a
behavior change and deserves a test.

### A3. `requests.packages.urllib3` is a legacy alias
`qcportal/qcportal/client_base.py:149` — the only remaining mypy error in the two client files
(`Module has no attribute "urllib3"`).

`requests.packages.urllib3 is urllib3` is `True` for every supported requests version
(verified against requests 2.34.2 / urllib3 2.7.0), so
`urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)` is equivalent
and type-checks. Left alone only because it is a runtime change.

### A4. `is_single` treats a `set` of IDs as a single ID
`_get_records_by_type` and `get_molecules` use `is_single = not isinstance(ids, Sequence)`.
A `set` is not a `Sequence`, so `client.get_records({1, 2, 3})` fetches three records and
returns only the first. The type hints say `Sequence[int]`, so a set is out of contract, but
`make_list()` accepts sets everywhere else and this fails silently.

Note: the obvious fix (`isinstance(ids, int)`) changes behavior for numpy integer scalars,
which are not `int` instances but are currently treated as a single ID. Needs a deliberate
decision.

### A5. `make_list()` wraps generators instead of expanding them
`qcportal/qcportal/utils.py`

Only `list`, `str`, `set`, and `Sequence` are special-cased; everything else is wrapped in a
one-element list. So a generator, `dict.keys()`, or any other non-Sequence iterable becomes
`[<generator>]` and fails server-side validation.

This makes the pervasive `Optional[Union[str, Iterable[str]]]` query-parameter hints in
`client.py` too permissive — they advertise support for iterables that do not work. Two ways
out:

1. Narrow ~200 parameter annotations from `Iterable[X]` to `Sequence[X]` (honest, big diff,
   arguably narrows the public API); or
2. Teach `make_list()` to expand iterators (objects with `__next__`) while still wrapping
   pydantic models and dicts. Then the existing hints become true.

The overloads on `make_list` currently mirror runtime exactly, so whichever route is taken,
they need updating with it.

---

## B. qcportal — files not covered by this pass

Error counts from the run above (these are all *pre-existing*; the pass introduced none):

| file | errors |
| --- | --- |
| `cache.py` | 51 |
| `dataset_models.py` | 38 |
| `neb/record_models.py` | 26 |
| `record_models.py` | 24 |
| `reaction/record_models.py` | 16 |
| `torsiondrive/record_models.py` | 14 |
| `manybody/record_models.py` | 11 |
| `gridoptimization/record_models.py` | 11 |
| `optimization/record_models.py` | 9 |
| `project_models.py` | 7 |
| `singlepoint/record_models.py` | 5 |
| `reaction/dataset_models.py` | 5 |
| everything else | 1-4 each |

Dominant patterns, in rough order of payoff:

### B1. Lazily-fetched fields declared non-Optional
The `x_` / `@property x` pattern in `record_models.py` (`compute_history`, `comments`,
`native_files`, `outputs`, ...) — the backing field is `... | None`, the property is annotated
as the non-Optional type, and the property body returns the field directly after a fetch. mypy
flags the return; a human reads it as "this is never None after fetching". Either assert or
narrow inside the property. ~19 `return-value` + ~26 `union-attr` errors across
`record_models.py` and the per-type record models come from this one shape.

### B2. `client` parameters are untyped
`propagate_client(self, client, ...)`, `record_from_dict(data, client: Any = None, ...)` and
friends take `Any`, which erases checking at every record/dataset construction site. A
`TYPE_CHECKING` import of `PortalClient` plus `Optional[PortalClient]` would restore it.
Circular-import safe because of `from __future__ import annotations`.

### B3. `BaseRecord` has no `specification`
`record_models.py:970` (`compare_base_records`) accesses `record.specification`, which only
exists on subclasses. Either move the attribute up as an abstract property or narrow the
function's parameter types.

### B4. Missing stubs
`types-python-dateutil` would clear the `dateutil.parser` `import-untyped` errors in
`record_models.py`, `managers/models.py`, `internal_jobs/models.py`, `serverinfo/models.py`.
Worth adding to the dev/CI environment rather than to `mypy.ini`.

### B5. `cache.py` (51 errors)
Not investigated. Largest single remaining file in qcportal.

---

## C. qcfractal — surveyed only, nothing touched

`MYPYPATH=$PWD/qcportal mypy --config-file mypy.ini qcfractal/qcfractal`
→ **1425 errors in 120 files** (203 files checked).

By category: `attr-defined` 432, `arg-type` 272, `assignment` 169, `union-attr` 113,
`misc` 102, `index` 74, `return-value` 57, `no-redef` 40.

By file (top offenders):

| file | errors |
| --- | --- |
| `components/base_dataset_socket.py` | 186 |
| `components/record_socket.py` | 55 |
| `components/neb/record_socket.py` | 55 |
| `components/reaction/record_socket.py` | 50 |
| `components/dataset_routes.py` | 47 |
| `components/manybody/record_socket.py` | 44 |
| `components/gridoptimization/record_socket.py` | 41 |
| `components/torsiondrive/record_socket.py` | 35 |
| `components/register_all.py` | 34 |
| `components/record_routes.py` | 27 |

### C1. ORM class-attribute placeholders (~130 errors in one file, more in the record sockets)
`components/base_dataset_socket.py:36-40`

```python
class BaseDatasetSocket:
    dataset_orm = None
    specification_orm = None
    entry_orm = None
    record_item_orm = None
    record_orm = None
```

Subclasses assign real ORM classes, but mypy infers the base-class type as `None`, so every
`self.entry_orm.record_id` in the base class is an error. Declaring them without a value
(`dataset_orm: ClassVar[Type[BaseORM]]`) fixes the whole category at once. The `assert ... is
not None` lines in `__init__` can then go too.

### C2. `LocalProxy[SQLAlchemySocket]` does not forward attributes (~120 errors)
`components/record_routes.py`, `dataset_routes.py`, `project_routes.py`, `auth/routes.py`.

Every `storage_socket.records...` is `"LocalProxy[SQLAlchemySocket]" has no attribute
"records"`. werkzeug's `LocalProxy` is generic but does not proxy attribute access in its
stubs. One typed accessor (a module-level `storage_socket: SQLAlchemySocket = cast(...)`, or a
small `get_socket()` helper) would clear all of them.

### C3. Real contract question in `base_dataset_socket.py:123,127`
A function annotated `-> tuple[str, PriorityEnum, int]` builds its result from values typed
`str | None` and `int | None` (`default_compute_tag` / `default_compute_priority` /
`owner_user_id`). Worth deciding whether those can genuinely be None at that point, since the
client-facing defaults depend on it.

### C4. Duplicate function definition — possible bug
`flask_app/handlers.py:165` and `:171` both define `handle_compute_manager_error`. Flask
registers both via the decorators so it works at runtime, but the first (for
`SecurityNotEnabledError`, returning 401) is shadowed in the module namespace and carries a
copy-pasted comment. Looks like an editing artifact; worth a look.

### C5. `no-redef` noise from imports-for-side-effect
`flask_app/flask_app.py:95-96` — `from .api_v1 import routes`, `from .auth_v1 import routes`,
`from .compute_v1 import routes`. Intentional, but each rebinds `routes`. `importlib
.import_module(...)` or `as _api_routes` aliases would silence it.

---

## D. Suggested ordering

1. A3, C4 — one-liners, no behavior risk to speak of.
2. C1, C2 — highest error-per-change ratio in qcfractal (~250 errors between them).
3. A2 — small fix, needs a test.
4. B1, B2 — the two patterns behind most remaining qcportal errors.
5. A5 (with A4) — needs an API decision first: are `Iterable` query parameters supposed to
   accept generators or not?
6. B5, C3 — investigate.
