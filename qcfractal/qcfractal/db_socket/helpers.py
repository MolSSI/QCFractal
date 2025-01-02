from __future__ import annotations

import dataclasses
import logging
from typing import TYPE_CHECKING

from sqlalchemy import tuple_, and_, or_, func, select, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only, lazyload, defer

from qcfractal.db_socket import BaseORM
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import InsertMetadata, DeleteMetadata
from qcportal.utils import chunk_iterable

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    import sqlalchemy.orm.session
    from typing import (
        Sequence,
        List,
        Tuple,
        Union,
        Any,
        TypeVar,
        Type,
        Dict,
        Generator,
        Optional,
        Iterable,
        Optional,
        Set,
    )

    _ORM_T = TypeVar("_ORM_T", bound=BaseORM)
    _T = TypeVar("_T")
    TupleSequence = Union[Sequence[Tuple[_T, ...]], Generator[Tuple[_T, ...], None, None]]

# Which args to lazy= in a relationsip result in lazy loading
lazy_opts = {"select", "raise", "write_only"}

# A global batch size for all these functions
batchsize = 200

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class _ProjectionDefaultInformation:
    """
    Information about the default columns and relationships for a given ORM type

    This is used to memoize the results of the _get_default_attrs function
    """

    all_attrs: Any
    columns: Set[str]
    relationships: Set[str]

    deferred_columns: Set[str]
    lazy_relationships: Set[str]

    default_columns: Set[str]
    default_relationships: Set[str]


# Cache for inspection information and projection options
_query_defaults_cache: Dict[str, _ProjectionDefaultInformation] = {}
_query_proj_cache = {}


def get_count(session, stmt):
    """
    Returns a total count of an sql query statement

    This should be used before any limit/offset options are incorporated into the query
    """

    return session.scalar(select(func.count()).select_from(stmt.subquery()))


def _get_default_attrs(orm_type: Type[_ORM_T]) -> _ProjectionDefaultInformation:
    """
    Obtain default attributes for an ORM type

    This function returns information about the default columns and relationships for a given ORM type.
    This information is used to determine what columns and relationships are loaded by default when
    querying for ORM objects. This information is memoized.

    Parameters
    ----------
    orm_type
        The ORM type to inspect

    Returns
    -------
    :
        Information about the default columns and relationships for the given ORM type
    """

    key = orm_type.__name__
    if key in _query_defaults_cache:
        return _query_defaults_cache[key]

    mapper = inspect(orm_type)

    # We use mapper.mapper. This works for the usual ORM, as well as
    # with_polymorphic objects
    all_attrs = mapper.mapper.attrs
    columns = set(mapper.mapper.column_attrs.keys())
    relationships = set(mapper.mapper.relationships.keys())

    deferred_columns = set(k for k, v in mapper.mapper.column_attrs.items() if v.deferred)
    lazy_relationships = set(k for k, v in mapper.mapper.relationships.items() if v.lazy in lazy_opts)

    default_columns = set(columns) - set(deferred_columns)
    default_relationships = set(relationships) - set(lazy_relationships)

    ret = _ProjectionDefaultInformation(
        all_attrs=all_attrs,
        columns=columns,
        relationships=relationships,
        deferred_columns=deferred_columns,
        lazy_relationships=lazy_relationships,
        default_columns=default_columns,
        default_relationships=default_relationships,
    )

    _query_defaults_cache[key] = ret
    return ret


def _get_query_proj_options(
    orm_type: Type[_ORM_T], include: Optional[Tuple[str, ...]], exclude: Optional[Tuple[str, ...]]
) -> List[Any]:
    # Adjust include to be the default "None" if * is specified and no exclude is given
    if include and "*" in include and not exclude:
        include = None

    # If include and exclude are both none (common occurrence), then
    # we can skip everything
    if include is None and not exclude:
        return []

    include_set: Optional[Set[str]] = None
    exclude_set: Optional[Set[str]] = None

    if include is not None:
        include_set = set(include)

    if exclude is not None:
        exclude_set = set(exclude)

    # Get information about the defaults
    defaults = _get_default_attrs(orm_type)

    options = []
    if include_set is None and exclude_set:
        # no includes, some excludes
        # load only the non-excluded columns
        # skip loading excluded relationships
        defer_columns = defaults.default_columns & exclude_set
        noload_rels = defaults.default_relationships & exclude_set

        options += [lazyload(defaults.all_attrs[x]) for x in noload_rels]
        options += [defer(defaults.all_attrs[x]) for x in defer_columns]

    elif include_set is not None and not exclude_set:
        # Include, but with no excludes
        # Remove any columns/relationships not specified in the includes
        # (handling of the * should have been done already)
        defer_columns = defaults.default_columns - include_set
        noload_rels = defaults.default_relationships - include_set

        options += [lazyload(defaults.all_attrs[x]) for x in noload_rels]
        options += [defer(defaults.all_attrs[x]) for x in defer_columns]

    elif include_set is not None and exclude_set:
        # Both includes and excludes specified
        if "*" not in include_set:
            defer_columns = (defaults.default_columns - include_set) | (defaults.default_columns & exclude_set)
            noload_rels = (defaults.default_relationships - include_set) | (
                defaults.default_relationships & exclude_set
            )
        else:
            defer_columns = defaults.default_columns & exclude_set
            noload_rels = defaults.default_relationships & exclude_set

        options += [lazyload(defaults.all_attrs[x]) for x in noload_rels]
        options += [defer(defaults.all_attrs[x]) for x in defer_columns]

    else:
        raise RuntimeError(
            f"QCFractal Developer Error: orm_type={orm_type} include_set={include_set} exclude_set={exclude_set}"
        )

    return options


def get_query_proj_options(
    orm_type: Type[_ORM_T],
    include: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
) -> List[Any]:
    """
    Obtain options for an sqlalchemy query

    This function returns a list of objects that can be passed to an sqlalchemy
    .options() function call (as part of a statement) that implements a projection. That is,
    the options will select/deselect columns and relationships that are specified as strings
    to the `include` and `exclude` arguments of this function.

    Not that this function does not allow for including columns or relationships that are not
    included by default
    """

    # Wrap the include/exclude in tuples for memoization
    if include is not None:
        include = tuple(sorted(include))
    if exclude is not None:
        exclude = tuple(sorted(exclude))

    key = (orm_type.__name__, include, exclude)
    if key in _query_proj_cache:
        return _query_proj_cache[key]
    else:
        ret = _get_query_proj_options(orm_type, include, exclude)
        _query_proj_cache[key] = ret
        return ret


def find_all_indices(lst: Sequence[_T], value: _T) -> Tuple[int, ...]:
    """
    Finds all indices of a value in a list or other sequence

    This is somewhat like list.index, however returns a tuple of all indices where
    that value exists
    """

    return tuple(i for i, v in enumerate(lst) if v == value)


def map_duplicates(lst: Sequence[_T]) -> Dict[_T, Tuple[int, ...]]:
    """
    Create a mapping of unique values to indices where they exist in a list

    The keys are the unique entries in the list, and the values are a tuple
    containing the indices where that value was in the list.
    """

    # Written somewhat condensed for performance
    return {el: find_all_indices(lst, el) for el in set(lst)}


def form_query_filter(cols: Sequence[InstrumentedAttribute], values: Sequence[Tuple[Any, ...]]) -> Any:
    """
    Creates an sqlalchemy filter for use in a query

    This forms a filter for searching all the given columns for a sequence of values. For example, you
    may want to search (program, driver, method) for (psi4, energy, b3lyp) or (psi4, gradient, hf).
    In this case, the arguments to values would be ((psi4, energy, b3lyp), (psi4, gradient, hf))

    Parameters
    ----------
    cols
        Columns of an ORM to search for

    values
        Values of those colums to search for

    Returns
    -------
    Any
        An object that can be passed to SQLAlchemy filter() function
    """

    # First, check if there are None values. If so, we need to do something different
    has_none = any(None in x for x in values)

    if has_none:
        logger.warning("Query has None values! This is ok for now but will be deprecated in the future: ")
        logger.warning("Columns: " + str([c.name for c in cols]))

        query_parts = []
        for v in values:
            query_parts.append(and_(x == y for x, y in zip(cols, v)))

        return or_(False, *query_parts)

    else:
        return tuple_(*cols).in_(values)


def get_values(orm: BaseORM, cols: Sequence[InstrumentedAttribute]) -> Tuple:
    """
    Obtains values from an ORM object based on attributes

    Given an ORM object (like a MoleculeORM) and a list/iterable of attributes (like Molecule.id), obtain all
    the values, returning them in a tuple
    """

    return tuple(getattr(orm, x.key) for x in cols)


def unpack(lst: TupleSequence) -> List[_T]:
    """
    Unpack a list of tuples (of variable length) into a flat list

    For example, [(1,2), (3,), (4,5,6)] -> [1,2,3,4,5,6]
    """

    return [x for t in lst for x in t]


def insert_general(
    session: sqlalchemy.orm.session.Session,
    data: Sequence[_ORM_T],
    search_cols: Sequence[InstrumentedAttribute],
    returning: Sequence[InstrumentedAttribute],
    lock_id: int,
) -> Tuple[InsertMetadata, List[Tuple]]:
    """
    Perform a general insert, taking into account existing data

    For each ORM object in data, check if the object/row already exists in the database. If it doesn't exist,
    add it to the database. If the row does exist, data from the existing record will be returned.
    The columns passed to search_cols will be used to determine if the data/row already exists.

    If the row does not exist, but the input record has an auto-incremented primary key set, then
    that is considered an error, and an exception is thrown. This kind of case should be handled before calling
    this function.

    A list of tuples is returned, containing data from the columns specified in ``returning`` (in that order). The order
    of the tuples themselves is the same as was given in the ``data`` list, and will correspond to rows in the database.

    The ORM object passed in through ``data`` may be modified, and they may be attached to the given session upon
    returning. Various fields may be filled in.

    The ``lock_id`` parameter is used to block other inserts into the table for the duration of this insert.
    This is used to prevent duplicate entries.

    .. note::
        This function is used for various fields, such as records. Since records are not unique, we don't
        have a unique constraint and therefore cannot use the ``on_conflict_do_nothing`` clause. Hence the need
        for manual advisory locking

    WARNING: This does not commit the additions to the database, but does flush them.

    Parameters
    ----------
    session
        An existing SQLAlchemy session to use for querying/adding/updating/deleting
    data
        List/Iterable of ORM objects to be added to the database. These objects may be modified in-place.
    search_cols
        What columns to use to determine if data already exists in the database. This is usually in the form
        of [TableORM.id, TableORM.col2], etc
    returning
        What columns to return. This is usually in the form of [TableORM.id, TableORM.col2, etc]
    lock_id
        Unique ID for locking. The ID should be the same for a given table or type of record inserted,
        but different from IDs for other tables or record types.

    Returns
    -------
    :
        Metadata showing what was added/updated, and a list of returned results. The results list
        will contain tuples with whatever data was requested in the returning parameter.
    """

    # Lock for the entire transaction. Even if the caller does more after this
    session.execute(select(func.pg_advisory_xact_lock(lock_id))).scalar()

    n_data = len(data)

    # Return early if not given anything
    if n_data == 0:
        return InsertMetadata(), []

    inserted_idx: List[int] = []
    existing_idx: List[int] = []
    all_ret = []

    for start in range(0, n_data, batchsize):
        ins, ext, ret = _insert_general_batch(session, data[start : start + batchsize], search_cols, returning)
        inserted_idx.extend([start + x for x in ins])
        existing_idx.extend([start + x for x in ext])
        all_ret.extend(ret)

    return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx), all_ret


def insert_mixed_general(
    session: sqlalchemy.orm.session.Session,
    orm_type: Type[_ORM_T],
    data: Sequence[Union[int, _ORM_T]],
    id_col: InstrumentedAttribute,
    search_cols: Sequence[InstrumentedAttribute],
    returning: Sequence[InstrumentedAttribute],
    lock_id: int,
) -> Tuple[InsertMetadata, List[Optional[Tuple]]]:
    """
    Insert mixed input (ids or orm objects) taking into account existing data.

    This function is similar to insert_general, however the input data can be either an ID or an actual ORM to insert.
    If the input is an ID, then it is checked to make sure it exists. Otherwise, the data is attempted to be
    inserted via the same mechanism as insert_general.

    If an ID is given that does not exist, that is marked as an error and the return ID is None.

    See insert_general for more details about insertion.

    Parameters
    ----------
    session
        An existing SQLAlchemy session to use for querying/adding/updating/deleting
    orm_type
        An ORM type to be used (ie, MoleculeORM)
    data
        List/Iterable of ORM objects to be added to the database. These objects may be modified in-place.
    id_col
        What column to use for the ID (like Molecule.id). This column will be used to search for
        entries in ``data`` that are integers.
    search_cols
        What columns to use to determine if data already exists in the database. This is usually in the form
        of [TableORM.id, TableORM.col2], etc
    returning
        What columns to return. This is usually in the form of [TableORM.id, TableORM.col2, etc]
    lock_id
        Unique ID for locking. The ID should be the same for a given table or type of record inserted,
        but different from IDs for other tables or record types.

    Returns
    -------
    :
        Metadata showing what was added/updated, and a list of returned results. The results list
        will contain tuples with whatever data was requested in the returning parameter.
    """

    # Lock for the entire transaction. Even if the caller does more after this
    session.execute(select(func.pg_advisory_xact_lock(lock_id))).scalar()

    n_data = len(data)

    # Return early if not given anything
    if n_data == 0:
        return InsertMetadata(), []

    inserted_idx: List[int] = []
    existing_idx: List[int] = []
    errors: List[Tuple[int, str]] = []
    all_ret = []

    for start in range(0, n_data, batchsize):
        ins, ext, err, ret = _insert_mixed_general_batch(
            session, orm_type, data[start : start + batchsize], id_col, search_cols, returning
        )
        inserted_idx.extend([start + x for x in ins])
        existing_idx.extend([start + x for x in ext])
        errors.extend((start + x, msg) for x, msg in err)
        all_ret.extend(ret)

    return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx, errors=errors), all_ret


def get_general(
    session: sqlalchemy.orm.session.Session,
    orm_type: Type[_ORM_T],
    search_col: InstrumentedAttribute,
    search_values: Sequence[Any],
    include: Optional[Iterable[str]],
    exclude: Optional[Iterable[str]],
    missing_ok: bool,
    additional_options: Optional[Iterable[Any]] = None,
    internal_batchsize: int = 500,
) -> List[Optional[Dict[str, Any]]]:
    """
    Perform a query for records based on a unique id

    For a list of search values, obtain all the records, in input order. This function wraps a simple query
    to make sure that the returned ORM are in the same order as the input, and to optionally check that
    all required records exist.

    If additional options are specified, they are appended to the query statement. These are appended after
    any projection objects created based on include/exclude

    Parameters
    ----------
    session
        An existing SQLAlchemy session to use for querying/adding/updating/deleting
    orm_type
        ORM to search for (MoleculeORM, etc)
    include
        Which columns to include in the return. If specified, other columns will be excluded
    exclude
        Do not return these columns
    search_col
        The column to use for searching the database (typically TableORM.id or similar)
    search_values
        Values of the search column to search for, in order
    missing_ok
        If False, an exception is raised if one of the values is missing. Else, None is returned in the list
        in place of the missing data
    additional_options
        Extra SQLAlchemy options objects to append to the query

    Returns
    -------
    :
        A list of ORM objects in the same order as the search_values parameter.
        These ORM objects will be attached to the session.
        If the record does not exist and missing_ok is True, then the missing entry will be None, still maintaining
        the order of the search_values
    """

    if len(search_values) == 0:
        return []

    # If '**' was requested, that implies '*'
    if include and "**" in include:
        include = set(include) | {"*"}

    # We must make sure the column we are searching for is included
    if include is not None and "*" not in include:
        include = set(include) | {search_col.key}
    if exclude is not None:
        exclude = set(exclude) - {search_col.key}

    unique_values = list(set(search_values))
    proj_options = get_query_proj_options(orm_type, include, exclude)

    col_name = search_col.key
    result_map = {}
    for values_chunk in chunk_iterable(unique_values, internal_batchsize):
        stmt = select(orm_type).filter(search_col.in_(values_chunk))
        stmt = stmt.options(*proj_options)
        if additional_options:
            stmt = stmt.options(*additional_options)

        results = session.execute(stmt).scalars().all()
        result_map |= {r_model[col_name]: r_model for r_model in (r.model_dict() for r in results)}

    # Put into the requested order
    ret = [result_map.get(x, None) for x in search_values]

    if missing_ok is False and None in ret:
        raise MissingDataError("Could not find all requested records")

    return ret


def delete_general(
    session: sqlalchemy.orm.session.Session,
    orm_type: Type[_ORM_T],
    id_col: InstrumentedAttribute,
    ids_to_delete: Sequence[Any],
) -> DeleteMetadata:
    """
    Perform a general delete operation

    For a list of search values, delete all records in the database.

    WARNING: This does not commit the deletions to the database, but does flush them.

    Parameters
    ----------
    session
        An existing SQLAlchemy session to use for querying/adding/updating/deleting
    orm_type
        ORM to search for (MoleculeORM, etc)
    id_col
        Column of the ID to use for selecting records for deleting (typically TableORM.id or similar)
    ids_to_delete
        Delete records with these ids

    Returns
    -------
    :
        Information about what was deleted
    """

    n_ids = len(ids_to_delete)

    # Return early if not given anything
    if n_ids == 0:
        return DeleteMetadata()

    deleted_idx: List[int] = []
    errors: List[Tuple[int, str]] = []

    # Do in batches of 25 for efficiency
    chunk_size = 25
    for chunk_idx, ids_chunk in enumerate(chunk_iterable(ids_to_delete, chunk_size)):
        chunk_start = chunk_idx * chunk_size
        try:
            query_filter = id_col.in_([x[0] for x in ids_chunk])
            with session.begin_nested():
                session.query(orm_type).filter(query_filter).delete()
                deleted_idx.extend(range(chunk_start, chunk_start + len(ids_chunk)))

        except Exception:
            # Have to go one at a time
            for idx, single_id in enumerate(ids_chunk):
                try:
                    with session.begin_nested():
                        session.query(orm_type).filter(id_col == single_id).delete()
                        deleted_idx.append(chunk_start + idx)
                except IntegrityError:
                    err_msg = f"Integrity Error - may still be referenced"
                    errors.append((chunk_start + idx, err_msg))
                except Exception as e:
                    err_msg = f"Attempting to delete resulted in error: orm_type={orm_type.__name__}, id_col={id_col.key}, idx={idx}, search_value={single_id}, error={str(e)}"
                    errors.append((chunk_start + idx, err_msg))

    session.flush()

    return DeleteMetadata(deleted_idx=deleted_idx, errors=errors)


def _insert_general_batch(
    session: sqlalchemy.orm.session.Session,
    data: Sequence[_ORM_T],
    search_cols: Sequence[InstrumentedAttribute],
    returning: Sequence[InstrumentedAttribute],
) -> Tuple[List[int], List[int], List[Tuple]]:
    """
    Inserts a batch of data to the session. See documentation for insert_general

    Not meant for general use - should only be called from insert_general

    This returns the raw inserted idx and existing idx, rather than InsertMetadata. This is then
    collated in insert_general into that model
    """

    # Return early if the size of this batch is zero
    if len(data) == 0:
        return [], [], []

    # Build up a big query for all existing data
    search_values = [get_values(r, search_cols) for r in data]

    # Find and partition all duplicates in the list
    search_values_unique_map = map_duplicates(search_values)

    # We query for both the return values and what we are searching for
    query_filter = form_query_filter(search_cols, search_values_unique_map.keys())

    query = session.query(*search_cols, *returning)
    query = query.filter(query_filter)

    # Needed in case of duplicates
    query = query.distinct(*search_cols)

    query_results = query.all()

    # Partition each result into two tuples
    # The first tuple is the value of the search columns
    # The second tuple is the data to return
    n_search_cols = len(search_cols)
    existing_results = [(x[:n_search_cols], x[n_search_cols:]) for x in query_results]

    # Find out all existing idx
    existing_idx: List[int] = unpack([search_values_unique_map[x[0]] for x in existing_results])

    # Determine which of the search values we are missing, and what are the original indices of those missing values
    search_values_found = set(x[0] for x in existing_results)
    search_values_missing = set(search_values) - search_values_found

    # Contains tuples. Each tuple contains indices of duplicates
    missing_idx = [search_values_unique_map[x] for x in search_values_missing]

    # TODO: can we bulk add here, since now we don't have duplicates or existing data, and no errors
    # But then we might need another query at the end
    for idxs in missing_idx:
        # Only need one of the records, since the rest are equivalent
        rec = data[idxs[0]]
        session.add(rec)

    session.flush()

    # For inserted, we say we only inserted the first one. The rest are considered duplicates
    inserted_idx = [x[0] for x in missing_idx]
    existing_idx.extend(unpack(x[1:] for x in missing_idx))

    # Get the fields we should be returning from the full orm that we added
    ret_added = []
    for idxs in missing_idx:
        # Only the first one was added and now contains the relevant data
        ret_data = get_values(data[idxs[0]], returning)
        ret_added.extend([(idx, ret_data) for idx in idxs])

    # Now from existing
    ret_existing: List[Tuple[int, Tuple[Any, ...]]] = []
    for sv, r in existing_results:
        idxs = search_values_unique_map[sv]
        ret_existing.extend((idx, r) for idx in idxs)

    # combine the two result lists, sort, and flatten
    ret = [x[1] for x in sorted(ret_added + ret_existing)]

    return inserted_idx, existing_idx, ret


def _insert_mixed_general_batch(
    session: sqlalchemy.orm.session.Session,
    orm_type: Type[_ORM_T],
    data: Sequence[Union[int, _ORM_T]],
    id_col: InstrumentedAttribute,
    search_cols: Sequence[InstrumentedAttribute],
    returning: Sequence[InstrumentedAttribute],
) -> Tuple[List[int], List[int], List[Tuple[int, str]], List[Optional[Tuple]]]:
    """
    Insert a batched of mixed input (ids or orm objects) taking into account existing data.

    Not meant for general use - should only be called from insert_mixed_general
    """

    # Return early if the size of this batch is zero
    if len(data) == 0:
        return [], [], [], []

    # ORM objects passed in. Contains a tuple of index in the data list and the Molecule object
    input_orm: List[Tuple[int, _ORM_T]] = []

    # IDs passed in. Contains a tuple of (index, id)
    input_ids: List[Tuple[int, int]] = []

    # Any errors we want to return. Tuple of (index, error message)
    errors: List[Tuple[int, str]] = []

    for idx, m in enumerate(data):
        if isinstance(m, int):
            input_ids.append((idx, m))
        elif isinstance(m, orm_type):
            input_orm.append((idx, m))
        else:
            errors.append((idx, f"Data type for insert_mixed not understood: {type(m)}"))

    # Add all the data that are ORM objects
    orm_to_add = [x[1] for x in input_orm]
    inserted_idx_tmp, existing_idx_tmp, added_data = _insert_general_batch(session, orm_to_add, search_cols, returning)

    # All the returned info is in the same order as in the input list (input_orm/orm_to_add in this case)
    # Look up the original indices
    all_ret: List[Tuple[int, Optional[Tuple]]] = [(idx, x) for (idx, _), x in zip(input_orm, added_data)]

    # Adjust the indices we just got in the metadata from the insert. They correspond to the indices
    # in input_orm, so we look up the original indices there
    inserted_idx = [input_orm[x][0] for x in inserted_idx_tmp]
    existing_idx = [input_orm[x][0] for x in existing_idx_tmp]

    # Now make sure all the ids specified in data actually exist
    ids_to_get = [(x[1],) for x in input_ids]
    found_id_ret = session.query(id_col, *returning).filter(id_col.in_(ids_to_get)).all()
    found_id_map = {x[0]: tuple(x[1:]) for x in found_id_ret}

    # Map what we found back to the original indices.
    for idx, iid in input_ids:
        v = found_id_map.get(iid, None)
        all_ret.append((idx, v))
        if v is None:
            # This missing indices are errors in this case.
            errors.append((idx, f"{orm_type.__name__} object with id={iid} was not found in the database"))
        else:
            existing_idx.append(idx)

    # Sort the return (remember it is a list of tuples) which will sort by index (the first element)
    return inserted_idx, existing_idx, errors, [x[1] for x in sorted(all_ret)]
