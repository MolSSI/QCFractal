from __future__ import annotations
from ..interface.models.query_meta import InsertMetadata, DeleteMetadata
from qcfractal.storage_sockets.models import Base
from sqlalchemy import tuple_, and_, literal

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    import sqlalchemy.orm.session
    from typing import Sequence, List, Tuple, Union, Any, TypeVar, Type, Dict, Generator, Optional, Iterable

    _ORM_T = TypeVar("_ORM_T", bound=Base)
    _T = TypeVar("_T")
    TupleSequence = Union[Sequence[Tuple[_T, ...]], Generator[Tuple[_T, ...], None, None]]


# A global batch size for all these functions
batchsize = 200


def get_count(query):
    """
    Returns a total count of a query

    This should be used before any limit/skip options are incorporated into the query
    """

    # TODO - sqlalchemy 1.4 broke the "fast" way. Reverting to the slow way
    # TODO - should only be used sparingly (maybe for the first page?)

    # count_q = query.statement.with_only_columns([func.count()]).order_by(None)
    # count = query.session.execute(count_q).scalar()
    return query.count()


def get_query_proj_columns(
    orm_type: Type[_ORM_T], include: Optional[Iterable[str]] = None, exclude: Optional[Iterable[str]] = None
) -> Tuple[Tuple[str, ...], Tuple[InstrumentedAttribute, ...]]:

    # If include is none, then include all the columns
    if include is None:
        # Do not include hybrid or relationships
        col_list, _, _ = orm_type._get_col_types()
    else:
        col_list = list(include)

    if exclude is not None:
        col_list = [x for x in col_list if x not in exclude]

    # Now get the actual SQLAlchemy attributes of the ORM class
    attrs = tuple(getattr(orm_type, x) for x in col_list)
    attrs_names = tuple(x.name for x in attrs)
    return attrs_names, attrs


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


def get_values(orm: Base, cols: Sequence[InstrumentedAttribute]) -> Tuple:
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

    Returns
    -------
    :
        Metadata showing what was added/updated, and a list of returned results. The results list
        will contain tuples with whatever data was requested in the returning parameter.
    """

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

    return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx), all_ret  # type: ignore


def insert_mixed_general(
    session: sqlalchemy.orm.session.Session,
    orm_type: Type[_ORM_T],
    data: Sequence[Union[int, _ORM_T]],
    id_col: InstrumentedAttribute,
    search_cols: Sequence[InstrumentedAttribute],
    returning: Sequence[InstrumentedAttribute],
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

    Returns
    -------
    :
        Metadata showing what was added/updated, and a list of returned results. The results list
        will contain tuples with whatever data was requested in the returning parameter.
    """

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

    return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx, errors=errors), all_ret  # type: ignore


# def get_general(
#    session: sqlalchemy.orm.session.Session,
#    orm_type: Type[_ORM_T],
#    search_cols: Sequence[InstrumentedAttribute],
#    search_values: Sequence[Sequence[Any]],
#    missing_ok: bool = False,
# ) -> List[Optional[_ORM_T]]:
#    """
#    Perform a general query based on a single column
#
#    For a list of search values, obtain all the records, in input order. This function wraps a simple query
#    to make sure that the returned ORM are in the same order as the input, and to optionally check that
#    all required records exist.
#
#    Parameters
#    ----------
#    session
#        An existing SQLAlchemy session to use for querying/adding/updating/deleting
#    orm_type
#        ORM to search for (MoleculeORM, etc)
#    search_cols
#        The columns to use for searching the database (typically (TableORM.id,) or similar)
#    search_values
#        Values of the search column to search for, in order. Typically a list of Tuples
#    missing_ok
#        If False, an exception is raised if one of the values is missing. Else, None is returned in the list
#        in place of the missing data
#
#    Returns
#    -------
#    :
#        A list of ORM objects in the same order as the search_values parameter.
#        These ORM objects will be attached to the session.
#        If the record does not exist and missing_ok is True, then the missing entry will be None, still maintaining
#        the order of the search_values
#    """
#
#    n_search_values = len(search_values)
#
#    # Return early if not given anything
#    if n_search_values == 0:
#        return []
#
#    all_ret = []
#
#    for start in range(0, n_search_values, batchsize):
#        ret = _get_general_batch(session, orm_type, search_cols, search_values[start : start + batchsize], missing_ok)
#        all_ret.extend(ret)
#
#    return all_ret


# def get_general_proj(
#    session: sqlalchemy.orm.session.Session,
#    orm_type: Type[_ORM_T],
#    search_cols: Sequence[InstrumentedAttribute],
#    search_values: Sequence[Sequence[Any]],
#    returning: Sequence[InstrumentedAttribute],
#    missing_ok: bool = False,
# ) -> List[Optional[Tuple]]:
#    """
#    Perform a general query based on a single column, returning only specified columns
#
#    For a list of search values, obtain all the records, in input order. This function wraps a simple query
#    to make sure that the returned data is in the same order as the input, and to optionally check that
#    all required records exist.
#
#    Parameters
#    ----------
#    session
#        An existing SQLAlchemy session to use for querying/adding/updating/deleting
#    orm_type
#        ORM to search for (MoleculeORM, etc)
#    search_cols
#        The columns to use for searching the database (typically (TableORM.id,) or similar)
#    search_values
#        Values of the search column to search for, in order. Typically a list of Tuples
#    returning
#        What columns to return. This is usually in the form of [TableORM.id, TableORM.col2, etc]
#    missing_ok
#        If False, an exception is raised if one of the values is missing. Else, None is returned in the list
#        in place of the missing data
#
#    Returns
#    -------
#    :
#        A list of requested data in the same order as the search_values parameter.
#        If the record does not exist and missing_ok is True, then the missing entry will be None, still maintaining
#        the order of the search_values
#    """
#
#    n_search_values = len(search_values)
#
#    # Return early if not given anything
#    if n_search_values == 0:
#        return []
#
#    all_ret: List[Optional[Tuple]] = []
#
#    for start in range(0, n_search_values, batchsize):
#        ret = _get_general_proj_batch(
#            session, orm_type, search_cols, search_values[start : start + batchsize], returning, missing_ok
#        )
#        all_ret.extend(ret)
#
#    return all_ret


# def exists_general(
#    session: sqlalchemy.orm.session.Session,
#    orm_type: Type[_ORM_T],
#    search_cols: Sequence[InstrumentedAttribute],
#    search_values: Sequence[Sequence[Any]],
# ) -> List[bool]:
#    """
#    Determine if rows exist in the database
#
#    This will see if the specified entries exist in the database, and return a list of boolean
#    (in the same order of ``search_values``) specifying True if the row was found, or False if it wasn't.
#
#    Parameters
#    ----------
#    session
#        An existing SQLAlchemy session to use for querying/adding/updating/deleting
#    orm_type
#        ORM to search for (MoleculeORM, etc)
#    search_cols
#        The columns to use for searching the database (typically (TableORM.id,) or similar)
#    search_values
#        Values of the search column to search for, in order. Typically a list of Tuples
#
#    Returns
#    -------
#    :
#        A list where each entry corresponds to a single search value. This will be in the order
#        of ``search_values``
#    """
#
#    exists = get_general_proj(session, orm_type, search_cols, search_values, (literal(True),), True)
#    return [x is not None for x in exists]


def delete_general(
    session: sqlalchemy.orm.session.Session,
    orm_type: Type[_ORM_T],
    search_cols: Sequence[InstrumentedAttribute],
    search_values: Sequence[Any],
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
    search_cols
        The column to use for searching the database (typically TableORM.id or similar)
    search_values
        Values of the search column to search for, in order

    Returns
    -------
    :
        Information about what was deleted
    """

    n_search_values = len(search_values)

    # Return early if not given anything
    if n_search_values == 0:
        return DeleteMetadata()

    deleted_idx: List[int] = []
    missing_idx: List[int] = []
    errors: List[Tuple[int, str]] = []

    # Do one at a time to catch errors
    # We don't delete a whole lot, so this shouldn't be a bottleneck
    for idx, search_value in enumerate(search_values):
        try:
            q = [x == y for x, y in zip(search_cols, search_value)]
            n_deleted = session.query(orm_type).filter(and_(*q)).delete()
            if n_deleted == 0:
                missing_idx.append(idx)
            else:
                deleted_idx.append(idx)
        except Exception as e:
            scols = [x.key for x in search_cols]
            err_msg = f"Attempting to delete resulted in error: orm_type={orm_type.__name__}, search_cols={scols}, idx={idx}, search_value={search_value}, error={str(e)}"
            errors.append((idx, err_msg))

    session.flush()

    return DeleteMetadata(deleted_idx=deleted_idx, missing_idx=missing_idx, errors=errors)  # type: ignore


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

    # Handle the case where we have an autoincremented primary key already set in the orm
    # If an auto-incremented primary key is set in the input, then that is an exception
    # since this should have been fixed elsewhere in qcfractal
    auto_pkey = data[0].get_autoincrement_pkey()
    for rec in data:
        # idx is a tuple of equivalent indices
        # We only need to grab the first element of each tuple, as the rest are considered unique
        # based on our search criteria
        if getattr(rec, auto_pkey) is not None:
            raise RuntimeError(f"Autoincrement pkey is set when it shouldn't be. This is a QCFractal developer error")

    # Build up a big query for all existing data
    search_values = [get_values(r, search_cols) for r in data]

    # Find and partition all duplicates in the list
    search_values_unique_map = map_duplicates(search_values)

    # We query for both the return values and what we are searching for
    query_results = (
        session.query(*search_cols, *returning).filter(tuple_(*search_cols).in_(search_values_unique_map.keys())).all()
    )

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


# def _get_general_batch(
#    session: sqlalchemy.orm.session.Session,
#    orm_type: Type[_ORM_T],
#    search_cols: Sequence[InstrumentedAttribute],
#    search_values: Sequence[Sequence[Any]],
#    missing_ok: bool = False,
# ) -> List[Optional[_ORM_T]]:
#    """
#    Gets a batch of data from the database. See documentation for get_general
#
#    Not meant for general use - should only be called from get_general
#    """
#
#    # Return early if not given anything
#    if len(search_values) == 0:
#        return []
#
#    # Find and partition all duplicates in the list
#    search_values_unique_map = map_duplicates(search_values)
#
#    # Actually perform the query here
#    query_results = session.query(orm_type).filter(tuple_(*search_cols).in_(search_values_unique_map.keys())).all()
#
#    # Partition each result into two tuples
#    # The first tuple is the value of the search columns
#    # The second tuple is the data to return
#    existing_results = [(get_values(x, search_cols), x) for x in query_results]
#
#    # Determine which of the search values we are missing, and what are the original indices of those missing values
#    search_values_found = set(x[0] for x in existing_results)
#    search_values_missing = set(search_values) - search_values_found
#    missing_idx = [search_values_unique_map[x] for x in search_values_missing]
#
#    # If missing data is not ok, raise an exception
#    if len(missing_idx) > 0 and missing_ok is False:
#        scols = [x.key for x in search_cols]
#        raise RuntimeError(
#            f"Could not find all requested records. orm_type={orm_type.__name__}, search_col={scols}, search_values={search_values}. Missing indices: {missing_idx}"
#        )
#
#    # Now create a list of (idx, return_data) from our unique map and query results
#    ret: List[Tuple[int, Optional[_ORM_T]]] = []
#
#    for sv, r in existing_results:
#        idxs = search_values_unique_map[sv]
#        ret.extend((idx, r) for idx in idxs)
#
#    # And everything we didn't find
#    for idxs in missing_idx:
#        ret.extend((idx, None) for idx in idxs)
#
#    # Sanity check. All indices are accounted for
#    assert set(x[0] for x in ret) == set(range(len(search_values)))
#
#    # Only return the second part (ie, not the index)
#    # Also sort first by index
#    return [x[1] for x in sorted(ret)]


# def _get_general_proj_batch(
#    session: sqlalchemy.orm.session.Session,
#    orm_type: Type[_ORM_T],
#    search_cols: Sequence[InstrumentedAttribute],
#    search_values: Sequence[Sequence[Any]],
#    returning: Sequence[InstrumentedAttribute],
#    missing_ok: bool = False,
# ) -> List[Optional[Tuple]]:
#    """
#    Gets a batch of data from the database, returning only selected columns. See documentation for get_general_proj
#
#    Not meant for general use - should only be called from get_general_proj
#    """
#
#    # Return early if not given anything
#    if len(search_values) == 0:
#        return []
#
#    # Find and partition all duplicates in the list
#    search_values_unique_map = map_duplicates(search_values)
#
#    # We query for both the return values and what we are searching for
#    query_results = (
#        session.query(*search_cols, *returning).filter(tuple_(*search_cols).in_(search_values_unique_map.keys())).all()
#    )
#
#    # Partition each result into two tuples
#    # The first tuple is the value of the search columns
#    # The second tuple is the data to return
#    n_search_cols = len(search_cols)
#    existing_results = [(x[:n_search_cols], x[n_search_cols:]) for x in query_results]
#
#    # Determine which of the search values we are missing, and what are the original indices of those missing values
#    search_values_found = set(x[0] for x in existing_results)
#    search_values_missing = set(search_values) - search_values_found
#    missing_idx = [search_values_unique_map[x] for x in search_values_missing]
#
#    # If missing data is not ok, raise an exception
#    if len(missing_idx) > 0 and missing_ok is False:
#        raise RuntimeError(
#            f"Could not find all requested records. orm_type={orm_type.__name__}, search_col={search_cols}, search_values={search_values}. Missing indices: {missing_idx}"
#        )
#
#    # Now create a list of (idx, return_data) from our unique map and query results
#    ret: List[Tuple[int, Optional[Tuple]]] = []
#
#    for sv, r in existing_results:
#        idxs = search_values_unique_map[sv]
#        ret.extend((idx, r) for idx in idxs)
#
#    # And everything we didn't find
#    for idxs in missing_idx:
#        ret.extend((idx, None) for idx in idxs)
#
#    # Sanity check. All indices are accounted for
#    assert set(x[0] for x in ret) == set(range(len(search_values)))
#
#    # Only return the second part (ie, not the index)
#    # Also sort first by index
#    return [x[1] for x in sorted(ret)]
