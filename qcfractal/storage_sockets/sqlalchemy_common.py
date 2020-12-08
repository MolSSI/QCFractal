from .storage_meta import UpsertMetadata, InsertMetadata

from typing import Sequence, Iterable, List, Tuple, Union, TypeVar
from qcfractal.storage_sockets.models import Base

ORM_T = TypeVar("ORM_T", bound=Base)


def _upsert_general(
    session: "sqlalchemy.orm.session.Session",
    data: Sequence[ORM_T],
    search_cols: Iterable[str],
    update_cols: Iterable[str],
) -> Tuple[UpsertMetadata, List[Union[ORM_T, None]]]:
    """
    Perform a general insert/update, taking into account existing data

    For each ORM object in data, check if the object/row already exists in the database. If it doesn't exist,
    add it to the database. If the row does exist, the columns listed in update_cols will be updated and committed
    to the database. The columns passed to search_cols will be used to determine if the data/row already exists.

    If the row does not exist, but the input record has an auto-incremented primary key set, then
    that is considered an error.

    A list of ORM objects is returned. The order is the same as was given in the data list. These will
    correspond to rows in the database and will have primary keys, etc, filled in. These may alias variables
    in the data parameter. If an error occurs for a particular record, that index in the returned data will be None.

    The returned ORM objects are attached to the given session, but the records are committed

    Objects contained in the data parameter may be modified.


    Parameters
    ----------
    session: sqlalchemy.orm.session.Session
        An existing SQLAlchemy session to use for adding/updating
    data: Sequence[ORM_T]
        List/Sequence of ORM objects to be added to the database. These objects may be modified in-place.
    search_cols: Iterable[str]
        What columns to use to determine if data already exists in the database.
    update_cols: Iterable[str]
        For existing data, update these columns in the table to values given in the data dictionary

    Returns
    -------
    Tuple[UpsertMetadata, List[Union[Base, None]]
        Metadata showing what was added/updated, and a list of ORM objects. These
        ORM objects reflect updated or automatically-generated database fields (for example, ids).
        If an error occurs, the corresponding entry in the list will be None.
    """

    # These are the records/ORM objects to be returned
    ret = []

    # Indices of the returned list corresponding to inserted and existing records
    inserted_idx = []
    updated_idx = []
    errors = []

    # This is the type of the ORM. It should be the same for all items in the list
    orm_type = type(data[0])
    assert all(type(x) == orm_type for x in data)

    for idx, rec in enumerate(data):
        # Build up the filter dictionary
        query = {}
        for c in search_cols:
            query[c] = getattr(rec, c)

        # Now, does this record exist?
        existing = session.query(orm_type).filter_by(**query).all()

        # More than one record is an error
        if len(existing) > 1:
            err_msg = f"Cannot upsert with multiple returned records. orm_type = {str(orm_type)}, query = {str(query)}"
            errors.append((idx, err_msg))
            ret.append(None)
        elif len(existing) == 0:
            # Nothing found. We need to add
            # If an auto-incremented primary key is set in the input, then that is an error
            auto_pkey = rec.get_autoincrement_pkey()

            if getattr(rec, auto_pkey) is not None:
                err_msg = f"Attempting to insert with {auto_pkey} set, but does not exist in the database"
                errors.append((idx, err_msg))
                ret.append(None)
            else:
                session.add(rec)
                inserted_idx.append(idx)
                ret.append(rec)
        else:
            # Existing record found
            updated_idx.append(idx)

            # Do update. These changes are propagated to the database on commit
            for c in update_cols:
                new_value = getattr(rec, c)
                setattr(existing[0], c, new_value)

            ret.append(existing[0])

    session.commit()

    meta = UpsertMetadata(inserted_idx=inserted_idx, updated_idx=updated_idx, errors=errors)
    return meta, ret


def _insert_general(
    session: "sqlalchemy.orm.session.Session", data: Sequence[ORM_T], search_cols: Iterable[str]
) -> Tuple[UpsertMetadata, List[Union[ORM_T, None]]]:
    """
    Perform a general insert, taking into account existing data

    For each ORM object in data, check if the object/row already exists in the database. If it doesn't exist,
    add it to the database. If the row does exist, the existing record will be returned.
    to the database. The columns passed to search_cols will be used to determine if the data/row already exists.

    If the row does not exist, but the input record has an auto-incremented primary key set, then
    that is considered an error.

    A list of ORM objects is returned. The order is the same as was given in the data list. These will
    correspond to rows in the database and will have primary keys, etc, filled in. These may alias variables
    in the data parameter. If an error occurs for a particular record, that index in the returned data will be None.

    The returned ORM objects are attached to the given session, but the records are committed

    Objects contained in the data parameter may be modified.


    Parameters
    ----------
    session: sqlalchemy.orm.session.Session
        An existing SQLAlchemy session to use for adding/updating
    data: Sequence[ORM_T]
        List/Sequence of ORM objects to be added to the database. These objects may be modified in-place.
    search_cols: Iterable[str]
        What columns to use to determine if data already exists in the database.

    Returns
    -------
    Tuple[InsertMetadata, List[Union[Base, None]]
        Metadata showing what was added/updated, and a list of ORM objects. These
        ORM objects reflect existing or automatically-generated database fields (for example, ids).
        If an error occurs, the corresponding entry in the list will be None.
    """

    # These are the records/ORM objects to be returned
    ret = []

    # Indices of the returned list corresponding to inserted and existing records
    inserted_idx = []
    existing_idx = []
    errors = []

    # This is the type of the ORM. It should be the same for all items in the list
    orm_type = type(data[0])
    assert all(type(x) == orm_type for x in data)

    for idx, rec in enumerate(data):
        # Build up the filter dictionary
        query = {}
        for c in search_cols:
            query[c] = getattr(rec, c)

        # Now, does this record exist?
        existing = session.query(orm_type).filter_by(**query).all()

        # More than one record is ok (?)
        if len(existing) == 0:
            # Nothing found. We need to add
            # If an auto-incremented primary key is set in the input, then that is an error
            auto_pkey = rec.get_autoincrement_pkey()

            if getattr(rec, auto_pkey) is not None:
                err_msg = f"Attempting to insert with {auto_pkey} set, but does not exist in the database"
                errors.append((idx, err_msg))
                ret.append(None)
            else:
                session.add(rec)
                inserted_idx.append(idx)
                ret.append(rec)
        else:
            # Existing record found
            # We may have found more than one, but we will just take the first one
            existing_idx.append(idx)
            ret.append(existing[0])

    session.commit()

    meta = InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx, errors=errors)
    return meta, ret
