from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, delete, func, union
from sqlalchemy.orm import load_only, lazyload, joinedload, selectinload, with_polymorphic

from qcfractal.components.datasets.db_models import BaseDatasetORM, ContributedValuesORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.db_socket.helpers import (
    get_general,
    get_query_proj_options,
)
from qcportal.exceptions import AlreadyExistsError, MissingDataError
from qcportal.metadata_models import InsertMetadata, DeleteMetadata, UpdateMetadata
from qcportal.records import RecordStatusEnum, PriorityEnum

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.datasets.models import DatasetModifyMetadata
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.db_socket.base_orm import BaseORM
    from typing import Dict, Any, Optional, Sequence, Iterable, Tuple, List


class BaseDatasetSocket:
    """
    Base class for all dataset sockets

    Since all datasets have very similar structure, much of the work can be handled
    in this base class, as long as this class knows the various ORM types.
    """

    # Must be overridden by the derived classes
    dataset_orm = None
    specification_orm = None
    entry_orm = None
    record_item_orm = None
    record_orm = None

    def __init__(
        self,
        root_socket: SQLAlchemySocket,
    ):
        self.root_socket = root_socket

        # Make sure these were set by the derived classes
        assert self.dataset_orm is not None
        assert self.specification_orm is not None
        assert self.entry_orm is not None
        assert self.record_item_orm is not None

        # Use the identity from the ORM object. This keeps everything consistent
        self.dataset_type = self.dataset_orm.__mapper_args__["polymorphic_identity"]

    def _add_specification(self, session, specification) -> Tuple[InsertMetadata, Optional[int]]:
        raise NotImplementedError("_add_specification must be overridden by the derived class")

    def _create_entries(self, session, dataset_id, new_entries) -> Sequence[BaseORM]:
        raise NotImplementedError("_create_entries must be overridden by the derived class")

    def get_records_select(self):
        """
        Create a statement that selects the dataset id, entry_name, specification_name, and record_id
        from a dataset (with appropriate labels).
        """

        # Use the common stuff here, but this function can be overridden

        stmt = select(
            self.record_item_orm.dataset_id.label("dataset_id"),
            self.record_item_orm.entry_name.label("entry_name"),
            self.record_item_orm.specification_name.label("specification_name"),
            self.record_item_orm.record_id.label("record_id"),
        )

        return [stmt]

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[Any],
        specification_orm: Iterable[Any],
        existing_records: Iterable[Tuple[str, str]],
        tag: str,
        priority: PriorityEnum,
    ):
        raise NotImplementedError("_submit must be overridden by the derived class")

    def get_tag_priority(
        self, dataset_id: int, tag: Optional[str], priority: Optional[str], *, session: Optional[Session] = None
    ) -> Tuple[str, PriorityEnum]:
        """
        Obtains the tag and priority a new record should be computed with

        This takes into account the tag and priority passed in, as well as the dataset's default
        and priority.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        tag
            Specified tag to use. If None, then the default tag will be used instead
        priority
            Specified priority to use. If None, then the default priority will be used instead
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            The tag and priority to use
        """

        if tag is None or priority is None:
            default_tag, default_priority = self.get_default_tag_priority(dataset_id, session=session)
            if tag is None:
                tag = default_tag
            else:
                tag = tag.lower()
            if priority is None:
                priority = default_priority
        return tag, priority

    def get_default_tag_priority(
        self, dataset_id: int, *, session: Optional[Session] = None
    ) -> Tuple[str, PriorityEnum]:
        """
        Obtain a dataset's default tag and priority

        Parameters
        ----------
        dataset_id
            ID of a dataset
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        stmt = select(BaseDatasetORM.default_tag, BaseDatasetORM.default_priority)
        stmt = stmt.where(BaseDatasetORM.id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            r = session.execute(stmt).one_or_none()
            if r is None:
                raise MissingDataError(f"Cannot get default tag & priority - dataset id {dataset_id} does not exist")
            return tuple(r)

    def get(
        self,
        dataset_id: int,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Obtain a dataset from the database

        Parameters
        ----------
        dataset_id
            ID of a dataset
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, None will be returned if the dataset does not exist. Otherwise,
           an exception is raised.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Dataset information as a dictionary
        """

        with self.root_socket.optional_session(session) as session:
            return get_general(
                session, self.dataset_orm, self.dataset_orm.id, (dataset_id,), include, exclude, missing_ok
            )[0]

    def status(self, dataset_id: int, *, session: Optional[Session] = None) -> Dict[str, Dict[RecordStatusEnum, int]]:
        """
        Compute the status of a dataset

        Parameters
        ----------
        dataset_id
            ID of a dataset
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Dictionary with specifications as the keys, and record status/counts as values.
        """

        stmt = select(self.record_item_orm.specification_name, BaseRecordORM.status, func.count(BaseRecordORM.id))
        stmt = stmt.join(self.record_item_orm, BaseRecordORM.id == self.record_item_orm.record_id)
        stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)
        stmt = stmt.group_by(self.record_item_orm.specification_name, BaseRecordORM.status)

        with self.root_socket.optional_session(session, True) as session:
            stats = session.execute(stmt).all()

        ret: Dict[str, Dict[RecordStatusEnum, int]] = {}
        for s in stats:
            ret.setdefault(s[0], dict())
            ret[s[0]][s[1]] = s[2]
        return ret

    def detailed_status(
        self, dataset_id: int, *, session: Optional[Session] = None
    ) -> List[Tuple[str, str, RecordStatusEnum]]:
        """
        Compute the detailed status of a dataset

        This breaks down the status into entry/specification pairs.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            List of tuple (entry name, specification name, status)
        """

        stmt = select(self.record_item_orm.entry_name, self.record_item_orm.specification_name, BaseRecordORM.status)
        stmt = stmt.join(self.record_item_orm, BaseRecordORM.id == self.record_item_orm.record_id)
        stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            stats = session.execute(stmt).all()
            return [tuple(x) for x in stats]

    def add(
        self,
        name: str,
        description: str,
        tagline: str,
        tags: List[str],
        group: str,
        provenance: Dict[str, Any],
        visibility: bool,
        default_tag: str,
        default_priority: PriorityEnum,
        metadata: Dict[str, Any],
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Create a new dataset in the database

        If a dataset already exists with the same name and type, an exception is raised

        Returns
        -------
        :
            ID of the new dataset
        """

        ds_orm = self.dataset_orm(
            dataset_type=self.dataset_type,
            name=name,
            lname=name.lower(),
            tagline=tagline,
            description=description,
            tags=tags,
            group=group,
            provenance=provenance,
            visibility=visibility,
            default_tag=default_tag.lower(),
            default_priority=default_priority,
            meta=metadata,
            extras={},
        )

        with self.root_socket.optional_session(session) as session:
            stmt = select(self.dataset_orm.id)
            stmt = stmt.where(self.dataset_orm.lname == name.lower())
            stmt = stmt.where(self.dataset_orm.dataset_type == self.dataset_type)
            existing = session.execute(stmt).scalar_one_or_none()

            if existing is not None:
                raise AlreadyExistsError(f"Dataset with type='{self.dataset_type}' and name='{name}' already exists")

            session.add(ds_orm)
            session.commit()
            return ds_orm.id

    def update_metadata(
        self, dataset_id: int, new_metadata: DatasetModifyMetadata, *, session: Optional[Session] = None
    ):
        """
        Updates the metadata of the dataset

        This will overwrite the existing metadata. An exception is raised on any error

        Parameters
        ----------
        dataset_id
            ID of a dataset
        new_metadata
            New metadata to store
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            stmt = select(self.dataset_orm).where(self.dataset_orm.id == dataset_id)
            stmt = stmt.options(lazyload("*")).with_for_update()
            ds = session.execute(stmt).scalar_one_or_none()

            if ds is None:
                raise MissingDataError(f"Could not find dataset with type={self.dataset_type} and {dataset_id}")

            if ds.name != new_metadata.name:
                stmt2 = select(self.dataset_orm.id)
                stmt2 = stmt2.where(self.dataset_orm.dataset_type == self.dataset_type)
                stmt2 = stmt2.where(self.dataset_orm.lname == new_metadata.name.lower())
                existing = session.execute(stmt2).scalar_one_or_none()

                if existing:
                    raise AlreadyExistsError(f"{self.dataset_type} dataset named '{new_metadata.name}' already exists")

                ds.name = new_metadata.name
                ds.lname = new_metadata.name.lower()

            ds.description = new_metadata.description
            ds.tagline = new_metadata.tagline
            ds.tags = new_metadata.tags
            ds.group = new_metadata.group
            ds.visibility = new_metadata.visibility
            ds.provenance = new_metadata.provenance

            # "metadata" is reserved. The field is 'metadata' but accessed via 'meta'
            ds.meta = new_metadata.metadata

            ds.default_tag = new_metadata.default_tag
            ds.default_priority = new_metadata.default_priority

    def add_specifications(
        self,
        dataset_id: int,
        new_specifications: Sequence[Any],  # we don't know the type here
        *,
        session: Optional[Session] = None,
    ) -> InsertMetadata:
        """
        Adds specifications to a dataset in the database

        Parameters
        ----------
        dataset_id
            ID of a dataset
        new_specifications
            Specifications to add
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion
        """

        existing_idx: List[int] = []
        inserted_idx: List[int] = []
        errors: List[Tuple[int, str]] = []

        with self.root_socket.optional_session(session) as session:
            stmt = select(self.specification_orm.name)
            stmt = stmt.where(self.specification_orm.dataset_id == dataset_id)

            existing_specs = session.execute(stmt).scalars().all()

            for idx, ds_spec in enumerate(new_specifications):
                if ds_spec.name in existing_specs:
                    existing_idx.append(idx)
                    continue

                # call the derived class function for adding a specification
                meta, spec_id = self._add_specification(session, ds_spec.specification)

                if not meta.success:
                    err_str = f"Unable to add {self.dataset_type} specification: " + meta.error_string
                    errors.append((idx, err_str))
                    continue

                ds_spec_orm = self.specification_orm(
                    dataset_id=dataset_id, name=ds_spec.name, description=ds_spec.description, specification_id=spec_id
                )

                session.add(ds_spec_orm)
                inserted_idx.append(idx)

        return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)

    def fetch_specifications(
        self,
        dataset_id: int,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Get specifications for a dataset from the database

        It's expected there aren't too many specifications, so this always fetches them all.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Specifications as a dictionary, with the key being the name and the value being
            the specification (as a dictionary)
        """

        stmt = select(self.specification_orm)
        stmt = stmt.where(self.specification_orm.dataset_id == dataset_id)
        stmt = stmt.options(joinedload(self.specification_orm.specification))

        if include or exclude:
            query_opts = get_query_proj_options(self.entry_orm, include, exclude)
            stmt = stmt.options(*query_opts)

        with self.root_socket.optional_session(session, True) as session:
            entries = session.execute(stmt).scalars().all()

        return {x.name: x.model_dict() for x in entries}

    def delete_specifications(
        self,
        dataset_id: int,
        specification_names: Iterable[str],
        delete_records: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> DeleteMetadata:
        """
        Deletes specifications from a dataset

        Specifications which do not exist for the dataset will be silently ignored

        Parameters
        ----------
        dataset_id
            ID of a dataset
        specification_names
            Names of the specifications to delete
        delete_records
            If True, also (hard) delete all records associated with this specification
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the deletion. If delete_records is True, then n_children_deleted will
            contain the number of records deleted.
        """

        with self.root_socket.optional_session(session) as session:
            if delete_records:
                # Store all record ids for later deletion
                stmt = select(self.record_item_orm.record_id)
                stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)
                stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))
                record_ids = session.execute(stmt).scalars().all()

            # Deleting the specification will cascade to the dataset->record association table
            stmt = delete(self.specification_orm)
            stmt = stmt.where(self.specification_orm.dataset_id == dataset_id)
            stmt = stmt.where(self.specification_orm.name.in_(specification_names))
            stmt = stmt.returning(self.specification_orm.name)
            deleted_entries = session.execute(stmt).scalars().all()
            session.flush()

            n_children_deleted = 0
            if delete_records:
                rec_meta = self.root_socket.records.delete(
                    record_ids, soft_delete=False, delete_children=True, session=session
                )
                n_children_deleted = rec_meta.n_deleted

            deleted_idx = [idx for idx, name in enumerate(specification_names) if name in deleted_entries]
            errors = [
                (idx, "specification does not exist")
                for idx, name in enumerate(specification_names)
                if name not in deleted_entries
            ]
            return DeleteMetadata(deleted_idx=deleted_idx, errors=errors, n_children_deleted=n_children_deleted)

    def rename_specifications(
        self, dataset_id: int, specification_name_map: Dict[str, str], *, session: Optional[Session] = None
    ):
        """
        Renames specifications

        The specification_name_map maps the old name to the new name (ie, `specification_name_map[old_name] = new_name`).
        If any of the new names exist, an exception is raised and no renaming takes place.
        If a specification does not exist under the old name, then that renaming is silently ignored.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        specification_name_map
            Mapping of old name to new name
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        stmt = select(self.specification_orm)
        stmt = stmt.where(self.specification_orm.dataset_id == dataset_id)
        stmt = stmt.where(self.specification_orm.name.in_(specification_name_map.keys()))
        stmt = stmt.options(load_only(self.specification_orm.name))

        # See if any of the new names already exist
        exist_stmt = select(self.specification_orm.name)
        exist_stmt = exist_stmt.where(self.specification_orm.dataset_id == dataset_id)
        exist_stmt = exist_stmt.where(self.specification_orm.name.in_(specification_name_map.values()))

        with self.root_socket.optional_session(session) as session:
            existing = session.execute(exist_stmt).scalars().all()
            if existing:
                raise AlreadyExistsError(
                    f"Cannot rename specification to {existing[0]} - specification with that name already exists"
                )

            specs = session.execute(stmt).scalars().all()

            for spec in specs:
                spec.name = specification_name_map[spec.name]

    def add_entries(
        self, dataset_id: int, new_entries: Sequence[Any], *, session: Optional[Session] = None
    ) -> InsertMetadata:
        """
        Adds entries to a dataset in the database

        Parameters
        ----------
        dataset_id
            ID of a dataset
        new_entries
            Entries to add
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion
        """

        with self.root_socket.optional_session(session) as session:
            # Create orm for all entries (in derived class)
            entry_orm = self._create_entries(session, dataset_id, new_entries)

            # Get all existing entries first
            stmt = select(self.entry_orm.name)
            stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)
            existing_entries = session.execute(stmt).scalars().all()

            inserted_idx: List[int] = []
            existing_idx: List[int] = []

            for idx, entry in enumerate(entry_orm):
                # Only add if the entry does not exist
                if entry.name in existing_entries:
                    existing_idx.append(idx)
                else:
                    session.add(entry)
                    inserted_idx.append(idx)

        return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)

    def fetch_entry_names(
        self,
        dataset_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[str]:
        """
        Obtain all entry names for a dataset

        Parameters
        ----------
        dataset_id
            ID of a dataset
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            All entry names as a list
        """
        stmt = select(self.entry_orm.name)
        stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            ret = session.execute(stmt).scalars().all()
            return list(ret)

    def fetch_entries(
        self,
        dataset_id: int,
        entry_names: Optional[Sequence[str]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Obtain full entries for a dataset from the database

        Parameters
        ----------
        dataset_id
            ID of a dataset
        entry_names
            Names of the entries to fetch. If None, fetch all
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, None will be returned if the dataset does not exist. Otherwise,
           an exception is raised.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Entries as a dictionary, with the key being the name and the value being
            the entry (as a dictionary)
        """

        stmt = select(self.entry_orm)
        stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)

        if entry_names is not None:
            stmt = stmt.where(self.entry_orm.name.in_(entry_names))

        if include or exclude:
            query_opts = get_query_proj_options(self.entry_orm, include, exclude)
            stmt = stmt.options(*query_opts)

        with self.root_socket.optional_session(session, True) as session:
            entries = session.execute(stmt).scalars().all()

        if entry_names is not None and missing_ok is False:
            found_entries = {x.name for x in entries}
            missing_entries = set(entry_names) - found_entries
            if missing_entries:
                s = "\n".join(missing_entries)
                raise MissingDataError(f"Missing {len(missing_entries)} entries: {s}")

        return {x.name: x.model_dict() for x in entries}

    def delete_entries(
        self,
        dataset_id: int,
        entry_names: Sequence[str],
        delete_records: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> DeleteMetadata:
        """
        Deletes entries from a dataset

        Entries which do not exist for the dataset will be silently ignored

        Parameters
        ----------
        dataset_id
            ID of a dataset
        entry_names
            Names of the entries to delete
        delete_records
            If True, also (hard) delete all records associated with this specification
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the deletion. If delete_records is True, then n_children_deleted will
            contain the number of records deleted.
        """

        with self.root_socket.optional_session(session) as session:
            if delete_records:
                # Store all record ids for later deletion
                stmt = select(self.record_item_orm.record_id)
                stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)
                stmt = stmt.where(self.record_item_orm.entry_name.in_(entry_names))
                record_ids = session.execute(stmt).scalars().all()

            # Delete the entries will cascade to the dataset->record association table
            stmt = delete(self.entry_orm)
            stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)
            stmt = stmt.where(self.entry_orm.name.in_(entry_names))
            stmt = stmt.returning(self.entry_orm.name)
            deleted_entries = session.execute(stmt).scalars().all()
            session.flush()

            n_children_deleted = 0
            if delete_records:
                rec_meta = self.root_socket.records.delete(
                    record_ids, soft_delete=False, delete_children=True, session=session
                )
                n_children_deleted = rec_meta.n_deleted

            deleted_idx = [idx for idx, name in enumerate(entry_names) if name in deleted_entries]
            errors = [
                (idx, "entry does not exist") for idx, name in enumerate(entry_names) if name not in deleted_entries
            ]
            return DeleteMetadata(deleted_idx=deleted_idx, errors=errors, n_children_deleted=n_children_deleted)

    def rename_entries(self, dataset_id: int, entry_name_map: Dict[str, str], *, session: Optional[Session] = None):
        """
        Renames entries for a dataset

        The entry_name_map maps the old name to the new name (ie, `entry_name_map[old_name] = new_name`).
        If any of the new names exist, an exception is raised and no renaming takes place.
        If an entry does not exist under the old name, then that renaming is silently ignored.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        entry_name_map
            Mapping of old name to new name
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        stmt = select(self.entry_orm)
        stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)
        stmt = stmt.where(self.entry_orm.name.in_(entry_name_map.keys()))
        stmt = stmt.options(load_only(self.entry_orm.name))

        # See if any of the new names already exist
        exist_stmt = select(self.entry_orm.name)
        exist_stmt = exist_stmt.where(self.entry_orm.dataset_id == dataset_id)
        exist_stmt = exist_stmt.where(self.entry_orm.name.in_(entry_name_map.values()))

        with self.root_socket.optional_session(session) as session:
            existing = session.execute(exist_stmt).scalars().all()
            if existing:
                raise AlreadyExistsError(f"Cannot rename entry to {existing[0]} - entry with that name already exists")

            # Now do the renaming
            entries = session.execute(stmt).scalars().all()

            for entry in entries:
                entry.name = entry_name_map[entry.name]

    def fetch_records(
        self,
        dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
        specification_names: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """
        Obtain records for a dataset from the database

        The returned list is in an indeterminant order

        Parameters
        ----------
        dataset_id
            ID of a dataset
        entry_names
            Fetch records belonging to these entries. If None, fetch records belonging to any entry.
        specification_names
            Fetch records belonging to these specifications. If None, fetch records belonging to any specification.
        status
            Fetch records whose status is in the given list (or other iterable) of statuses
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Entries matching the given criteria
        """

        stmt = select(self.record_item_orm)
        stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

        if entry_names is not None:
            stmt = stmt.where(self.record_item_orm.entry_name.in_(entry_names))
        if specification_names is not None:
            stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))
        if status:
            stmt = stmt.join(self.record_item_orm.record)
            stmt = stmt.where(self.record_orm.status.in_(status))

        query_opts = []
        if include or exclude:
            query_opts = get_query_proj_options(self.record_orm, include, exclude)

        stmt = stmt.options(selectinload(self.record_item_orm.record).options(*query_opts))

        with self.root_socket.optional_session(session, True) as session:
            records = session.execute(stmt).scalars().all()

        return [x.model_dict() for x in records]

    def remove_records(
        self,
        dataset_id: int,
        entry_names: Sequence[str],
        specification_names: Sequence[str],
        delete_records: bool,
        *,
        session: Optional[Session] = None,
    ):
        """
        Removes a record from this dataset, optionally deleting the record

        This does not delete entries or specifications.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        entry_names
            Delete records belonging to these entries
        specification_names
            Delete records belonging to these specifications
        delete_records
            If True, actually delete the records (rather than simply un-associating them from this dataset)
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            if delete_records:
                # Store all record ids for later deletion
                stmt = select(self.record_item_orm.record_id)
                stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)
                stmt = stmt.where(self.record_item_orm.entry_name.in_(entry_names))
                stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))
                record_ids = session.execute(stmt).scalars().all()

            stmt = delete(self.record_item_orm)
            stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)
            stmt = stmt.where(self.record_item_orm.entry_name.in_(entry_names))
            stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))
            session.execute(stmt)

            if delete_records:
                self.root_socket.records.delete(record_ids, soft_delete=False, delete_children=True, session=session)

    def delete_dataset(
        self,
        dataset_id: int,
        delete_records: bool,
        *,
        session: Optional[Session] = None,
    ):
        """
        Deletes an entire dataset from the database

        Parameters
        ----------
        dataset_id
            ID of a dataset
        delete_records
            If true, delete all the individual records as well
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            if delete_records:
                # Store all record ids for later deletion
                stmt = select(self.record_item_orm.record_id)
                stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)
                record_ids = session.execute(stmt).scalars().all()

            stmt = delete(self.dataset_orm)
            stmt = stmt.where(self.dataset_orm.id == dataset_id)
            session.execute(stmt)

            if delete_records:
                self.root_socket.records.delete(record_ids, soft_delete=False, delete_children=True, session=session)

    def submit(
        self,
        dataset_id: int,
        entry_names: Optional[Iterable[str]],
        specification_names: Optional[Iterable[str]],
        tag: Optional[str],
        priority: Optional[PriorityEnum],
        *,
        session: Optional[Session] = None,
    ):
        """
        Submit computations for this dataset

        If any specification or entry name does not exist, an exception is raised and
        no submission takes place

        Parameters
        ----------
        dataset_id
            ID of a dataset
        entry_names
            Submit records belonging to these entries. If None, submit for all entries
        specification_names
            Submit records belonging to these specifications. If None, submit for all specifications
        tag
            Computational tag for new records. If None, use the dataset's default. Existing records
            will not be modified.
        priority
            Priority for new records. If None, use the dataset's default. Existing records
            will not be modified.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            tag, priority = self.get_tag_priority(dataset_id, tag, priority, session=session)

            ################################
            # Get specification details
            ################################
            stmt = select(self.specification_orm)

            # We want the actual optimization specification as well
            stmt = stmt.join(self.specification_orm.specification)
            stmt = stmt.where(self.specification_orm.dataset_id == dataset_id)
            if specification_names is not None:
                stmt = stmt.where(self.specification_orm.name.in_(specification_names))

            ds_specs = session.execute(stmt).scalars().all()

            # Check to make sure we found all the specifications
            if specification_names is not None:
                found_specs = {x.name for x in ds_specs}
                missing_specs = set(specification_names) - found_specs
                if missing_specs:
                    raise MissingDataError(f"Could not find all specifications. Missing: {missing_specs}")

            ################################
            # Get entry details
            ################################
            stmt = select(self.entry_orm)
            stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)

            if entry_names is not None:
                stmt = stmt.where(self.entry_orm.name.in_(entry_names))

            entries = session.execute(stmt).scalars().all()

            # Check to make sure we found all the entries
            if entry_names is not None:
                found_entries = {x.name for x in entries}
                missing_entries = set(entry_names) - found_entries
                if missing_entries:
                    raise MissingDataError(f"Could not find all entries. Missing: {missing_entries}")

            # Find which records/record_items already exist
            stmt = select(self.record_item_orm)
            stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

            if entry_names is not None:
                stmt = stmt.where(self.record_item_orm.entry_name.in_(entry_names))
            if specification_names is not None:
                stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))

            existing_record_orm = session.execute(stmt).scalars().all()
            existing_records = [(x.entry_name, x.specification_name) for x in existing_record_orm]

            return self._submit(session, dataset_id, entries, ds_specs, existing_records, tag, priority)

    #######################
    # Record modification
    #######################
    def _lookup_record_ids(
        self,
        session: Session,
        dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
        specification_names: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        for_update: bool = False,
    ) -> List[int]:
        """
        Lookup the record IDs for a dataset

        The returned IDs are in an indeterminant order

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        dataset_id
            ID of a dataset
        entry_names
            Find records belonging to these entries. If None, look up all entries
        specification_names
            Find records belonging to these specifications. If None, look up all specifications
        status
            Look up records whose status is in the given list (or other iterable) of statuses
        for_update
            If True, select the records with row-level locking (postgres FOR UPDATE)

        Returns
        -------
        :
            List of record IDs
        """
        stmt = select(self.record_item_orm.record_id)
        stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

        if entry_names is not None:
            stmt = stmt.where(self.record_item_orm.entry_name.in_(entry_names))
        if specification_names is not None:
            stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))
        if status is not None:
            stmt = stmt.join(BaseRecordORM, BaseRecordORM.id == self.record_item_orm.record_id)
            stmt = stmt.where(BaseRecordORM.status.in_(status))

        # Locks the record items, not the actual record
        if for_update:
            stmt = stmt.with_for_update()

        r = session.execute(stmt).scalars().all()
        return r

    def modify_records(
        self,
        dataset_id: int,
        username: Optional[str],
        entry_names: Optional[Iterable[str]] = None,
        specification_names: Optional[List[str]] = None,
        status: Optional[RecordStatusEnum] = None,
        priority: Optional[PriorityEnum] = None,
        tag: Optional[str] = None,
        comment: Optional[str] = None,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Modify records belonging to a dataset

        Parameters
        ----------
        dataset_id
            ID of a dataset
        username
            Username of the user modifying these records
        entry_names
            Modify records belonging to these entries. If None, modify records belonging to any entry.
        specification_names
            Modify records belonging to these specifications. If None, modify records belonging to any specification.
        username
            Username of the user modifying the records
        status
            New status for the records. Only certain status transitions will be allowed.
        priority
            New priority for these records
        tag
            New tag for these records
        comment
            Adds a new comment to these records
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        with self.root_socket.optional_session(session) as session:

            record_ids = self._lookup_record_ids(
                session,
                dataset_id,
                entry_names,
                specification_names,
                for_update=True,
            )

            return self.root_socket.records.modify_generic(
                record_ids, username, status=status, priority=priority, tag=tag, comment=comment, session=session
            )

    def revert_records(
        self,
        dataset_id: int,
        revert_status: RecordStatusEnum,
        entry_names: Optional[Iterable[str]] = None,
        specification_names: Optional[List[str]] = None,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Reverts the status of dataset records to their previous status

        Parameters
        ----------
        dataset_id
            ID of a dataset
        revert_status
            Revert records of this status. For example, if `revert_status` is `deleted`, then it will be undeleted
        entry_names
            Modify records belonging to these entries. If None, modify records belonging to any entry.
        specification_names
            Modify records belonging to these specifications. If None, modify records belonging to any specification.

        Returns
        -------
        :
            Metadata about the modification/update

        """

        with self.root_socket.optional_session(session) as session:
            record_ids = self._lookup_record_ids(session, dataset_id, entry_names, specification_names, for_update=True)

            return self.root_socket.records.revert_generic(record_ids, revert_status)


class DatasetSocket:
    """
    Root socket for all dataset sockets
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        from .singlepoint.sockets import SinglepointDatasetSocket
        from .optimization.sockets import OptimizationDatasetSocket
        from .torsiondrive.sockets import TorsiondriveDatasetSocket
        from .gridoptimization.sockets import GridoptimizationDatasetSocket
        from .manybody.sockets import ManybodyDatasetSocket
        from .reaction.sockets import ReactionDatasetSocket

        self.singlepoint = SinglepointDatasetSocket(root_socket)
        self.optimization = OptimizationDatasetSocket(root_socket)
        self.torsiondrive = TorsiondriveDatasetSocket(root_socket)
        self.gridoptimization = GridoptimizationDatasetSocket(root_socket)
        self.manybody = ManybodyDatasetSocket(root_socket)
        self.reaction = ReactionDatasetSocket(root_socket)

        self._handler_map: Dict[str, BaseDatasetSocket] = {
            self.singlepoint.dataset_type: self.singlepoint,
            self.optimization.dataset_type: self.optimization,
            self.torsiondrive.dataset_type: self.torsiondrive,
            self.gridoptimization.dataset_type: self.gridoptimization,
            self.manybody.dataset_type: self.manybody,
            self.reaction.dataset_type: self.reaction,
        }

        # Get the SQL 'select' statements from the handlers
        selects = []
        for h in self._handler_map.values():
            sel = h.get_records_select()
            selects.extend(sel)

        # Union them into a CTE
        self._record_cte = union(*selects).cte()

    def get_socket(self, dataset_type: str) -> BaseDatasetSocket:
        """
        Get the socket for a specific kind of dataset type
        """

        handler = self._handler_map.get(dataset_type, None)
        if handler is None:
            raise MissingDataError(f"Cannot find handler for type {dataset_type}")
        return handler

    def get(
        self,
        dataset_id: int,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Obtain a dataset with the specified ID

        Parameters
        ----------
        dataset_id
            ID of a dataset
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, None will be returned if the dataset does not exist. Otherwise,
           an exception is raised.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Dataset information as a dictionary
        """

        # If all columns are included, then we can load
        # the data from derived classes as well.
        if (include is None or "*" in include) and not exclude:
            wp = with_polymorphic(BaseDatasetORM, "*")
        else:
            wp = BaseRecordORM

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, wp, wp.id, [dataset_id], include, exclude, missing_ok)[0]

    def lookup_type(self, dataset_id: int, *, session: Optional[Session] = None) -> str:
        """
        Look up the type of dataset given its ID
        """

        stmt = select(BaseDatasetORM.dataset_type)
        stmt = stmt.where(BaseDatasetORM.id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            ds_type = session.execute(stmt).scalar_one_or_none()

            if ds_type is None:
                raise MissingDataError(f"Could not find dataset with id {dataset_id}")

            return ds_type

    def lookup_id(
        self, dataset_type: str, dataset_name: str, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[int]:
        """
        Look up a dataset ID given its dataset type and name
        """

        stmt = select(BaseDatasetORM.id)
        stmt = stmt.where(BaseDatasetORM.lname == dataset_name.lower())
        stmt = stmt.where(BaseDatasetORM.dataset_type == dataset_type.lower())

        with self.root_socket.optional_session(session, True) as session:
            ds_id = session.execute(stmt).scalar_one_or_none()

            if missing_ok is False and ds_id is None:
                raise MissingDataError(f"Could not find {dataset_type} dataset with name '{dataset_name}'")
            return ds_id

    def list(self, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get a list of datasets in the database
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(BaseDatasetORM.id, BaseDatasetORM.dataset_type, BaseDatasetORM.name)
            r = session.execute(stmt).all()

            return [{"id": x[0], "dataset_type": x[1], "dataset_name": x[2]} for x in r]

    def query_dataset_records(
        self,
        record_id: Iterable[int],
        dataset_type: Optional[Iterable[str]] = None,
        *,
        session: Optional[Session] = None,
    ):
        """
        Query which datasets the specified records belong to

        This returns a dictionary containing the dataset id, type, and name, as well as
        the entry and specification names that the record belongs to
        """

        stmt = select(
            self._record_cte.c.record_id,
            BaseDatasetORM.id,
            BaseDatasetORM.dataset_type,
            BaseDatasetORM.name,
            self._record_cte.c.entry_name,
            self._record_cte.c.specification_name,
        )
        stmt = stmt.join(self._record_cte, BaseDatasetORM.id == self._record_cte.c.dataset_id)
        stmt = stmt.where(self._record_cte.c.record_id.in_(record_id))

        if dataset_type is not None:
            stmt = stmt.where(BaseDatasetORM.dataset_type == dataset_type)

        with self.root_socket.optional_session(session, True) as session:
            ret = session.execute(stmt).all()
            return [
                {
                    "record_id": x[0],
                    "dataset_id": x[1],
                    "dataset_type": x[2],
                    "dataset_name": x[3],
                    "entry_name": x[4],
                    "specification_name": x[5],
                }
                for x in ret
            ]

    def get_contributed_values(self, dataset_id: int, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get the contributed values for a dataset
        """

        stmt = select(ContributedValuesORM)
        stmt = stmt.where(ContributedValuesORM.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            cv = session.execute(stmt).scalars().all()
            return [x.model_dict() for x in cv]
