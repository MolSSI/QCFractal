from __future__ import annotations

import logging
import os
import tempfile
from typing import TYPE_CHECKING

from sqlalchemy import select, delete, func, union, text, and_, literal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only, lazyload, joinedload, noload, with_polymorphic
from sqlalchemy.orm.attributes import flag_modified

from qcfractal.components.dataset_db_models import (
    BaseDatasetORM,
    ContributedValuesORM,
    DatasetAttachmentORM,
    DatasetInternalJobORM,
)
from qcfractal.components.dataset_processing import create_view_file
from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket.helpers import get_count
from qcfractal.db_socket.helpers import (
    get_general,
    get_query_proj_options,
)
from qcportal.dataset_models import BaseDataset, DatasetAttachmentType
from qcportal.exceptions import AlreadyExistsError, MissingDataError, UserReportableError
from qcportal.internal_jobs import InternalJobStatusEnum
from qcportal.metadata_models import InsertMetadata, DeleteMetadata, UpdateMetadata, InsertCountsMetadata
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.serialization import encode_to_json
from qcportal.utils import chunk_iterable, now_at_utc

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.dataset_models import DatasetModifyMetadata
    from qcfractal.components.internal_jobs.status import JobProgress
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.db_socket.base_orm import BaseORM
    from typing import Dict, Any, Optional, Sequence, Iterable, Tuple, List, Union
    from typing import Iterable, List, Dict, Any
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


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

        self._logger = logging.getLogger(__name__)

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

    def get_record_counts_select(self):
        """
        Create a statement that selects the dataset id, entry_name, specification_name, and record_id
        from a dataset (with appropriate labels).
        """

        # Use the common stuff here, but this function can be overridden

        stmt = select(self.record_item_orm.dataset_id.label("dataset_id"), func.count().label("record_count")).group_by(
            self.record_item_orm.dataset_id
        )

        return [stmt]

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[Any],
        specification_orm: Iterable[Any],
        existing_records: Iterable[Tuple[str, str]],
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
    ) -> InsertCountsMetadata:
        raise NotImplementedError("_submit must be overridden by the derived class")

    def get_submit_info(
        self,
        dataset_id: int,
        compute_tag: Optional[str],
        compute_priority: Optional[str],
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        *,
        session: Optional[Session] = None,
    ) -> Tuple[str, PriorityEnum, int, int]:
        """
        Obtains the tag, priority, and group a new record should be computed with

        This takes into account the fields passed in, as well as the dataset's default
        and priority.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        compute_tag
            Specified tag to use. If None, then the default tag will be used instead
        compute_priority
            Specified priority to use. If None, then the default priority will be used instead
        owner_group
            Specified owner_group to use. If None, then the datasets owner_group will be used instead
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            The tag, priority, owner id, and group id
        """

        with self.root_socket.optional_session(session, True) as session:
            default_tag, default_priority, default_group_id = self.get_submit_defaults(dataset_id, session=session)

            if compute_tag is None:
                compute_tag = default_tag
            else:
                compute_tag = compute_tag.lower()

            if compute_priority is None:
                compute_priority = default_priority

            if owner_group is None:
                owner_group = default_group_id

            user_id, group_id = self.root_socket.users.get_owner_ids(owner_user, owner_group, session=session)

        return compute_tag, compute_priority, user_id, group_id

    def get_submit_defaults(
        self, dataset_id: int, *, session: Optional[Session] = None
    ) -> Tuple[str, PriorityEnum, int]:
        """
        Obtain a dataset's default submission information

        Parameters
        ----------
        dataset_id
            ID of a dataset
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        stmt = select(
            BaseDatasetORM.default_compute_tag, BaseDatasetORM.default_compute_priority, BaseDatasetORM.owner_group_id
        )
        stmt = stmt.where(BaseDatasetORM.id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            r = session.execute(stmt).one_or_none()
            if r is None:
                raise MissingDataError(f"Cannot get default submission info - dataset id {dataset_id} does not exist")
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

    def get_record_count(self, dataset_id: int, *, session: Optional[Session] = None) -> int:
        """
        Retrieve the number of records stored by the dataset

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
            Number of records that this dataset contains
        """

        stmt = select(func.count()).select_from(self.record_item_orm)
        stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            count = session.execute(stmt).scalar_one_or_none()

            if count is None:
                raise MissingDataError(f"Could not find dataset with type={self.dataset_type} and id={dataset_id}")

            return count

    def get_computed_properties(self, dataset_id: int, *, session: Optional[Session] = None) -> Dict[str, List[str]]:
        """
        Retrieve the typical properties computed by each specification

        These should be the properties found in the properties dictionary.

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
            Dictionary with keys representing the specification name, and a list of
            properties typically computed by that specification (as a list of strings).
        """

        # Easier to do raw SQL I think
        stmt = text(
            f"""
             WITH cte AS (
                 SELECT DISTINCT ON (dr.specification_name) dr.specification_name, br.id
                 FROM base_record br
                 INNER JOIN {self.record_item_orm.__tablename__} dr ON dr.record_id = br.id
                 WHERE dr.dataset_id = :dataset_id and br.status = 'complete'
             )
             SELECT cte.specification_name, ARRAY(SELECT jsonb_object_keys(br.properties))
             FROM cte INNER JOIN base_record br ON br.id = cte.id;
        """
        )

        stmt = stmt.bindparams(dataset_id=dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            rows = session.execute(stmt).all()
            return {r[0]: r[1] for r in rows}

    def add(
        self,
        name: str,
        description: str,
        tagline: str,
        tags: List[str],
        provenance: Dict[str, Any],
        default_compute_tag: str,
        default_compute_priority: PriorityEnum,
        extras: Dict[str, Any],
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        existing_ok: bool,
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Create a new dataset in the database

        If a dataset already exists with the same name and type, an exception is raised if existing_ok is False

        Returns
        -------
        :
            ID of the new dataset
        """

        ds_orm = self.dataset_orm(
            dataset_type=self.dataset_type,
            name=name,
            tagline=tagline,
            description=description,
            tags=tags,
            provenance=provenance,
            default_compute_tag=default_compute_tag.lower(),
            default_compute_priority=default_compute_priority,
            extras=extras,
        )

        with self.root_socket.optional_session(session) as session:
            user_id, group_id = self.root_socket.users.get_owner_ids(owner_user, owner_group)
            self.root_socket.users.assert_group_member(user_id, group_id, session=session)

            stmt = select(self.dataset_orm.id)
            stmt = stmt.where(self.dataset_orm.lname == name.lower())
            stmt = stmt.where(self.dataset_orm.dataset_type == self.dataset_type)
            existing_id = session.execute(stmt).scalar_one_or_none()

            if existing_id is not None:
                if existing_ok:
                    return existing_id
                else:
                    raise AlreadyExistsError(
                        f"Dataset with type='{self.dataset_type}' and name='{name}' already exists"
                    )

            ds_orm.owner_user_id = user_id
            ds_orm.owner_group_id = group_id

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
                raise MissingDataError(f"Could not find dataset with type={self.dataset_type} and id={dataset_id}")

            if ds.name != new_metadata.name:
                # If only change in case, no need to check if it already exists
                if ds.name.lower() != new_metadata.name.lower():
                    stmt2 = select(self.dataset_orm.id)
                    stmt2 = stmt2.where(self.dataset_orm.dataset_type == self.dataset_type)
                    stmt2 = stmt2.where(self.dataset_orm.lname == new_metadata.name.lower())
                    existing = session.execute(stmt2).scalar_one_or_none()

                    if existing:
                        raise AlreadyExistsError(
                            f"{self.dataset_type} dataset named '{new_metadata.name}' already exists"
                        )

                ds.name = new_metadata.name

            ds.description = new_metadata.description
            ds.tagline = new_metadata.tagline
            ds.tags = new_metadata.tags
            ds.provenance = new_metadata.provenance
            ds.extras = new_metadata.extras

            ds.default_compute_tag = new_metadata.default_compute_tag
            ds.default_compute_priority = new_metadata.default_compute_priority

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

    def fetch_specification_names(
        self,
        dataset_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[str]:
        """
        Obtain all specification names for a dataset

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
        stmt = select(self.specification_orm.name)
        stmt = stmt.where(self.specification_orm.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            ret = session.execute(stmt).scalars().all()
            return list(ret)

    def fetch_specifications(
        self,
        dataset_id: int,
        specification_names: Optional[Sequence[str]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        Fetch specifications for a dataset from the database

        It's expected there aren't too many specifications, so this always fetches them all.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        specification_names
            Names of the specifications to fetch. If None, fetch all
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

        if specification_names is not None:
            stmt = stmt.where(self.specification_orm.name.in_(specification_names))

        if include or exclude:
            query_opts = get_query_proj_options(self.specification_orm, include, exclude)
            stmt = stmt.options(*query_opts)

        with self.root_socket.optional_session(session, True) as session:
            specifications = session.execute(stmt).scalars().all()

        if specification_names is not None and missing_ok is False:
            found_specifications = {x.name for x in specifications}
            missing_specifications = set(specification_names) - found_specifications
            if missing_specifications:
                s = "\n".join(missing_specifications)
                raise MissingDataError(f"Missing {len(missing_specifications)} specifications: {s}")

        return {x.name: x.model_dict() for x in specifications}

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

        specification_name_map = {k: v for k, v in specification_name_map.items() if k != v}
        if not specification_name_map:
            return

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

            inserted_idx: List[int] = []
            existing_idx: List[int] = []

            # Go in batches of 200 to avoid huge queries
            idx = 0
            for entry_orm_batch in chunk_iterable(entry_orm, 200):
                # Get all existing entries first
                stmt = select(self.entry_orm.name)
                stmt = stmt.where(
                    and_(
                        self.entry_orm.dataset_id == dataset_id,
                        self.entry_orm.name.in_([x.name for x in entry_orm_batch]),
                    )
                )

                existing_entries = set(session.execute(stmt).scalars().all())
                entries_to_add = []

                for entry in entry_orm_batch:
                    # Only add if the entry does not exist
                    if entry.name in existing_entries:
                        existing_idx.append(idx)
                    else:
                        entries_to_add.append(entry)
                        inserted_idx.append(idx)

                    idx += 1

                session.add_all(entries_to_add)

        return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)

    def background_add_entries(
        self, dataset_id: int, new_entries: Sequence[Any], *, session: Optional[Session] = None
    ) -> int:
        """
        Adds entries to a dataset in the database as an internal job

        This creates an internal job for the addition and returns the ID

        See :meth:`add_entries` for details for the rest of the details on functionality and parameters.

        Returns
        -------
        :
            ID of the created internal job
        """

        with self.root_socket.optional_session(session) as session:
            job_id = self.root_socket.internal_jobs.add(
                f"dataset_add_entries_{dataset_id}",
                now_at_utc(),
                f"datasets.add_entry_dicts",
                {
                    "dataset_id": dataset_id,
                    "entry_dicts": encode_to_json(new_entries),
                },
                user_id=None,
                unique_name=False,
                serial_group=f"ds_add_entries_{dataset_id}",  # only run one addition for this dataset at a time
                session=session,
            )

            stmt = (
                insert(DatasetInternalJobORM)
                .values(dataset_id=dataset_id, internal_job_id=job_id)
                .on_conflict_do_nothing()
            )
            session.execute(stmt)
            return job_id

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

        # strip out any renames that don't rename
        entry_name_map = {k: v for k, v in entry_name_map.items() if k != v}
        if not entry_name_map:
            return

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

    def modify_entries(
        self,
        dataset_id: int,
        attribute_map: Optional[Dict[str, Dict[str, Any]]] = None,
        comment_map: Optional[Dict[str, str]] = None,
        overwrite_attributes: bool = False,
        *,
        session: Optional[Session] = None,
    ):
        """
        Modify the attributes of the entries in a dataset.

        If overwrite_attributes is True, replaces existing attribute entry with the value in attribute_map.
        If overwrite_attributes is False, updates existing fields within attributes and adds non-existing fields.
        The attribute_map maps the name of the entry to the new attribute data.
        The comment_map maps the name of an entry to the comment.

        Parameters
        ----------
        dataset_id
            ID of a dataset
        attribute_map
            Mapping of entry names to attributes.
        comment_map
            Mapping of entry names to comments
        overwrite_attributes
            Boolean to indicate if existing entries should be overwritten.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """
        stmt = select(self.entry_orm)
        stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)

        stmt = stmt.where(
            self.entry_orm.name.in_(
                (attribute_map.keys() if (attribute_map is not None) else set())
                | (comment_map.keys() if (comment_map is not None) else set())
            )
        )
        stmt = stmt.options(load_only(self.entry_orm.name, self.entry_orm.attributes, self.entry_orm.comment))
        stmt = stmt.options(lazyload("*"))
        stmt = stmt.with_for_update(skip_locked=False)

        attribute_keys = attribute_map.keys() if (attribute_map is not None) else list()
        comment_keys = comment_map.keys() if (comment_map is not None) else list()

        with self.root_socket.optional_session(session) as session:
            entries = session.execute(stmt).scalars().all()

            for entry in entries:
                if overwrite_attributes:
                    if entry.name in attribute_keys:
                        entry.attributes = attribute_map[entry.name]
                else:
                    if entry.name in attribute_keys:
                        entry.attributes.update(attribute_map[entry.name])
                        flag_modified(entry, "attributes")

                if entry.name in comment_keys:
                    entry.comment = comment_map[entry.name]

    def fetch_records(
        self,
        dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
        specification_names: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        *,
        session: Optional[Session] = None,
    ) -> List[Tuple[str, str, int]]:
        """
        Obtain record ids for a dataset from the database

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
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of record information in the form (entry_name, specification_name, record_id)
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

        with self.root_socket.optional_session(session, True) as session:
            record_items = session.execute(stmt).scalars().all()
            return [(x.entry_name, x.specification_name, x.record_id) for x in record_items]

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

            stmt = delete(BaseDatasetORM)
            stmt = stmt.where(BaseDatasetORM.id == dataset_id)
            session.execute(stmt)

            if delete_records:
                self.root_socket.records.delete(record_ids, soft_delete=False, delete_children=True, session=session)

    def submit(
        self,
        dataset_id: int,
        entry_names: Optional[Iterable[str]],
        specification_names: Optional[Iterable[str]],
        compute_tag: Optional[str],
        compute_priority: Optional[PriorityEnum],
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        job_progress: Optional[JobProgress] = None,
        session: Optional[Session] = None,
    ) -> InsertCountsMetadata:
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
        compute_tag
            Computational tag for new records. If None, use the dataset's default. Existing records
            will not be modified.
        compute_priority
            Priority for new records. If None, use the dataset's default. Existing records
            will not be modified.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records
        job_progress
            Object used to track progress if this function is being run in a background job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Counts of how many records were inserted or already existing. This only applies to records - existing
            records already part of this dataset (ie, a given entry/specification pair already has a record)
            is not counted as existing in the return value.
        """

        n_inserted = 0
        n_existing = 0

        with self.root_socket.optional_session(session) as session:
            compute_tag, compute_priority, owner_user_id, owner_group_id = self.get_submit_info(
                dataset_id, compute_tag, compute_priority, owner_user, owner_group, session=session
            )

            ################################
            # Get specification details
            ################################
            stmt = select(self.specification_orm)

            # We want the actual full specification as well
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
            if entry_names is None:
                # Do all entries in batches using server-side cursors
                stmt = select(self.entry_orm)
                stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)

                # for progress tracking
                if job_progress is not None:
                    total_records = len(ds_specs) * get_count(session, stmt)
                    records_done = 0

                r = session.execute(stmt).scalars()

                while entries_batch := r.fetchmany(500):
                    entries_batch_names = [e.name for e in entries_batch]

                    # Find which records/record_items already exist
                    stmt = select(self.record_item_orm.entry_name, self.record_item_orm.specification_name)
                    stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

                    stmt = stmt.where(self.record_item_orm.entry_name.in_(entries_batch_names))
                    if specification_names is not None:
                        stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))

                    existing_records = session.execute(stmt).all()

                    batch_meta = self._submit(
                        session,
                        dataset_id,
                        entries_batch,
                        ds_specs,
                        existing_records,
                        compute_tag,
                        compute_priority,
                        owner_user_id,
                        owner_group_id,
                        find_existing,
                    )

                    n_inserted += batch_meta.n_inserted
                    n_existing += batch_meta.n_existing

                    if job_progress is not None:
                        job_progress.raise_if_cancelled()
                        records_done += len(entries_batch)
                        job_progress.update_progress(100 * (records_done * len(ds_specs)) / total_records)

            else:  # entry names were given

                # for progress tracking
                if job_progress is not None:
                    total_records = len(ds_specs) * len(entry_names)
                    records_done = 0

                # For checking for missing entries
                found_entries = []

                # Do entries in batches via the given entry names (in batches)
                for entries_names_batch in chunk_iterable(entry_names, 500):
                    stmt = select(self.entry_orm)
                    stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)
                    stmt = stmt.where(self.entry_orm.name.in_(entries_names_batch))

                    entries_batch = session.execute(stmt).scalars().all()

                    entries_batch_names = [e.name for e in entries_batch]
                    found_entries.extend(entries_batch_names)

                    # Find which records/record_items already exist
                    stmt = select(self.record_item_orm.entry_name, self.record_item_orm.specification_name)
                    stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

                    stmt = stmt.where(self.record_item_orm.entry_name.in_(entries_batch_names))
                    if specification_names is not None:
                        stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))

                    existing_records = session.execute(stmt).all()

                    batch_meta = self._submit(
                        session,
                        dataset_id,
                        entries_batch,
                        ds_specs,
                        existing_records,
                        compute_tag,
                        compute_priority,
                        owner_user_id,
                        owner_group_id,
                        find_existing,
                    )

                    n_inserted += batch_meta.n_inserted
                    n_existing += batch_meta.n_existing

                    if job_progress is not None:
                        job_progress.raise_if_cancelled()
                        records_done += len(entries_names_batch)
                        job_progress.update_progress(100 * (records_done * len(ds_specs)) / total_records)

                if entry_names is not None:
                    missing_entries = set(entry_names) - set(found_entries)
                    if missing_entries:
                        raise MissingDataError(f"Could not find all entries. Missing: {missing_entries}")

        return InsertCountsMetadata(n_inserted=n_inserted, n_existing=n_existing)

    def background_submit(
        self,
        dataset_id: int,
        entry_names: Optional[Iterable[str]],
        specification_names: Optional[Iterable[str]],
        compute_tag: Optional[str],
        compute_priority: Optional[PriorityEnum],
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Submit computations for this dataset as an internal job

        This creates an internal job for the submission and returns the ID

        See :meth:`submit` for details for the rest of the details on functionality and parameters.

        Returns
        -------
        :
            ID of the created internal job
        """

        with self.root_socket.optional_session(session) as session:
            job_id = self.root_socket.internal_jobs.add(
                f"dataset_submit_{dataset_id}",
                now_at_utc(),
                f"datasets.submit",
                {
                    "dataset_id": dataset_id,
                    "entry_names": entry_names,
                    "specification_names": specification_names,
                    "compute_tag": compute_tag,
                    "compute_priority": compute_priority,
                    "owner_user": owner_user,
                    "owner_group": owner_group,
                    "find_existing": find_existing,
                },
                user_id=None,
                unique_name=False,
                serial_group=f"ds_submit_{dataset_id}",  # only run one submission for this dataset at a time
                session=session,
            )

            stmt = (
                insert(DatasetInternalJobORM)
                .values(dataset_id=dataset_id, internal_job_id=job_id)
                .on_conflict_do_nothing()
            )
            session.execute(stmt)
            return job_id

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
        compute_priority: Optional[PriorityEnum] = None,
        compute_tag: Optional[str] = None,
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
        compute_priority
            New priority for these records
        compute_tag
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
                record_ids,
                username,
                status=status,
                compute_priority=compute_priority,
                compute_tag=compute_tag,
                comment=comment,
                session=session,
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

    def _copy_entries(
        self,
        session: Session,
        source_dataset_id: int,
        destination_dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
    ):
        """
        Copy entries from one dataset to another

        If `entry_names` is not provided, all entries will be copied
        """

        raise NotImplementedError("_clone_entries must be overridden by the derived class")

    def copy_entries(
        self,
        session: Session,
        source_dataset_id: int,
        destination_dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
    ):
        """
        Copy entries from one dataset to another

        If `entry_names` is not provided, all entries will be copied
        """

        try:
            self._copy_entries(session, source_dataset_id, destination_dataset_id, entry_names)
        except IntegrityError:
            raise UserReportableError(
                "Cannot copy entries from dataset - destination already has entries with the same name"
            )

    def copy_specifications(
        self,
        session: Session,
        source_dataset_id: int,
        destination_dataset_id: int,
        specification_names: Optional[Iterable[str]] = None,
    ):
        """
        Copy specifications from one dataset to another

        If `specification_names` is not provided, all specifications will be copied
        """

        # All dataset specifications (so far) have the same structure. So we can do it for all
        # types of datasets here
        select_stmt = select(
            literal(destination_dataset_id),
            self.specification_orm.name,
            self.specification_orm.description,
            self.specification_orm.specification_id,
        )

        select_stmt = select_stmt.where(self.specification_orm.dataset_id == source_dataset_id)

        if specification_names is not None:
            select_stmt = select_stmt.where(self.specification_orm.name.in_(specification_names))

        stmt = insert(self.specification_orm)
        stmt = stmt.from_select(
            [
                self.specification_orm.dataset_id,
                self.specification_orm.name,
                self.specification_orm.description,
                self.specification_orm.specification_id,
            ],
            select_stmt,
        )

        try:
            session.execute(stmt)
        except IntegrityError:
            raise UserReportableError(
                "Cannot copy specifications from dataset - destination already has specifications with the same name"
            )

    def copy_record_items(
        self,
        session: Session,
        source_dataset_id: int,
        destination_dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
        specification_names: Optional[Iterable[str]] = None,
    ):
        """
        Copy record items from one dataset to another

        This doesn't clone actual records, just the links to the records for the dataset

        If `entry_names` is not provided, all entries will be copied
        If `specification_names` is not provided, all entries will be copied.
        """

        # All dataset record items (so far) have the same structure. So we can do it for all
        # types of datasets here
        select_stmt = select(
            literal(destination_dataset_id),
            self.record_item_orm.entry_name,
            self.record_item_orm.specification_name,
            self.record_item_orm.record_id,
        )

        select_stmt = select_stmt.where(self.record_item_orm.dataset_id == source_dataset_id)

        if entry_names is not None:
            select_stmt = select_stmt.where(self.record_item_orm.entry_name.in_(entry_names))
        if specification_names is not None:
            select_stmt = select_stmt.where(self.record_item_orm.specification_name.in_(specification_names))

        stmt = insert(self.record_item_orm)
        stmt = stmt.from_select(
            [
                self.record_item_orm.dataset_id,
                self.record_item_orm.entry_name,
                self.record_item_orm.specification_name,
                self.record_item_orm.record_id,
            ],
            select_stmt,
        )

        session.execute(stmt)

    def copy_from(
        self,
        source_dataset_id: int,
        destination_dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
        specification_names: Optional[Iterable[str]] = None,
        copy_entries: bool = False,
        copy_specifications: bool = False,
        copy_records: bool = False,
        *,
        session: Optional[Session] = None,
    ):
        """
        Copies entries, specifications, and record items from one dataset to another

        Entries and specifications are copied. Actual records are not duplicated - the new
        dataset will still refer to the same records as the source.

        Parameters
        ----------
        source_dataset_id
            ID of the dataset to copy from
        destination_dataset_id
            ID of the dataset to copy to
        entry_names
            Only copy records for these entries. If none, copy records for all entries
        specification_names
            Only copy records for these specifications. If none, copy records for all specifications
        copy_entries
            If True, copy entries from the source dataset.
        copy_entries
            If True, copy entries from the source dataset.
        copy_specifications
            If True, copy specifications from the source dataset.
        copy_records
            If True, copy record (items) from the source dataset. Implies copy_entries and copy_specifications.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:

            source_dataset_type = self.root_socket.datasets.lookup_type(source_dataset_id, session=session)

            if source_dataset_type != self.dataset_type:
                raise UserReportableError(
                    f"Source dataset type {source_dataset_type} does not match destination type {self.dataset_type}"
                )

            # Copy specifications
            if copy_specifications or copy_records:
                self.copy_specifications(
                    session, source_dataset_id, destination_dataset_id, specification_names=specification_names
                )

            # Copy entries
            if copy_entries or copy_records:
                self.copy_entries(session, source_dataset_id, destination_dataset_id, entry_names=entry_names)

            # Copy record items
            if copy_records:
                self.copy_record_items(
                    session,
                    source_dataset_id,
                    destination_dataset_id,
                    entry_names=entry_names,
                    specification_names=specification_names,
                )

    def clone(
        self,
        source_dataset_id: int,
        new_dataset_name: str,
        *,
        session: Optional[Session] = None,
    ):
        """
        Clones a dataset, copying all entries, specifications and record items

        Entries and specifications are copied. Actual records are not duplicated - the new
        dataset will still refer to the same records as the source.

        Parameters
        ----------
        source_dataset_id
            ID of the dataset to clone
        new_dataset_name
            Name of the new, cloned dataset
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            stmt = select(self.dataset_orm).where(BaseDatasetORM.id == source_dataset_id)
            stmt = stmt.options(noload("*"))
            source_orm = session.execute(stmt).scalar_one_or_none()

            if source_orm is None:
                raise MissingDataError(f"Cannot find dataset with ID {source_dataset_id} for cloning")

            ds_socket = self.root_socket.datasets.get_socket(source_orm.dataset_type)
            new_dataset_id = ds_socket.add(
                name=new_dataset_name,
                description=source_orm.description,
                tagline=source_orm.tagline,
                tags=source_orm.tags,
                provenance=source_orm.provenance,
                default_compute_tag=source_orm.default_compute_tag,
                default_compute_priority=source_orm.default_compute_priority,
                extras=source_orm.extras,
                owner_user=source_orm.owner_user,
                owner_group=source_orm.owner_group,
                existing_ok=False,
            )

            #################################################
            # Copy entries, specifications, and record items
            #################################################
            self.copy_from(source_dataset_id, new_dataset_id, copy_records=True, session=session)

            #####################
            # Contributed values
            #####################
            # I changed my mind - maybe we shouldn't copy contributed values. But leaving this here
            # in case we want to in the future
            # stmt = text("""
            #    INSERT INTO contributed_values (dataset_id, name, values, index, values_structure, theory_level,
            #                                    units, theory_level_details, citations, external_url, doi, comments)
            #    SELECT :new_dataset_id, name, values, index, values_structure, theory_level,
            #           units, theory_level_details, citations, external_url, doi, comments
            #    FROM contributed_values
            #    WHERE dataset_id = :source_dataset_id
            # """)

            # stmt = stmt.bindparams(new_dataset_id=new_dataset_id, source_dataset_id=source_dataset_id)
            # session.execute(stmt)

            #####################
            # Attachments
            #####################
            # Similarly, I don't think we want to copy attachments. The whole point is that the new
            # dataset will be modified/expanded, so not sure it makes sense. But someone might want it
            # in the future

            return new_dataset_id


class DatasetSocket:
    """
    Root socket for all dataset sockets
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        from qcfractal.components.singlepoint.dataset_socket import SinglepointDatasetSocket
        from qcfractal.components.optimization.dataset_socket import OptimizationDatasetSocket
        from qcfractal.components.torsiondrive.dataset_socket import TorsiondriveDatasetSocket
        from qcfractal.components.gridoptimization.dataset_socket import GridoptimizationDatasetSocket
        from qcfractal.components.manybody.dataset_socket import ManybodyDatasetSocket
        from qcfractal.components.reaction.dataset_socket import ReactionDatasetSocket
        from qcfractal.components.neb.dataset_socket import NEBDatasetSocket

        self.singlepoint = SinglepointDatasetSocket(root_socket)
        self.optimization = OptimizationDatasetSocket(root_socket)
        self.torsiondrive = TorsiondriveDatasetSocket(root_socket)
        self.gridoptimization = GridoptimizationDatasetSocket(root_socket)
        self.manybody = ManybodyDatasetSocket(root_socket)
        self.reaction = ReactionDatasetSocket(root_socket)
        self.neb = NEBDatasetSocket(root_socket)

        self._handler_map: Dict[str, BaseDatasetSocket] = {
            self.singlepoint.dataset_type: self.singlepoint,
            self.optimization.dataset_type: self.optimization,
            self.torsiondrive.dataset_type: self.torsiondrive,
            self.gridoptimization.dataset_type: self.gridoptimization,
            self.manybody.dataset_type: self.manybody,
            self.reaction.dataset_type: self.reaction,
            self.neb.dataset_type: self.neb,
        }

        # Get the SQL 'select' statements for records belonging to a dataset
        # and union them into a CTE
        record_selects = []
        for h in self._handler_map.values():
            sel = h.get_records_select()
            record_selects.extend(sel)

        self._record_cte = union(*record_selects).cte()

        # Same for record counts
        counts_selects = []
        for h in self._handler_map.values():
            sel = h.get_record_counts_select()
            counts_selects.extend(sel)

        self._record_count_cte = union(*counts_selects).cte()

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

    def status(self, dataset_id: int, *, session: Optional[Session] = None) -> Dict[str, Dict[RecordStatusEnum, int]]:
        """
        Compute the status of a dataset

        This function will perform the lookup of the dataset type and the call the dataset-specific socket

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

        with self.root_socket.optional_session(session, True) as session:
            ds_type = self.lookup_type(dataset_id, session=session)
            ds_socket = self.get_socket(ds_type)
            return ds_socket.status(dataset_id, session=session)

    def overall_status(self, dataset_id: int, *, session: Optional[Session] = None) -> Dict[RecordStatusEnum, int]:
        """
        Compute the overall status of a dataset

        Similar to the status() function, but is only a dictionary of status and the counts. That is, the status
        is not broken out by specification

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
            Dictionary with record status as keys and counts as values.
        """

        stat = self.status(dataset_id, session=session)

        overall_stat = {}
        for spec_stats in stat.values():
            for k, v in spec_stats.items():
                overall_stat.setdefault(k, 0)
                overall_stat[k] += v

        return overall_stat

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
            stmt = select(
                BaseDatasetORM.id,
                BaseDatasetORM.dataset_type,
                BaseDatasetORM.name,
                BaseDatasetORM.tagline,
                BaseDatasetORM.description,
                func.coalesce(self._record_count_cte.c.record_count, 0),
            )
            stmt = stmt.join(
                self._record_count_cte, self._record_count_cte.c.dataset_id == BaseDatasetORM.id, isouter=True
            )
            stmt = stmt.order_by(BaseDatasetORM.id.asc())
            r = session.execute(stmt).all()

            return [
                {
                    "id": x[0],
                    "dataset_type": x[1],
                    "dataset_name": x[2],
                    "tagline": x[3],
                    "description": x[4],
                    "record_count": x[5],
                }
                for x in r
            ]

    def delete(
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
            ds_type = self.root_socket.datasets.lookup_type(dataset_id, session=session)
            ds_socket = self.root_socket.datasets.get_socket(ds_type)
            return ds_socket.delete_dataset(dataset_id, delete_records, session=session)

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

    def get_internal_job(
        self,
        dataset_id: int,
        job_id: int,
        *,
        session: Optional[Session] = None,
    ):
        stmt = select(InternalJobORM)
        stmt = stmt.join(DatasetInternalJobORM, DatasetInternalJobORM.internal_job_id == InternalJobORM.id)
        stmt = stmt.where(DatasetInternalJobORM.dataset_id == dataset_id)
        stmt = stmt.where(DatasetInternalJobORM.internal_job_id == job_id)

        with self.root_socket.optional_session(session, True) as session:
            ij_orm = session.execute(stmt).scalar_one_or_none()
            if ij_orm is None:
                raise MissingDataError(f"Job id {job_id} not found in dataset {dataset_id}")
            return ij_orm.model_dict()

    def list_internal_jobs(
        self,
        dataset_id: int,
        status: Optional[Iterable[InternalJobStatusEnum]] = None,
        *,
        session: Optional[Session] = None,
    ):
        stmt = select(InternalJobORM)
        stmt = stmt.join(DatasetInternalJobORM, DatasetInternalJobORM.internal_job_id == InternalJobORM.id)
        stmt = stmt.where(DatasetInternalJobORM.dataset_id == dataset_id)

        if status is not None:
            stmt = stmt.where(InternalJobORM.status.in_(status))

        with self.root_socket.optional_session(session, True) as session:
            ij_orm = session.execute(stmt).scalars().all()
            return [i.model_dict() for i in ij_orm]

    def get_attachments(self, dataset_id: int, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get the attachments for a dataset
        """

        stmt = select(DatasetAttachmentORM)
        stmt = stmt.where(DatasetAttachmentORM.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            att = session.execute(stmt).scalars().all()
            return [x.model_dict() for x in att]

    def delete_attachment(self, dataset_id: int, file_id: int, *, session: Optional[Session] = None):
        stmt = select(DatasetAttachmentORM)
        stmt = stmt.where(DatasetAttachmentORM.dataset_id == dataset_id)
        stmt = stmt.where(DatasetAttachmentORM.id == file_id)
        stmt = stmt.with_for_update()

        with self.root_socket.optional_session(session) as session:
            att = session.execute(stmt).scalar_one_or_none()
            if att is None:
                raise MissingDataError(f"Attachment with file id {file_id} not found in dataset {dataset_id}")

            return self.root_socket.external_files.delete(file_id, session=session)

    def attach_file(
        self,
        dataset_id: int,
        attachment_type: DatasetAttachmentType,
        file_path: str,
        file_name: str,
        description: Optional[str],
        provenance: Dict[str, Any],
        *,
        job_progress: Optional[JobProgress] = None,
        session: Optional[Session] = None,
    ) -> int:
        """
        Attach a file to a dataset

        This function uploads the specified file and associates it with the dataset by creating
        a corresponding dataset attachment record. This operation requires S3 storage to be enabled.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to which the file will be attached.
        attachment_type
            The type of attachment that categorizes the file being added.
        file_path
            The local file system path to the file that needs to be uploaded.
        file_name
            The name of the file to be used in the attachment record. This is the filename that is
            recommended to the user by default.
        description
            An optional description of the file
        provenance
            A dictionary containing metadata regarding the origin or history of the file.
        job_progress
            Object used to track progress if this function is being run in a background job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Raises
        ------
        UserReportableError
            Raised if S3 storage is not enabled
        """

        if not self.root_socket.qcf_config.s3.enabled:
            raise UserReportableError("S3 storage is not enabled. Can not attach file to a dataset")

        self._logger.info(f"Uploading/Attaching dataset-related file: {file_path}")
        with self.root_socket.optional_session(session) as session:
            ef = DatasetAttachmentORM(
                dataset_id=dataset_id,
                attachment_type=attachment_type,
                file_name=file_name,
                description=description,
                provenance=provenance,
            )

            file_id = self.root_socket.external_files.add_file(
                file_path, ef, session=session, job_progress=job_progress
            )

            self._logger.info(f"Dataset attachment {file_path} successfully uploaded to S3. ID is {file_id}")
            return file_id

    def create_view_attachment(
        self,
        dataset_id: int,
        dataset_type: str,
        description: Optional[str],
        provenance: Dict[str, Any],
        status: Optional[Iterable[RecordStatusEnum]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        *,
        include_children: bool = True,
        job_progress: Optional[JobProgress] = None,
        session: Optional[Session] = None,
    ):
        """
        Creates a dataset view and attaches it to the dataset

        Uses a temporary directory within the globally-configured `temporary_dir`

        Parameters
        ----------
        dataset_id : int
            ID of the dataset to create the view for
        dataset_type
            Type of dataset the ID is
        description
            Optional string describing the view file
        provenance
            Dictionary with any metadata or other information about the view. Information regarding
            the options used to create the view will be added.
        status
            List of statuses to include. Default is to include records with any status
        include
            List of specific record fields to include in the export. Default is to include most fields
        exclude
            List of specific record fields to exclude from the export. Defaults to excluding none.
        include_children
            Specifies whether child records associated with the main records should also be included (recursively)
            in the view file.
        job_progress
            Object used to track progress if this function is being run in a background job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        if not self.root_socket.qcf_config.s3.enabled:
            raise UserReportableError("S3 storage is not enabled. Can not not create view")

        # Add the options for the view creation to the provenance
        provenance = provenance | {
            "options": {
                "status": status,
                "include": include,
                "exclude": exclude,
                "include_children": include_children,
            }
        }

        with tempfile.TemporaryDirectory(dir=self.root_socket.qcf_config.temporary_dir) as tmpdir:
            self._logger.info(f"Using temporary directory {tmpdir} for view creation")

            file_name = f"dataset_{dataset_id}_view.sqlite"
            tmp_file_path = os.path.join(tmpdir, file_name)

            create_view_file(
                session,
                self.root_socket,
                dataset_id,
                dataset_type,
                tmp_file_path,
                status=status,
                include=include,
                exclude=exclude,
                include_children=include_children,
                job_progress=job_progress,
            )

            self._logger.info(f"View file created. File size is {os.path.getsize(tmp_file_path)/1048576} MiB.")

            if job_progress is not None:
                job_progress.update_progress(90, "Uploading view file to S3")

            file_id = self.attach_file(
                dataset_id, DatasetAttachmentType.view, tmp_file_path, file_name, description, provenance
            )

            if job_progress is not None:
                job_progress.update_progress(100)

            return file_id

    def add_create_view_attachment_job(
        self,
        dataset_id: int,
        dataset_type: str,
        description: str,
        provenance: Dict[str, Any],
        status: Optional[Iterable[RecordStatusEnum]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        *,
        include_children: bool = True,
        session: Optional[Session] = None,
    ) -> int:
        """
        Creates an internal job for creating and attaching a view to a dataset

        See :meth:`create_view_attachment` for a description of the parameters

        Returns
        -------
        :
            ID of the created internal job
        """

        if not self.root_socket.qcf_config.s3.enabled:
            raise UserReportableError("S3 storage is not enabled. Can not not create view")

        with self.root_socket.optional_session(session) as session:
            job_id = self.root_socket.internal_jobs.add(
                f"create_attach_view_ds_{dataset_id}",
                now_at_utc(),
                f"datasets.create_view_attachment",
                {
                    "dataset_id": dataset_id,
                    "dataset_type": dataset_type,
                    "description": description,
                    "provenance": provenance,
                    "status": status,
                    "include": include,
                    "exclude": exclude,
                    "include_children": include_children,
                },
                user_id=None,
                unique_name=True,
                serial_group="ds_create_view",
                session=session,
            )

            stmt = (
                insert(DatasetInternalJobORM)
                .values(dataset_id=dataset_id, internal_job_id=job_id)
                .on_conflict_do_nothing()
            )
            session.execute(stmt)
            return job_id

    def submit(
        self,
        dataset_id: int,
        entry_names: Optional[Iterable[str]],
        specification_names: Optional[Iterable[str]],
        compute_tag: Optional[str],
        compute_priority: Optional[PriorityEnum],
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ):
        """
        Submit computations for a dataset

        This function looks up the dataset socket and then call submit on that socket
        """

        with self.root_socket.optional_session(session) as session:
            ds_type = self.lookup_type(dataset_id)
            ds_socket = self.get_socket(ds_type)

            return ds_socket.submit(
                dataset_id=dataset_id,
                entry_names=entry_names,
                specification_names=specification_names,
                compute_tag=compute_tag,
                compute_priority=compute_priority,
                owner_user=owner_user,
                owner_group=owner_group,
                find_existing=find_existing,
                session=session,
            )

    def add_entry_dicts(
        self, dataset_id: int, entry_dicts: bytes, *, session: Optional[Session] = None
    ) -> InsertMetadata:
        """
        Add entries to a dataset, where entries are dictionaries

        This function looks up the dataset socket and then casts to the appropriate type,
        calling the add_entries function of that socket
        """

        with self.root_socket.optional_session(session) as session:
            ds_type = self.lookup_type(dataset_id)
            ds_socket = self.get_socket(ds_type)

            # entry types always derive from newentry types
            entry_type = BaseDataset.get_subclass(ds_type)._new_entry_type
            new_entries = [entry_type(**d) for d in entry_dicts]

            return ds_socket.add_entries(
                dataset_id=dataset_id,
                new_entries=new_entries,
                session=session,
            )
