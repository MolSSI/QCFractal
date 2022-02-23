from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, delete, func, union
from sqlalchemy.orm import load_only, lazyload

from qcfractal.components.datasets.db_models import CollectionORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.db_socket.helpers import (
    get_general,
    get_query_proj_options,
)
from qcportal.exceptions import AlreadyExistsError, MissingDataError
from qcportal.records import RecordModifyBody, RecordStatusEnum, PriorityEnum

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.metadata_models import InsertMetadata
    from qcportal.datasets.models import DatasetRecordModifyBody, DatasetRecordRevertBody, DatasetModifyMetadataBody
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.db_socket.base_orm import BaseORM
    from typing import Dict, Any, Optional, Sequence, Iterable, Tuple, List


class BaseDatasetSocket:

    # Must be overridden by the derived classes
    dataset_orm = None
    specification_orm = None
    entry_orm = None
    record_item_orm = None

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

        self.dataset_type = self.dataset_orm.__mapper_args__["polymorphic_identity"]

    def _add_specification(self, session, specification) -> Tuple[InsertMetadata, Optional[int]]:
        raise NotImplementedError("_add_specification must be overridden by the derived class")

    def _create_entries(self, session, dataset_id, new_entries) -> Sequence[BaseORM]:
        raise NotImplementedError("_create_entries must be overridden by the derived class")

    @staticmethod
    def get_records_select():
        raise NotImplementedError(f"get_records_select not implemented! This is a developer error")

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
        raise NotImplementedError("submit must be overridden by the derived class")

    def get_tag_priority(
        self, dataset_id: int, tag: Optional[str], priority: Optional[str], *, session: Optional[Session] = None
    ):
        if tag is None or priority is None:
            default_tag, default_priority = self.get_default_tag_priority(dataset_id, session=session)
            if tag is None:
                tag = default_tag
            if priority is None:
                priority = default_priority
        return tag, priority

    def get_default_tag_priority(
        self, dataset_id: int, *, session: Optional[Session] = None
    ) -> Tuple[str, PriorityEnum]:

        stmt = select(CollectionORM.default_tag, CollectionORM.default_priority)
        stmt = stmt.where(CollectionORM.id == dataset_id)

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

        with self.root_socket.optional_session(session) as session:
            return get_general(
                session, self.dataset_orm, self.dataset_orm.id, (dataset_id,), include, exclude, missing_ok
            )[0]

    def status(self, dataset_id: int, *, session: Optional[Session] = None) -> Dict[str, Dict[RecordStatusEnum, int]]:

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

        stmt = select(self.record_item_orm.entry_name, self.record_item_orm.specification_name, BaseRecordORM.status)
        stmt = stmt.join(self.record_item_orm, BaseRecordORM.id == self.record_item_orm.record_id)
        stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            stats = session.execute(stmt).all()
            return [tuple(x) for x in stats]

    def add(
        self,
        name: str,
        description: Optional[str] = None,
        tagline: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        group: Optional[str] = None,
        provenance: Optional[Dict[str, Any]] = None,
        visibility: bool = True,
        default_tag: Optional[str] = None,
        default_priority: PriorityEnum = PriorityEnum.normal,
        *,
        session: Optional[Session] = None,
    ) -> int:

        # TODO - nullable group?
        if group is None:
            group = "default"

        ds_orm = self.dataset_orm(
            collection_type=self.dataset_type,
            collection=self.dataset_type,
            name=name,
            lname=name.lower(),
            tagline=tagline,
            description=description,
            tags=tags,
            group=group,
            provenance=provenance,
            visibility=visibility,
            default_tag=default_tag,
            default_priority=default_priority,
        )

        with self.root_socket.optional_session(session) as session:
            stmt = select(self.dataset_orm.id)
            stmt = stmt.where(self.dataset_orm.lname == name.lower())
            stmt = stmt.where(self.dataset_orm.collection_type == self.dataset_type)
            existing = session.execute(stmt).scalar_one_or_none()

            if existing is not None:
                raise AlreadyExistsError(f"Dataset with type='{self.dataset_type}' and name='{name}' already exists")

            session.add(ds_orm)
            session.commit()
            return ds_orm.id

    def update_metadata(
        self, dataset_id: int, new_metadata: DatasetModifyMetadataBody, *, session: Optional[Session] = None
    ):
        with self.root_socket.optional_session(session) as session:
            stmt = select(self.dataset_orm).options(lazyload("*")).with_for_update()
            ds = session.execute(stmt).scalar_one_or_none()

            if ds is None:
                raise MissingDataError(f"Could not find dataset with type={self.dataset_type} and {dataset_id}")

            ds.name = new_metadata.name
            ds.lname = new_metadata.name.lower()
            ds.description = new_metadata.description
            ds.tagline = new_metadata.tagline
            ds.tags = new_metadata.tags
            ds.group = new_metadata.group
            ds.visibility = new_metadata.visibility
            ds.provenance = new_metadata.provenance

            ds.default_tag = new_metadata.default_tag
            ds.default_priority = new_metadata.default_priority

    def add_specifications(
        self,
        dataset_id: int,
        new_specifications: Iterable[Any],  # we don't know the type here
        *,
        session: Optional[Session] = None,
    ):

        with self.root_socket.optional_session(session) as session:
            stmt = select(self.specification_orm.name)
            stmt = stmt.where(self.specification_orm.dataset_id == dataset_id)

            existing_specs = session.execute(stmt).scalars().all()

            for ds_spec in new_specifications:
                if ds_spec.name in existing_specs:
                    raise AlreadyExistsError(f"Specification '{ds_spec.name}' already exists for this dataset")

                # call the derived class function for adding a specification
                meta, spec_id = self._add_specification(session, ds_spec.specification)

                if not meta.success:
                    raise RuntimeError(
                        f"Unable to add {self.dataset_type} specification: " + meta.error_string,
                    )

                ds_spec_orm = self.specification_orm(
                    dataset_id=dataset_id, name=ds_spec.name, description=ds_spec.description, specification_id=spec_id
                )

                session.add(ds_spec_orm)

    def delete_specifications(
        self,
        dataset_id: int,
        specification_names: Iterable[str],
        delete_records: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> int:

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
            r = session.execute(stmt)
            session.flush()

            if delete_records:
                self.root_socket.records.delete(record_ids, soft_delete=False, delete_children=True, session=session)

            return r.rowcount

    def rename_specifications(
        self, dataset_id: int, specification_name_map: Dict[str, str], *, session: Optional[Session] = None
    ):
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

    def add_entries(self, dataset_id: int, new_entries: Sequence[Any], *, session: Optional[Session] = None):

        with self.root_socket.optional_session(session) as session:
            # Create orm for all entries (in derived class)
            entry_orm = self._create_entries(session, dataset_id, new_entries)

            # Get all existing entries first
            stmt = select(self.entry_orm.name)
            stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)
            existing_entries = session.execute(stmt).scalars().all()

            for entry in entry_orm:
                # Only add if the entry does not exist
                if entry.name not in existing_entries:
                    session.add(entry)

    def get_entry_names(
        self,
        dataset_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[str]:
        stmt = select(self.entry_orm.name)
        stmt = stmt.where(self.entry_orm.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            ret = session.execute(stmt).scalars().all()
            return list(ret)

    def get_entries(
        self,
        dataset_id: int,
        entry_names: Optional[Sequence[str]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
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

        return [x.dict() for x in entries]

    def delete_entries(
        self,
        dataset_id: int,
        entry_names: Iterable[str],
        delete_records: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> int:

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
            r = session.execute(stmt)
            session.flush()

            if delete_records:
                self.root_socket.records.delete(record_ids, soft_delete=False, delete_children=True, session=session)

            return r.rowcount

    def rename_entries(self, dataset_id: int, entry_name_map: Dict[str, str], *, session: Optional[Session] = None):
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

    def get_record_items(
        self,
        dataset_id: int,
        entry_names: Optional[Sequence[str]] = None,
        specification_names: Optional[Sequence[str]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:

        stmt = select(self.record_item_orm)
        stmt = stmt.where(self.record_item_orm.dataset_id == dataset_id)

        if entry_names is not None:
            stmt = stmt.where(self.record_item_orm.entry_name.in_(entry_names))
        if specification_names is not None:
            stmt = stmt.where(self.record_item_orm.specification_name.in_(specification_names))

        if include or exclude:
            query_opts = get_query_proj_options(self.record_item_orm, include, exclude)
            stmt = stmt.options(*query_opts)

        with self.root_socket.optional_session(session, True) as session:
            records = session.execute(stmt).scalars().all()

        return [x.dict() for x in records]

    def delete_record_items(
        self,
        dataset_id: int,
        entry_names: Sequence[str],
        specification_names: Sequence[str],
        delete_records: bool,
        *,
        session: Optional[Session] = None,
    ):

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
        modify_data: DatasetRecordModifyBody,
        username: Optional[str],
        *,
        session: Optional[Session] = None,
    ):

        with self.root_socket.optional_session(session) as session:

            record_ids = self._lookup_record_ids(
                session,
                dataset_id,
                modify_data.entry_names,
                modify_data.specification_names,
                for_update=True,
            )

            # TODO - kinda hacky to pull in a rest model?
            # Break these out into function arguments?
            rec_modify_data = RecordModifyBody(
                record_ids=record_ids,
                status=modify_data.status,
                priority=modify_data.priority,
                tag=modify_data.tag,
                comment=modify_data.comment,
            )

            self.root_socket.records.modify_generic(rec_modify_data, username, session=session)

    def revert_records(
        self,
        dataset_id: int,
        revert_data: DatasetRecordRevertBody,
        *,
        session: Optional[Session] = None,
    ):

        with self.root_socket.optional_session(session) as session:
            record_ids = self._lookup_record_ids(
                session, dataset_id, revert_data.entry_names, revert_data.specification_names, for_update=True
            )

            self.root_socket.records.revert_generic(record_ids, revert_data.revert_status)


class DatasetSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        from .singlepoint.sockets import SinglepointDatasetSocket
        from .optimization.sockets import OptimizationDatasetSocket

        self.singlepoint = SinglepointDatasetSocket(root_socket)
        self.optimization = OptimizationDatasetSocket(root_socket)

        self._handler_map: Dict[str, BaseDatasetSocket] = {
            self.singlepoint.dataset_type: self.singlepoint,
            self.optimization.dataset_type: self.optimization,
        }

        # Get the SQL 'select' statements from the handlers
        selects = []
        for h in self._handler_map.values():
            sel = h.get_records_select()
            selects.extend(sel)

        # Union them into a CTE
        self._record_cte = union(*selects).cte()

    def get_socket(self, dataset_type: str) -> BaseDatasetSocket:
        handler = self._handler_map.get(dataset_type, None)
        if handler is None:
            raise MissingDataError(f"Cannot find handler for type {dataset_type}")
        return handler

    def lookup_type(self, dataset_id: int, *, session: Optional[Session] = None) -> str:

        stmt = select(CollectionORM.collection_type)
        stmt = stmt.where(CollectionORM.id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            ds_type = session.execute(stmt).scalar_one_or_none()

            if ds_type is None:
                raise MissingDataError(f"Could not find dataset with id {dataset_id}")

            return ds_type

    def lookup_id(
        self, dataset_type: str, dataset_name: str, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[int]:

        stmt = select(CollectionORM.id)
        stmt = stmt.where(CollectionORM.lname == dataset_name.lower())
        stmt = stmt.where(CollectionORM.collection_type == dataset_type.lower())

        with self.root_socket.optional_session(session, True) as session:
            ds_id = session.execute(stmt).scalar_one_or_none()

            if missing_ok is False and ds_id is None:
                raise MissingDataError(f"Could not find {dataset_type} dataset with name '{dataset_name}'")
            return ds_id

    def list(self, *, session: Optional[Session] = None):
        with self.root_socket.optional_session(session, True) as session:
            stmt = select(CollectionORM.id, CollectionORM.collection_type, CollectionORM.name)
            r = session.execute(stmt).all()

            return [{"id": x[0], "dataset_type": x[1], "dataset_name": x[2]} for x in r]
