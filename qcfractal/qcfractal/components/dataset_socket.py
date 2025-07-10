from __future__ import annotations

import logging
import os
import tempfile
from typing import TYPE_CHECKING

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import with_polymorphic

from qcfractal.components.base_dataset_socket import BaseDatasetSocket
from qcfractal.components.dataset_db_models import (
    BaseDatasetORM,
    ContributedValuesORM,
    DatasetAttachmentORM,
    DatasetInternalJobORM,
)
from qcfractal.components.dataset_db_views import DatasetDirectRecordsView
from qcfractal.components.dataset_processing import create_view_file
from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.db_socket.helpers import (
    get_general,
)
from qcportal.dataset_models import BaseDataset, DatasetAttachmentType
from qcportal.exceptions import MissingDataError, UserReportableError
from qcportal.internal_jobs import InternalJobStatusEnum
from qcportal.metadata_models import InsertMetadata
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.components.internal_jobs.status import JobProgress
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Dict, Any, Optional, Sequence, Iterable, List, Union
    from typing import Iterable, List, Dict, Any
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


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
            wp = BaseDatasetORM

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

        count_cte = (
            select(
                DatasetDirectRecordsView.c.dataset_id,
                func.count(DatasetDirectRecordsView.c.record_id).label("record_count"),
            )
            .group_by(DatasetDirectRecordsView.c.dataset_id)
            .cte()
        )

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(
                BaseDatasetORM.id,
                BaseDatasetORM.dataset_type,
                BaseDatasetORM.name,
                BaseDatasetORM.tagline,
                BaseDatasetORM.description,
                func.coalesce(count_cte.c.record_count, 0),
            )
            stmt = stmt.join(count_cte, count_cte.c.dataset_id == BaseDatasetORM.id, isouter=True)
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
            DatasetDirectRecordsView.c.record_id,
            BaseDatasetORM.id,
            BaseDatasetORM.dataset_type,
            BaseDatasetORM.name,
            DatasetDirectRecordsView.c.entry_name,
            DatasetDirectRecordsView.c.specification_name,
        )
        stmt = stmt.join(DatasetDirectRecordsView, BaseDatasetORM.id == DatasetDirectRecordsView.c.dataset_id)
        stmt = stmt.where(DatasetDirectRecordsView.c.record_id.in_(record_id))

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
        creator_user: Optional[Union[int, str]],
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
                creator_user=creator_user,
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
