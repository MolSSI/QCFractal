from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, delete, func

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.components.project_db_models import (
    ProjectORM,
    ProjectRecordORM,
    ProjectDatasetORM,
    ProjectAttachmentORM,
    ProjectInternalJobORM,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket.helpers import get_general
from qcportal.all_inputs import AllInputTypes
from qcportal.exceptions import MissingDataError, UserReportableError, AlreadyExistsError
from qcportal.internal_jobs import InternalJobStatusEnum
from qcportal.metadata_models import InsertCountsMetadata
from qcportal.project_models import ProjectAttachmentType
from qcportal.record_models import PriorityEnum, RecordStatusEnum

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.components.internal_jobs.status import JobProgress
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Dict, Any, Optional, Sequence, Iterable, List
    from typing import Iterable, List, Dict, Any, Union, Tuple
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


class ProjectSocket:
    """
    Socket for projects
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

    def add(
        self,
        name: str,
        description: str,
        tagline: str,
        tags: List[str],
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
        Create a new project in the database

        If a project already exists with the same name, an exception is raised if existing_ok is False

        Returns
        -------
        :
            ID of the new dataset
        """

        proj_orm = ProjectORM(
            name=name,
            description=description,
            tagline=tagline,
            tags=tags,
            default_compute_tag=default_compute_tag.lower(),
            default_compute_priority=default_compute_priority,
            extras=extras,
        )

        with self.root_socket.optional_session(session) as session:
            user_id, group_id = self.root_socket.users.get_owner_ids(owner_user, owner_group)
            self.root_socket.users.assert_group_member(user_id, group_id, session=session)

            stmt = select(ProjectORM.id)
            stmt = stmt.where(ProjectORM.lname == name.lower())
            existing_id = session.execute(stmt).scalar_one_or_none()

            if existing_id is not None:
                if existing_ok:
                    return existing_id
                else:
                    raise AlreadyExistsError(f"Project with name='{name}' already exists")

            proj_orm.owner_user_id = user_id
            proj_orm.owner_group_id = group_id

            session.add(proj_orm)
            session.commit()
            return proj_orm.id

    def get(
        self,
        project_id: int,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Obtain a project with the specified ID

        Parameters
        ----------
        project_id
            ID of a project
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, None will be returned if the project does not exist. Otherwise,
           an exception is raised.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Project information as a dictionary
        """

        with self.root_socket.optional_session(session, True) as session:
            try:
                return get_general(session, ProjectORM, ProjectORM.id, [project_id], include, exclude, missing_ok)[0]
            except MissingDataError:
                raise MissingDataError(f"Could not find project with id '{project_id}'")

    def lookup_id(
        self, project_name: str, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[int]:
        """
        Look up a project ID given its project type and name
        """

        stmt = select(ProjectORM.id)
        stmt = stmt.where(ProjectORM.lname == project_name.lower())

        with self.root_socket.optional_session(session, True) as session:
            ds_id = session.execute(stmt).scalar_one_or_none()

            if missing_ok is False and ds_id is None:
                raise MissingDataError(f"Could not find project with name '{project_name}'")
            return ds_id

    def list(self, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get a list of projects in the database
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(
                ProjectORM.id,
                ProjectORM.name,
            )
            stmt = stmt.order_by(ProjectORM.id.asc())
            r = session.execute(stmt).all()

            return [{"id": x[0], "project_name": x[1]} for x in r]

    def delete_project(
        self,
        project_id: int,
        delete_records: bool,
        delete_datasets: bool,
        delete_datasets_records: bool,
        *,
        session: Optional[Session] = None,
    ):
        """
        Deletes a project from the database

        Parameters
        ----------
        project_id
            ID of a dataset
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            if delete_records:
                rec_ids = self.get_record_ids(project_id, session=session)
                self.unlink_records(project_id, rec_ids, delete_records=True, session=session)
            if delete_datasets:
                ds_ids = self.get_dataset_ids(project_id, session=session)
                self.unlink_datasets(
                    project_id,
                    ds_ids,
                    delete_datasets=True,
                    delete_datasets_records=delete_datasets_records,
                    session=session,
                )

            stmt = delete(ProjectORM)
            stmt = stmt.where(ProjectORM.id == project_id)
            session.execute(stmt)

    #################################
    # Status and General Info
    #################################
    def status(self, project_id: int, *, session: Optional[Session] = None) -> Dict[str, Dict[RecordStatusEnum, int]]:

        total_status = {}

        with self.root_socket.optional_session(session, True) as session:
            # Record status
            stmt = select(BaseRecordORM.status, func.count(BaseRecordORM.id))
            stmt = stmt.join(ProjectRecordORM, BaseRecordORM.id == ProjectRecordORM.record_id)
            stmt = stmt.where(ProjectRecordORM.project_id == project_id)
            stmt = stmt.group_by(BaseRecordORM.status)

            record_status = session.execute(stmt).all()
            total_status["records"] = {r[0]: r[1] for r in record_status}

            # Dataset status
            # do one at a time
            total_status["datasets"] = {}

            ds_ids = self.get_dataset_ids(project_id, session=session)
            for ds_id in ds_ids:
                ds_status = self.root_socket.datasets.overall_status(ds_id)
                for k, v in ds_status.items():
                    total_status["datasets"].setdefault(k, 0)
                    total_status["datasets"][k] += v

            return total_status

    #################################
    # Datasets
    #################################

    def assert_dataset_belongs(self, project_id: int, dataset_id: int, *, session: Optional[Session] = None):

        stmt = select(ProjectDatasetORM.dataset_id)
        stmt = stmt.where(ProjectDatasetORM.project_id == project_id)
        stmt = stmt.where(ProjectDatasetORM.dataset_id == dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            ds_id = session.execute(stmt).scalar_one_or_none()

            if ds_id is None:
                raise MissingDataError(f"Dataset {dataset_id} not found in project {project_id}")

    def dataset_name_exists(self, project_id: int, dataset_name: str, *, session: Optional[Session] = None):
        stmt = select(ProjectDatasetORM.dataset_id)
        stmt = stmt.join(BaseDatasetORM, ProjectDatasetORM.dataset_id == BaseDatasetORM.id)
        stmt = stmt.where(ProjectDatasetORM.project_id == project_id)
        stmt = stmt.where(BaseDatasetORM.lname == dataset_name.lower())

        with self.root_socket.optional_session(session, True) as session:
            ds_id = session.execute(stmt).scalar_one_or_none()
            return ds_id is not None

    def get_dataset_metadata(self, project_id: int, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        stmt = select(
            BaseDatasetORM.id,
            BaseDatasetORM.dataset_type,
            BaseDatasetORM.name,
            BaseDatasetORM.description,
            BaseDatasetORM.tagline,
            BaseDatasetORM.tags,
        )
        stmt = stmt.join(ProjectDatasetORM, BaseDatasetORM.id == ProjectDatasetORM.dataset_id)
        stmt = stmt.where(ProjectDatasetORM.project_id == project_id)

        with self.root_socket.optional_session(session, True) as session:
            meta_info = session.execute(stmt).all()
            return [
                {
                    "dataset_id": d[0],
                    "dataset_type": d[1],
                    "name": d[2],
                    "description": d[3],
                    "tagline": d[4],
                    "tags": d[5],
                }
                for d in meta_info
            ]

    def get_dataset_ids(self, project_id: int, *, session: Optional[Session] = None) -> List[int]:
        stmt = select(ProjectDatasetORM.dataset_id).where(ProjectDatasetORM.project_id == project_id)

        with self.root_socket.optional_session(session, True) as session:
            rids = session.execute(stmt).scalars().all()
            return list(rids)

    def add_dataset(
        self,
        project_id: int,
        dataset_type: str,
        dataset_name: str,
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

        ds_socket = self.root_socket.datasets.get_socket(dataset_type)

        with self.root_socket.optional_session(session) as session:
            if self.dataset_name_exists(project_id, dataset_name, session=session):
                raise ValueError(f"Dataset '{dataset_name}' already exists in project {project_id}")

            ds_id = ds_socket.add(
                name=dataset_name,
                description=description,
                tagline=tagline,
                tags=tags,
                provenance=provenance,
                default_compute_tag=default_compute_tag,
                default_compute_priority=default_compute_priority,
                extras=extras,
                owner_user=owner_user,
                owner_group=owner_group,
                existing_ok=existing_ok,
                session=session,
            )

            proj_ds_orm = ProjectDatasetORM(project_id=project_id, dataset_id=ds_id)
            session.add(proj_ds_orm)
            return ds_id

    def get_dataset(
        self,
        project_id: int,
        dataset_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:

        with self.root_socket.optional_session(session, True) as session:
            self.assert_dataset_belongs(project_id, dataset_id, session=session)
            return self.root_socket.datasets.get(dataset_id, missing_ok=False, session=session)

    def unlink_datasets(
        self,
        project_id: int,
        dataset_ids: Iterable[int],
        delete_datasets: bool,
        delete_datasets_records: bool,
        *,
        session: Optional[Session] = None,
    ):

        stmt = delete(ProjectDatasetORM)
        stmt = stmt.where(ProjectDatasetORM.project_id == project_id)
        stmt = stmt.where(ProjectDatasetORM.dataset_id.in_(dataset_ids))
        stmt = stmt.returning(ProjectDatasetORM.dataset_id)

        with self.root_socket.optional_session(session, True) as session:
            ds_ids = session.execute(stmt).scalars().all()

            # Use ds_ids so we only delete datasets that were removed from this dataset
            if delete_datasets:
                for ds_id in ds_ids:
                    self.root_socket.datasets.delete(ds_id, delete_datasets_records, session=session)

    #################################
    # Records
    #################################

    def assert_record_belongs(self, project_id: int, record_id: int, *, session: Optional[Session] = None):

        stmt = select(ProjectRecordORM.record_id)
        stmt = stmt.where(ProjectRecordORM.project_id == project_id)
        stmt = stmt.where(ProjectRecordORM.record_id == record_id)

        with self.root_socket.optional_session(session, True) as session:
            r_id = session.execute(stmt).scalar_one_or_none()

            if r_id is None:
                raise MissingDataError(f"Record {record_id} not found in project {project_id}")

    def record_name_exists(self, project_id: int, record_name: str, *, session: Optional[Session] = None):
        stmt = select(ProjectRecordORM.record_id)
        stmt = stmt.where(ProjectRecordORM.project_id == project_id)
        stmt = stmt.where(ProjectRecordORM.lname == record_name.lower())

        with self.root_socket.optional_session(session, True) as session:
            r_id = session.execute(stmt).scalar_one_or_none()
            return r_id is not None

    def get_record_metadata(self, project_id: int, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        stmt = select(ProjectRecordORM, BaseRecordORM.record_type, BaseRecordORM.status)
        stmt = stmt.join(BaseRecordORM, BaseRecordORM.id == ProjectRecordORM.record_id)
        stmt = stmt.where(ProjectRecordORM.project_id == project_id)

        with self.root_socket.optional_session(session, True) as session:
            meta_info = session.execute(stmt).all()
            return [{"record_type": m[1], "status": m[2], **m[0].model_dict()} for m in meta_info]

    def get_record_ids(self, project_id: int, *, session: Optional[Session] = None) -> List[int]:
        stmt = select(ProjectRecordORM.record_id).where(ProjectRecordORM.project_id == project_id)

        with self.root_socket.optional_session(session, True) as session:
            rids = session.execute(stmt).scalars().all()
            return list(rids)

    def add_record(
        self,
        project_id: int,
        record_name: str,
        record_description: str,
        record_tags: List[str],
        record_input: AllInputTypes,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertCountsMetadata, int]:

        with self.root_socket.optional_session(session) as session:
            if self.record_name_exists(project_id, record_name, session=session):
                raise ValueError(f"Record '{record_name}' already exists in project {project_id}")

            meta, record_id = self.root_socket.records.add_from_input(
                record_input,
                compute_tag=compute_tag,
                compute_priority=compute_priority,
                owner_user=owner_user,
                owner_group=owner_group,
                find_existing=find_existing,
                session=session,
            )

            if meta.success:
                proj_rec_orm = ProjectRecordORM(
                    project_id=project_id,
                    record_id=record_id,
                    name=record_name,
                    description=record_description,
                    tags=record_tags,
                )
                session.add(proj_rec_orm)

            return meta, record_id

    def get_record(
        self,
        project_id: int,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:

        with self.root_socket.optional_session(session, True) as session:
            self.assert_record_belongs(project_id, record_id, session=session)
            return self.root_socket.records.get([record_id], missing_ok=False, session=session)[0]

    def unlink_records(
        self, project_id: int, record_ids: Iterable[int], delete_records: bool, *, session: Optional[Session] = None
    ):

        stmt = delete(ProjectRecordORM)
        stmt = stmt.where(ProjectRecordORM.project_id == project_id)
        stmt = stmt.where(ProjectRecordORM.record_id.in_(record_ids))
        stmt = stmt.returning(ProjectRecordORM.record_id)

        with self.root_socket.optional_session(session, True) as session:
            r_ids = session.execute(stmt).scalars().all()

            # Use r_ids so we only delete records that were removed from this dataset
            if delete_records:
                self.root_socket.records.delete(r_ids, False, session=session)

    #################################
    # Internal Jobs
    #################################

    def get_internal_job(
        self,
        project_id: int,
        job_id: int,
        *,
        session: Optional[Session] = None,
    ):
        stmt = select(InternalJobORM)
        stmt = stmt.join(ProjectInternalJobORM, ProjectInternalJobORM.internal_job_id == InternalJobORM.id)
        stmt = stmt.where(ProjectInternalJobORM.project_id == project_id)
        stmt = stmt.where(ProjectInternalJobORM.internal_job_id == job_id)

        with self.root_socket.optional_session(session, True) as session:
            ij_orm = session.execute(stmt).scalar_one_or_none()
            if ij_orm is None:
                raise MissingDataError(f"Job id {job_id} not found in project {project_id}")
            return ij_orm.model_dict()

    def list_internal_jobs(
        self,
        project_id: int,
        status: Optional[Iterable[InternalJobStatusEnum]] = None,
        *,
        session: Optional[Session] = None,
    ):
        stmt = select(InternalJobORM)
        stmt = stmt.join(ProjectInternalJobORM, ProjectInternalJobORM.internal_job_id == InternalJobORM.id)
        stmt = stmt.where(ProjectInternalJobORM.project_id == project_id)

        if status is not None:
            stmt = stmt.where(InternalJobORM.status.in_(status))

        with self.root_socket.optional_session(session, True) as session:
            ij_orm = session.execute(stmt).scalars().all()
            return [i.model_dict() for i in ij_orm]

    #################################
    # Attachments
    #################################
    def get_attachments(self, project_id: int, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get the attachments for a project
        """

        stmt = select(ProjectAttachmentORM)
        stmt = stmt.where(ProjectAttachmentORM.project_id == project_id)

        with self.root_socket.optional_session(session, True) as session:
            att = session.execute(stmt).scalars().all()
            return [x.model_dict() for x in att]

    def delete_attachment(self, project_id: int, file_id: int, *, session: Optional[Session] = None):
        stmt = select(ProjectAttachmentORM)
        stmt = stmt.where(ProjectAttachmentORM.project_id == project_id)
        stmt = stmt.where(ProjectAttachmentORM.id == file_id)
        stmt = stmt.with_for_update()

        with self.root_socket.optional_session(session) as session:
            att = session.execute(stmt).scalar_one_or_none()
            if att is None:
                raise MissingDataError(f"Attachment with file id {file_id} not found in project {project_id}")

            return self.root_socket.external_files.delete(file_id, session=session)

    def attach_file(
        self,
        project_id: int,
        attachment_type: ProjectAttachmentType,
        file_path: str,
        file_name: str,
        description: Optional[str],
        *,
        job_progress: Optional[JobProgress] = None,
        session: Optional[Session] = None,
    ) -> int:
        """
        Attach a file to a project

        This function uploads the specified file and associates it with the project by creating
        a corresponding project attachment record. This operation requires S3 storage to be enabled.

        Parameters
        ----------
        project_id
            The ID of the project to which the file will be attached.
        attachment_type
            The type of attachment that categorizes the file being added.
        file_path
            The local file system path to the file that needs to be uploaded.
        file_name
            The name of the file to be used in the attachment record. This is the filename that is
            recommended to the user by default.
        description
            An optional description of the file
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
            raise UserReportableError("S3 storage is not enabled. Can not attach file to a project")

        self._logger.info(f"Uploading/Attaching project-related file: {file_path}")
        with self.root_socket.optional_session(session) as session:
            ef = ProjectAttachmentORM(
                project_id=project_id,
                attachment_type=attachment_type,
                file_name=file_name,
                description=description,
            )

            file_id = self.root_socket.external_files.add_file(
                file_path, ef, session=session, job_progress=job_progress
            )

            self._logger.info(f"Project attachment {file_path} successfully uploaded to S3. ID is {file_id}")
            return file_id
