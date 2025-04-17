from __future__ import annotations

from typing import Optional, Iterable, Dict, Any

from sqlalchemy import (
    select,
    union,
    Column,
    Integer,
    String,
    JSON,
    Index,
    Computed,
    ForeignKey,
    UniqueConstraint,
    Enum,
)
from sqlalchemy.orm import relationship

from qcfractal.components.auth.db_models import UserIDMapSubquery, UserORM
from qcfractal.components.external_files.db_models import ExternalFileORM
from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.db_socket import BaseORM
from qcportal.project_models import ProjectAttachmentType
from qcfractal.db_socket.db_views import view
from qcfractal.components.dataset_db_views import DatasetDirectRecordsView
from qcfractal.components.record_db_views import RecordChildrenView


class ProjectORM(BaseORM):
    """
    Table/ORM for a QCArchive project
    """

    __tablename__ = "project"

    id = Column(Integer, primary_key=True)

    name = Column(String(100), nullable=False)
    lname = Column(String(100), Computed("LOWER(name)"), nullable=False)

    description = Column(String, nullable=False)
    tagline = Column(String, nullable=False)
    tags = Column(JSON, nullable=False)

    default_compute_tag = Column(String, nullable=False)
    default_compute_priority = Column(Integer, nullable=False)

    extras = Column(JSON, nullable=False)

    # Ownership of this project
    owner_user_id = Column(Integer, ForeignKey(UserORM.id), nullable=True)

    owner_user = relationship(
        UserIDMapSubquery,
        foreign_keys=[owner_user_id],
        primaryjoin="ProjectORM.owner_user_id == UserIDMapSubquery.id",
        lazy="selectin",
        viewonly=True,
    )

    attachments = relationship(
        "ProjectAttachmentORM",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint("lname", name="ux_project_project_type_lname"),
        Index("ix_project_owner_user_id", "owner_user_id"),
    )

    _qcportal_model_excludes = ["lname", "owner_user_id"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        d["owner_user"] = self.owner_user.username if self.owner_user is not None else None

        return d


class ProjectMoleculeORM(BaseORM):
    __tablename__ = "project_molecule"

    project_id = Column(Integer, ForeignKey("project.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column(Integer, ForeignKey("molecule.id"), nullable=False)

    name = Column(String, nullable=False)
    lname = Column(String(100), Computed("LOWER(name)"), primary_key=True)
    description = Column(String, nullable=False)
    tags = Column(JSON, nullable=False)

    __table_args__ = (Index("ix_project_molecule_molecule_id", "molecule_id"),)


class ProjectRecordORM(BaseORM):
    __tablename__ = "project_record"

    project_id = Column(Integer, ForeignKey("project.id", ondelete="cascade"), primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id"), nullable=False)

    name = Column(String, nullable=False)
    lname = Column(String(100), Computed("LOWER(name)"), primary_key=True)
    description = Column(String, nullable=False)
    tags = Column(JSON, nullable=False)

    __table_args__ = (Index("ix_project_record_record_id", "record_id"),)


class ProjectDatasetORM(BaseORM):
    __tablename__ = "project_dataset"

    project_id = Column(Integer, ForeignKey("project.id", ondelete="cascade"), primary_key=True)
    dataset_id = Column(Integer, ForeignKey("base_dataset.id"), primary_key=True)

    __table_args__ = (Index("ix_project_dataset_dataset_id", "dataset_id"),)


class ProjectInternalJobORM(BaseORM):
    __tablename__ = "project_internal_job"

    internal_job_id = Column(Integer, ForeignKey(InternalJobORM.id, ondelete="cascade"), primary_key=True)
    project_id = Column(Integer, ForeignKey("project.id", ondelete="cascade"), primary_key=True)


class ProjectAttachmentORM(ExternalFileORM):
    __tablename__ = "project_attachment"

    id = Column(Integer, ForeignKey(ExternalFileORM.id, ondelete="cascade"), primary_key=True)
    project_id = Column(Integer, ForeignKey("project.id", ondelete="cascade"), nullable=False)

    attachment_type = Column(Enum(ProjectAttachmentType), nullable=False)
    tags = Column(JSON, nullable=False)

    __mapper_args__ = {"polymorphic_identity": "project_attachment"}

    __table_args__ = (Index("ix_project_attachment_project_id", "project_id"),)

    _qcportal_model_excludes = ["project_id"]


ProjectRecordsView = view(
    "project_records_view",
    BaseORM.metadata,
    union(
        select(ProjectRecordORM.project_id.label("project_id"), ProjectRecordORM.record_id.label("record_id")),
        select(ProjectRecordORM.project_id.label("project_id"), RecordChildrenView.c.child_id.label("record_id")).join(
            RecordChildrenView, ProjectRecordORM.record_id == RecordChildrenView.c.parent_id
        ),
        select(
            ProjectDatasetORM.project_id.label("project_id"), DatasetDirectRecordsView.c.record_id.label("record_id")
        ).join(DatasetDirectRecordsView, ProjectDatasetORM.dataset_id == DatasetDirectRecordsView.c.dataset_id),
    ),
)
