from __future__ import annotations

from typing import Optional, Iterable, Dict, Any

from sqlalchemy import (
    Column,
    Integer,
    String,
    JSON,
    Boolean,
    Index,
    Computed,
    ForeignKey,
    ForeignKeyConstraint,
    UniqueConstraint,
    Enum,
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_keyed_dict

from qcfractal.components.auth.db_models import UserIDMapSubquery, GroupIDMapSubquery, UserORM, GroupORM
from qcfractal.components.external_files.db_models import ExternalFileORM
from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.db_socket import BaseORM, MsgpackExt
from qcportal.dataset_models import DatasetAttachmentType


class BaseDatasetORM(BaseORM):
    """
    A base class for all dataset ORM
    """

    __tablename__ = "base_dataset"

    id = Column(Integer, primary_key=True)
    dataset_type = Column(String, nullable=False)

    lname = Column(String(100), Computed("LOWER(name)"), nullable=False)
    name = Column(String(100), nullable=False)

    tags = Column(JSON, nullable=False)
    tagline = Column(String, nullable=False)
    description = Column(String, nullable=False)

    # Ownership of this dataset
    owner_user_id = Column(Integer, ForeignKey(UserORM.id), nullable=True)
    owner_group_id = Column(Integer, ForeignKey(GroupORM.id), nullable=True)

    owner_user = relationship(
        UserIDMapSubquery,
        foreign_keys=[owner_user_id],
        primaryjoin="BaseDatasetORM.owner_user_id == UserIDMapSubquery.id",
        lazy="selectin",
        viewonly=True,
    )

    owner_group = relationship(
        GroupIDMapSubquery,
        foreign_keys=[owner_group_id],
        primaryjoin="BaseDatasetORM.owner_group_id == GroupIDMapSubquery.id",
        lazy="selectin",
        viewonly=True,
    )

    default_compute_tag = Column(String, nullable=False)
    default_compute_priority = Column(Integer, nullable=False)

    provenance = Column(JSON, nullable=False)
    extras = Column("extras", JSON, nullable=False)

    contributed_values = relationship(
        "ContributedValuesORM",
        collection_class=attribute_keyed_dict("name"),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    attachments = relationship(
        "DatasetAttachmentORM",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint("dataset_type", "lname", name="ux_base_dataset_dataset_type_lname"),
        Index("ix_base_dataset_dataset_type", "dataset_type"),
        Index("ix_base_dataset_owner_user_id", "owner_user_id"),
        Index("ix_base_dataset_owner_group_id", "owner_group_id"),
        ForeignKeyConstraint(
            ["owner_user_id", "owner_group_id"],
            ["user_groups.user_id", "user_groups.group_id"],
        ),
    )

    __mapper_args__ = {"polymorphic_on": "dataset_type"}

    _qcportal_model_excludes = ["lname", "owner_user_id", "owner_group_id"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        d["owner_user"] = self.owner_user.username if self.owner_user is not None else None
        d["owner_group"] = self.owner_group.groupname if self.owner_group is not None else None

        # TODO - DEPRECATED - REMOVE EVENTUALLY
        d["group"] = "default"
        d["visibility"] = True
        if "extras" in d:
            d["metadata"] = d.pop("extras")
            d["extras"] = {}
        if "default_compute_tag" in d:
            d["default_tag"] = d.pop("default_compute_tag")
        if "default_compute_priority" in d:
            d["default_priority"] = d.pop("default_compute_priority")

        return d


class ContributedValuesORM(BaseORM):
    __tablename__ = "contributed_values"

    dataset_id = Column(Integer, ForeignKey("base_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, nullable=False, primary_key=True)
    values = Column(MsgpackExt, nullable=False)
    index = Column(MsgpackExt, nullable=False)
    values_structure = Column(JSON, nullable=False)

    theory_level = Column(JSON, nullable=False)
    units = Column(String, nullable=False)
    theory_level_details = Column(JSON)

    citations = Column(JSON)
    external_url = Column(String)
    doi = Column(String)

    comments = Column(String)

    __table_args__ = (Index("ix_contributed_values_dataset_id", "dataset_id"),)

    _qcportal_model_excludes = ["dataset_id"]


class DatasetInternalJobORM(BaseORM):
    __tablename__ = "dataset_internal_job"

    internal_job_id = Column(Integer, ForeignKey(InternalJobORM.id, ondelete="cascade"), primary_key=True)
    dataset_id = Column(Integer, ForeignKey("base_dataset.id", ondelete="cascade"), primary_key=True)


class DatasetAttachmentORM(ExternalFileORM):
    __tablename__ = "dataset_attachment"

    id = Column(Integer, ForeignKey(ExternalFileORM.id, ondelete="cascade"), primary_key=True)
    dataset_id = Column(Integer, ForeignKey("base_dataset.id", ondelete="cascade"), nullable=False)

    attachment_type = Column(Enum(DatasetAttachmentType), nullable=False)

    __mapper_args__ = {"polymorphic_identity": "dataset_attachment"}

    __table_args__ = (Index("ix_dataset_attachment_dataset_id", "dataset_id"),)

    _qcportal_model_excludes = ["dataset_id"]
