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
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_keyed_dict

from qcfractal.components.auth.db_models import UserIDMapSubquery, GroupIDMapSubquery, UserORM, GroupORM
from qcfractal.db_socket import BaseORM, MsgpackExt


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

    group = Column(String(100), nullable=False)
    visibility = Column(Boolean, nullable=False)

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

    default_tag = Column(String, nullable=False)
    default_priority = Column(Integer, nullable=False)

    provenance = Column(JSON, nullable=False)

    # metadata is reserved in sqlalchemy
    meta = Column("metadata", JSON, nullable=False)

    extras = Column(JSON, nullable=False)

    contributed_values = relationship(
        "ContributedValuesORM",
        collection_class=attribute_keyed_dict("name"),
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

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # lname is only for the server
        # strip user/group ids
        exclude = self.append_exclude(exclude, "lname", "owner_user_id", "owner_group_id")

        d = BaseORM.model_dict(self, exclude)

        # meta -> metadata
        if "meta" in d:
            d["metadata"] = d.pop("meta")

        d["owner_user"] = self.owner_user.username if self.owner_user is not None else None
        d["owner_group"] = self.owner_group.groupname if self.owner_group is not None else None

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

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "dataset_id")
        return BaseORM.model_dict(self, exclude)
