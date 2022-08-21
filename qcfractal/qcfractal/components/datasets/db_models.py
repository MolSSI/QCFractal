from __future__ import annotations

from typing import Optional, Iterable, Dict, Any

from sqlalchemy import Column, Integer, String, JSON, Boolean, Index, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from qcfractal.db_socket import BaseORM, MsgpackExt


class BaseDatasetORM(BaseORM):
    """
    A base class for all dataset ORM
    """

    __tablename__ = "base_dataset"

    id = Column(Integer, primary_key=True)
    dataset_type = Column(String, nullable=False)

    lname = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)

    tags = Column(JSON, nullable=False)
    tagline = Column(String, nullable=False)
    description = Column(String, nullable=False)

    group = Column(String(100), nullable=False)
    visibility = Column(Boolean, nullable=False)

    default_tag = Column(String, nullable=False)
    default_priority = Column(Integer, nullable=False)

    provenance = Column(JSON, nullable=False)

    # metadata is reserved in sqlalchemy
    meta = Column("metadata", JSON, nullable=False)

    extras = Column(JSON, nullable=False)

    contributed_values = relationship(
        "ContributedValuesORM", collection_class=attribute_mapped_collection("name"), cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("dataset_type", "lname", name="uix_dataset_type_lname"),
        Index("ix_dataset_type", "dataset_type"),
    )

    __mapper_args__ = {"polymorphic_on": "dataset_type"}

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # lname is only for the server
        exclude = self.append_exclude(exclude, "lname")

        d = BaseORM.model_dict(self, exclude)

        # meta -> metadata
        if "meta" in d:
            d["metadata"] = d.pop("meta")

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

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "dataset_id")
        return BaseORM.model_dict(self, exclude)
