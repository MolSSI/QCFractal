from __future__ import annotations

from sqlalchemy import JSON, Column, Integer, ForeignKey, String, ForeignKeyConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM, SinglepointRecordORM
from qcfractal.db_socket import BaseORM


class SinglepointDatasetEntryORM(BaseORM):
    """Association table for many to many"""

    __tablename__ = "singlepoint_dataset_entry"

    dataset_id = Column(Integer, ForeignKey("singlepoint_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, nullable=False, primary_key=True)
    comment = Column(String)

    molecule_id = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    additional_keywords = Column(JSONB, nullable=False)
    attributes = Column(JSONB, nullable=False)

    local_results = Column(JSON)

    molecule = relationship(MoleculeORM, lazy="joined")

    __table_args__ = (
        Index("ix_singlepoint_dataset_entry_dataset_id", "dataset_id"),
        Index("ix_singlepoint_dataset_entry_name", "name"),
        Index("ix_singlepoint_dataset_entry_molecule_id", "molecule_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "molecule_id"]


class SinglepointDatasetSpecificationORM(BaseORM):
    __tablename__ = "singlepoint_dataset_specification"

    dataset_id = Column(Integer, ForeignKey("singlepoint_dataset.id", ondelete="cascade"), primary_key=True)
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)

    specification = relationship(QCSpecificationORM)

    __table_args__ = (
        Index("ix_singlepoint_dataset_specification_dataset_id", "dataset_id"),
        Index("ix_singlepoint_dataset_specification_name", "name"),
        Index("ix_singlepoint_dataset_specification_specification_id", "specification_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "specification_id"]


class SinglepointDatasetRecordItemORM(BaseORM):
    __tablename__ = "singlepoint_dataset_record"

    dataset_id = Column(Integer, ForeignKey("singlepoint_dataset.id", ondelete="cascade"), primary_key=True)
    entry_name = Column(String, primary_key=True)
    specification_name = Column(String, primary_key=True)
    record_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), nullable=False)

    record = relationship(SinglepointRecordORM)

    __table_args__ = (
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["singlepoint_dataset_entry.dataset_id", "singlepoint_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "specification_name"],
            ["singlepoint_dataset_specification.dataset_id", "singlepoint_dataset_specification.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        Index("ix_singlepoint_dataset_record_record_id", "record_id"),
    )

    _qcportal_model_excludes = ["dataset_id"]


class SinglepointDatasetORM(BaseDatasetORM):
    """
    The Dataset class for homogeneous computations on many molecules.
    """

    __tablename__ = "singlepoint_dataset"

    id = Column(Integer, ForeignKey(BaseDatasetORM.id, ondelete="cascade"), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "singlepoint",
    }
