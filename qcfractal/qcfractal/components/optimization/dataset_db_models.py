from __future__ import annotations

from sqlalchemy import Column, Integer, ForeignKey, String, ForeignKeyConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.optimization.record_db_models import (
    OptimizationSpecificationORM,
    OptimizationRecordORM,
)
from qcfractal.db_socket import BaseORM


class OptimizationDatasetEntryORM(BaseORM):
    __tablename__ = "optimization_dataset_entry"

    dataset_id = Column(Integer, ForeignKey("optimization_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, primary_key=True)
    comment = Column(String)

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    additional_keywords = Column(JSONB, nullable=False)
    attributes = Column(JSONB, nullable=False)

    initial_molecule = relationship(MoleculeORM, lazy="joined")

    __table_args__ = (
        Index("ix_optimization_dataset_entry_dataset_id", "dataset_id"),
        Index("ix_optimization_dataset_entry_name", "name"),
        Index("ix_optimization_dataset_entry_initial_molecule_id", "initial_molecule_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "initial_molecule_id"]


class OptimizationDatasetSpecificationORM(BaseORM):
    __tablename__ = "optimization_dataset_specification"

    dataset_id = Column(Integer, ForeignKey("optimization_dataset.id", ondelete="cascade"), primary_key=True)
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=False)

    specification = relationship(OptimizationSpecificationORM)

    __table_args__ = (
        Index("ix_optimization_dataset_specification_dataset_id", "dataset_id"),
        Index("ix_optimization_dataset_specification_name", "name"),
        Index("ix_optimization_dataset_specification_specification_id", "specification_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "specification_id"]


class OptimizationDatasetRecordItemORM(BaseORM):
    __tablename__ = "optimization_dataset_record"

    dataset_id = Column(Integer, ForeignKey("optimization_dataset.id", ondelete="cascade"), primary_key=True)
    entry_name = Column(String, primary_key=True)
    specification_name = Column(String, primary_key=True)
    record_id = Column(Integer, ForeignKey(OptimizationRecordORM.id), nullable=False)

    record = relationship(OptimizationRecordORM)

    __table_args__ = (
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["optimization_dataset_entry.dataset_id", "optimization_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "specification_name"],
            ["optimization_dataset_specification.dataset_id", "optimization_dataset_specification.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        Index("ix_optimization_dataset_record_record_id", "record_id"),
    )

    _qcportal_model_excludes = ["dataset_id"]


class OptimizationDatasetORM(BaseDatasetORM):
    __tablename__ = "optimization_dataset"

    id = Column(Integer, ForeignKey(BaseDatasetORM.id, ondelete="cascade"), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "optimization",
    }
