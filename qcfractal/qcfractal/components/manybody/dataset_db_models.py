from __future__ import annotations

from sqlalchemy import Column, Integer, ForeignKey, String, ForeignKeyConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.manybody.record_db_models import ManybodySpecificationORM, ManybodyRecordORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.db_socket import BaseORM


class ManybodyDatasetEntryORM(BaseORM):
    __tablename__ = "manybody_dataset_entry"

    dataset_id = Column(Integer, ForeignKey("manybody_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, primary_key=True)
    comment = Column(String)

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    additional_singlepoint_keywords = Column(JSONB, nullable=False)
    attributes = Column(JSONB, nullable=False)

    initial_molecule = relationship(MoleculeORM, lazy="joined")

    __table_args__ = (
        Index("ix_manybody_dataset_entry_dataset_id", "dataset_id"),
        Index("ix_manybody_dataset_entry_name", "name"),
        Index("ix_manybody_dataset_entry_initial_molecule_id", "initial_molecule_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "initial_molecule_id"]


class ManybodyDatasetSpecificationORM(BaseORM):
    __tablename__ = "manybody_dataset_specification"

    dataset_id = Column(Integer, ForeignKey("manybody_dataset.id", ondelete="cascade"), primary_key=True)
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    specification_id = Column(Integer, ForeignKey(ManybodySpecificationORM.id), nullable=False)

    specification = relationship(ManybodySpecificationORM)

    __table_args__ = (
        Index("ix_manybody_dataset_specification_dataset_id", "dataset_id"),
        Index("ix_manybody_dataset_specification_name", "name"),
        Index("ix_manybody_dataset_specification_specification_id", "specification_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "specification_id"]


class ManybodyDatasetRecordItemORM(BaseORM):
    __tablename__ = "manybody_dataset_record"

    dataset_id = Column(Integer, ForeignKey("manybody_dataset.id", ondelete="cascade"), primary_key=True)
    entry_name = Column(String, primary_key=True)
    specification_name = Column(String, primary_key=True)
    record_id = Column(Integer, ForeignKey(ManybodyRecordORM.id), nullable=False)

    record = relationship(ManybodyRecordORM)

    __table_args__ = (
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["manybody_dataset_entry.dataset_id", "manybody_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "specification_name"],
            ["manybody_dataset_specification.dataset_id", "manybody_dataset_specification.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        Index("ix_manybody_dataset_record_record_id", "record_id"),
    )

    _qcportal_model_excludes = ["dataset_id"]


class ManybodyDatasetORM(BaseDatasetORM):
    __tablename__ = "manybody_dataset"

    id = Column(Integer, ForeignKey(BaseDatasetORM.id, ondelete="cascade"), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "manybody",
    }
