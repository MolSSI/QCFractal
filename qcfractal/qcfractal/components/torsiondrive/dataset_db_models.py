from __future__ import annotations

from sqlalchemy import select, Column, Integer, ForeignKey, String, ForeignKeyConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB, array_agg
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.torsiondrive.record_db_models import (
    TorsiondriveRecordORM,
    TorsiondriveSpecificationORM,
)
from qcfractal.db_socket import BaseORM


class TorsiondriveDatasetMoleculeORM(BaseORM):
    """
    Association table torsiondrive -> initial molecules
    """

    __tablename__ = "torsiondrive_dataset_molecule"

    dataset_id = Column(Integer, primary_key=True)
    entry_name = Column(String, primary_key=True)
    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), primary_key=True)

    __table_args__ = (
        Index("ix_torsiondrive_dataset_molecule_dataset_id", "dataset_id"),
        Index("ix_torsiondrive_dataset_molecule_entry_name", "entry_name"),
        Index("ix_torsiondrive_dataset_molecule_molecule_id", "molecule_id"),
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["torsiondrive_dataset_entry.dataset_id", "torsiondrive_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
    )


class TorsiondriveDatasetEntryORM(BaseORM):
    __tablename__ = "torsiondrive_dataset_entry"

    dataset_id = Column(Integer, ForeignKey("torsiondrive_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, primary_key=True)
    comment = Column(String)

    additional_keywords = Column(JSONB, nullable=False)
    additional_optimization_keywords = Column(JSONB, nullable=False)
    attributes = Column(JSONB, nullable=False)

    # Mark as deferred. We generally don't want to load it
    initial_molecule_ids = column_property(
        select(array_agg(TorsiondriveDatasetMoleculeORM.molecule_id))
        .where(TorsiondriveDatasetMoleculeORM.dataset_id == dataset_id)
        .where(TorsiondriveDatasetMoleculeORM.entry_name == name)
        .scalar_subquery(),
        deferred=True,
    )

    initial_molecules = relationship(
        MoleculeORM, secondary=TorsiondriveDatasetMoleculeORM.__tablename__, lazy="selectin", viewonly=True
    )

    initial_molecules_assoc = relationship(
        TorsiondriveDatasetMoleculeORM, cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        Index("ix_torsiondrive_dataset_entry_dataset_id", "dataset_id"),
        Index("ix_torsiondrive_dataset_entry_name", "name"),
    )

    _qcportal_model_excludes = ["dataset_id", "initial_molecules_assoc", "initial_molecules_id"]


class TorsiondriveDatasetSpecificationORM(BaseORM):
    __tablename__ = "torsiondrive_dataset_specification"

    dataset_id = Column(Integer, ForeignKey("torsiondrive_dataset.id", ondelete="cascade"), primary_key=True)
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    specification_id = Column(Integer, ForeignKey(TorsiondriveSpecificationORM.id), nullable=False)

    specification = relationship(TorsiondriveSpecificationORM)

    __table_args__ = (
        Index("ix_torsiondrive_dataset_specification_dataset_id", "dataset_id"),
        Index("ix_torsiondrive_dataset_specification_name", "name"),
        Index("ix_torsiondrive_dataset_specification_specification_id", "specification_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "specification_id"]


class TorsiondriveDatasetRecordItemORM(BaseORM):
    __tablename__ = "torsiondrive_dataset_record"

    dataset_id = Column(Integer, ForeignKey("torsiondrive_dataset.id", ondelete="cascade"), primary_key=True)
    entry_name = Column(String, primary_key=True)
    specification_name = Column(String, primary_key=True)
    record_id = Column(Integer, ForeignKey(TorsiondriveRecordORM.id), nullable=False)

    record = relationship(TorsiondriveRecordORM)

    __table_args__ = (
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["torsiondrive_dataset_entry.dataset_id", "torsiondrive_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "specification_name"],
            ["torsiondrive_dataset_specification.dataset_id", "torsiondrive_dataset_specification.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        Index("ix_torsiondrive_dataset_record_record_id", "record_id"),
    )

    _qcportal_model_excludes = ["dataset_id"]


class TorsiondriveDatasetORM(BaseDatasetORM):
    __tablename__ = "torsiondrive_dataset"

    id = Column(Integer, ForeignKey(BaseDatasetORM.id, ondelete="cascade"), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "torsiondrive",
    }
