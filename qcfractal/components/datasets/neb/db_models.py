from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select, Column, Integer, ForeignKey, String, ForeignKeyConstraint, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB, array_agg
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.datasets.db_models import CollectionORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.singlepoint.db_models import QCSpecificationORM
from qcfractal.components.records.neb.db_models import NEBRecordORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class NEBDatasetMoleculeORM(BaseORM):
    """
    Association table neb -> initial molecules
    """

    __tablename__ = "neb_dataset_molecule"

    dataset_id = Column(Integer, primary_key=True)
    entry_name = Column(String, primary_key=True)
    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), primary_key=True)

    molecule = relationship(MoleculeORM)

    __table_args__ = (
        Index("ix_neb_dataset_molecule_dataset_id", "dataset_id"),
        Index("ix_neb_dataset_molecule_entry_name", "entry_name"),
        Index("ix_neb_dataset_molecule_molecule_id", "molecule_id"),
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["neb_dataset_entry.dataset_id", "neb_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "dataset_id")
        return BaseORM.model_dict(self, exclude)


class NEBDatasetEntryORM(BaseORM):
    __tablename__ = "neb_dataset_entry"

    dataset_id = Column(Integer, ForeignKey("neb_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, primary_key=True)
    comment = Column(String)

    neb_keywords = Column(JSONB, nullable=False)
    additional_keywords = Column(JSONB, nullable=False)
    attributes = Column(JSONB, nullable=False)

    initial_molecule_ids = column_property(
        select(array_agg(NEBDatasetMoleculeORM.molecule_id))
        .where(NEBDatasetMoleculeORM.dataset_id == dataset_id)
        .where(NEBDatasetMoleculeORM.entry_name == name)
        .scalar_subquery()
    )

    molecules = relationship(NEBDatasetMoleculeORM)

    __table_args__ = (
        Index("ix_neb_dataset_entry_dataset_id", "dataset_id"),
        Index("ix_neb_dataset_entry_name", "name"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "dataset_id")
        return BaseORM.model_dict(self, exclude)


class NEBDatasetSpecificationORM(BaseORM):
    __tablename__ = "neb_dataset_specification"

    dataset_id = Column(Integer, ForeignKey("neb_dataset.id", ondelete="cascade"), primary_key=True)
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)

    specification = relationship(QCSpecificationORM)

    __table_args__ = (
        Index("ix_neb_dataset_specification_dataset_id", "dataset_id"),
        Index("ix_neb_dataset_specification_name", "name"),
        Index("ix_neb_dataset_specification_specification_id", "specification_id"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "dataset_id", "specification_id")
        return BaseORM.model_dict(self, exclude)


class NEBDatasetRecordItemORM(BaseORM):
    __tablename__ = "neb_dataset_record"

    dataset_id = Column(Integer, ForeignKey("neb_dataset.id", ondelete="cascade"), primary_key=True)
    entry_name = Column(String, primary_key=True)
    specification_name = Column(String, primary_key=True)
    record_id = Column(Integer, ForeignKey(NEBRecordORM.id), nullable=False)

    record = relationship(NEBRecordORM)

    __table_args__ = (
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["neb_dataset_entry.dataset_id", "neb_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "specification_name"],
            ["neb_dataset_specification.dataset_id", "neb_dataset_specification.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        Index("ix_neb_dataset_record_record_id", "record_id"),
        UniqueConstraint(
            "dataset_id", "entry_name", "specification_name", name="ux_neb_dataset_record_unique"
        ),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "dataset_id")
        return BaseORM.model_dict(self, exclude)


class NEBDatasetORM(CollectionORM):
    __tablename__ = "neb_dataset"

    id = Column(Integer, ForeignKey(CollectionORM.id, ondelete="cascade"), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "neb",
    }
