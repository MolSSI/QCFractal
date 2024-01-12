from __future__ import annotations

from sqlalchemy import Column, Integer, ForeignKey, String, ForeignKeyConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.neb.record_db_models import NEBRecordORM, NEBSpecificationORM
from qcfractal.db_socket import BaseORM


class NEBDatasetInitialMoleculeORM(BaseORM):
    """
    Association table neb -> initial chain
    """

    __tablename__ = "neb_dataset_molecule"

    dataset_id = Column(Integer, primary_key=True)
    entry_name = Column(String, primary_key=True)
    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), primary_key=True)
    position = Column(Integer, primary_key=True)

    # molecule = relationship(MoleculeORM)

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

    _qcportal_model_excludes = ["dataset_id", "molecule_id", "entry_name", "position"]


class NEBDatasetEntryORM(BaseORM):
    __tablename__ = "neb_dataset_entry"

    dataset_id = Column(Integer, ForeignKey("neb_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, primary_key=True)
    comment = Column(String)

    additional_keywords = Column(JSONB, nullable=False)
    additional_singlepoint_keywords = Column(JSONB, nullable=False)
    attributes = Column(JSONB, nullable=False)

    # initial_molecule_ids = column_property(
    #   select(array_agg(NEBDatasetInitialMoleculeORM.molecule_id))
    #   .where(NEBDatasetInitialMoleculeORM.dataset_id == dataset_id)
    #   .where(NEBDatasetInitialMoleculeORM.entry_name == name)
    #   .scalar_subquery()
    # )

    initial_chain = relationship(
        MoleculeORM,
        secondary=NEBDatasetInitialMoleculeORM.__table__,
        order_by=NEBDatasetInitialMoleculeORM.__table__.c.position,
        viewonly=True,
        lazy="selectin",
    )

    initial_chain_assoc = relationship(NEBDatasetInitialMoleculeORM, cascade="all, delete-orphan", passive_deletes=True)

    __table_args__ = (
        Index("ix_neb_dataset_entry_dataset_id", "dataset_id"),
        Index("ix_neb_dataset_entry_name", "name"),
    )

    _qcportal_model_excludes = ["dataset_id"]


class NEBDatasetSpecificationORM(BaseORM):
    __tablename__ = "neb_dataset_specification"

    dataset_id = Column(Integer, ForeignKey("neb_dataset.id", ondelete="cascade"), primary_key=True)
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    specification_id = Column(Integer, ForeignKey(NEBSpecificationORM.id), nullable=False)

    specification = relationship(NEBSpecificationORM)

    __table_args__ = (
        Index("ix_neb_dataset_specification_dataset_id", "dataset_id"),
        Index("ix_neb_dataset_specification_name", "name"),
        Index("ix_neb_dataset_specification_specification_id", "specification_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "specification_id"]


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
    )

    _qcportal_model_excludes = ["dataset_id"]


class NEBDatasetORM(BaseDatasetORM):
    __tablename__ = "neb_dataset"

    id = Column(Integer, ForeignKey(BaseDatasetORM.id, ondelete="cascade"), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "neb",
    }
