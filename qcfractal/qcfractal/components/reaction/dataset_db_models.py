from __future__ import annotations

from sqlalchemy import Column, Integer, ForeignKey, String, ForeignKeyConstraint, Index
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.reaction.record_db_models import ReactionRecordORM, ReactionSpecificationORM
from qcfractal.db_socket import BaseORM


class ReactionDatasetStoichiometryORM(BaseORM):
    __tablename__ = "reaction_dataset_stoichiometry"

    dataset_id = Column(Integer, ForeignKey("reaction_dataset.id", ondelete="cascade"), primary_key=True)
    entry_name = Column(String, primary_key=True)

    molecule_id = Column(Integer, ForeignKey("molecule.id"), primary_key=True)
    coefficient = Column(DOUBLE_PRECISION, nullable=False)

    molecule = relationship(MoleculeORM, lazy="joined")

    __table_args__ = (
        Index("ix_reaction_dataset_stoichiometry_dataset_id", "dataset_id"),
        Index("ix_reaction_dataset_stoichiometry_entry_name", "entry_name"),
        Index("ix_reaction_dataset_stoichiometry_molecule_id", "molecule_id"),
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["reaction_dataset_entry.dataset_id", "reaction_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
    )

    _qcportal_model_excludes = ["dataset_id", "molecule_id"]


class ReactionDatasetEntryORM(BaseORM):
    __tablename__ = "reaction_dataset_entry"

    dataset_id = Column(Integer, ForeignKey("reaction_dataset.id", ondelete="cascade"), primary_key=True)

    name = Column(String, primary_key=True)
    comment = Column(String)

    additional_keywords = Column(JSONB, nullable=True)
    attributes = Column(JSONB, nullable=False)

    stoichiometries = relationship(
        ReactionDatasetStoichiometryORM, lazy="selectin", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        Index("ix_reaction_dataset_entry_dataset_id", "dataset_id"),
        Index("ix_reaction_dataset_entry_name", "name"),
    )

    _qcportal_model_excludes = ["dataset_id"]


class ReactionDatasetSpecificationORM(BaseORM):
    __tablename__ = "reaction_dataset_specification"

    dataset_id = Column(Integer, ForeignKey("reaction_dataset.id", ondelete="cascade"), primary_key=True)
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    specification_id = Column(Integer, ForeignKey(ReactionSpecificationORM.id), nullable=False)

    specification = relationship(ReactionSpecificationORM, uselist=False)

    __table_args__ = (
        Index("ix_reaction_dataset_specification_dataset_id", "dataset_id"),
        Index("ix_reaction_dataset_specification_name", "name"),
        Index("ix_reaction_dataset_specification_specification_id", "specification_id"),
    )

    _qcportal_model_excludes = ["dataset_id", "specification_id"]


class ReactionDatasetRecordItemORM(BaseORM):
    __tablename__ = "reaction_dataset_record"

    dataset_id = Column(Integer, ForeignKey("reaction_dataset.id", ondelete="cascade"), primary_key=True)
    entry_name = Column(String, primary_key=True)
    specification_name = Column(String, primary_key=True)
    record_id = Column(Integer, ForeignKey(ReactionRecordORM.id), nullable=False)

    record = relationship(ReactionRecordORM)

    __table_args__ = (
        ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["reaction_dataset_entry.dataset_id", "reaction_dataset_entry.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "specification_name"],
            ["reaction_dataset_specification.dataset_id", "reaction_dataset_specification.name"],
            ondelete="cascade",
            onupdate="cascade",
        ),
        Index("ix_reaction_dataset_record_record_id", "record_id"),
    )

    _qcportal_model_excludes = ["dataset_id"]


class ReactionDatasetORM(BaseDatasetORM):
    __tablename__ = "reaction_dataset"

    id = Column(Integer, ForeignKey(BaseDatasetORM.id, ondelete="cascade"), primary_key=True)

    __mapper_args__ = {
        "polymorphic_identity": "reaction",
    }
