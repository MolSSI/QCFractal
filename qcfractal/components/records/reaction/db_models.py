from __future__ import annotations

from typing import Dict, Optional

from sqlalchemy import select, Column, Integer, ForeignKey, ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.singlepoint.db_models import SinglepointRecordORM, QCSpecificationORM
from qcfractal.db_socket import BaseORM


class ReactionStoichiometriesORM(BaseORM):

    __tablename__ = "reaction_stoichiometries"

    reaction_id = Column(Integer, ForeignKey("reaction_record.id", ondelete="cascade"), primary_key=True)

    molecule_id = Column(Integer, ForeignKey("molecule.id"), primary_key=True)
    coefficient = Column(DOUBLE_PRECISION, nullable=False)

    molecule = relationship(MoleculeORM)


class ReactionComponentsORM(BaseORM):
    __tablename__ = "reaction_components"

    reaction_id = Column(Integer, ForeignKey("reaction_record.id", ondelete="cascade"), primary_key=True)

    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), primary_key=True)

    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), nullable=False)

    energy = column_property(
        select(SinglepointRecordORM.properties["return_energy"].astext.cast(DOUBLE_PRECISION))
        .where(SinglepointRecordORM.id == singlepoint_id)
        .scalar_subquery()
    )

    molecule = relationship(MoleculeORM)
    singlepoint_record = relationship(SinglepointRecordORM)

    __table_args__ = (
        ForeignKeyConstraint(
            ["reaction_id", "molecule_id"],
            ["reaction_stoichiometries.reaction_id", "reaction_stoichiometries.molecule_id"],
            ondelete="cascade",
        ),
    )


class ReactionRecordORM(BaseRecordORM):

    __tablename__ = "reaction_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    specification = relationship(QCSpecificationORM, lazy="selectin")

    total_energy = Column(DOUBLE_PRECISION, nullable=True)

    components = relationship(
        ReactionComponentsORM,
        cascade="all, delete-orphan",
    )

    stoichiometries = relationship(
        ReactionStoichiometriesORM,
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "reaction",
    }

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
