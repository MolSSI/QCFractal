from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select, Column, Integer, ForeignKey, ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.singlepoint.db_models import SinglepointRecordORM, QCSpecificationORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class ReactionStoichiometryORM(BaseORM):
    """
    Table for storing the molecules and coefficients of a reaction (stoichiometry)
    """

    __tablename__ = "reaction_stoichiometry"

    reaction_id = Column(Integer, ForeignKey("reaction_record.id", ondelete="cascade"), primary_key=True)

    molecule_id = Column(Integer, ForeignKey("molecule.id"), primary_key=True)
    coefficient = Column(DOUBLE_PRECISION, nullable=False)

    molecule = relationship(MoleculeORM)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "reaction_id")
        return BaseORM.model_dict(self, exclude)


class ReactionComponentORM(BaseORM):
    """
    Table for storing reaction specifications
    """

    __tablename__ = "reaction_component"

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
            ["reaction_stoichiometry.reaction_id", "reaction_stoichiometry.molecule_id"],
            ondelete="cascade",
        ),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "reaction_id")
        return BaseORM.model_dict(self, exclude)


class ReactionRecordORM(BaseRecordORM):
    """
    Table for storing reaction calculations
    """

    __tablename__ = "reaction_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    specification = relationship(QCSpecificationORM, lazy="selectin")

    total_energy = Column(DOUBLE_PRECISION, nullable=True)

    components = relationship(
        ReactionComponentORM,
        cascade="all, delete-orphan",
    )

    stoichiometries = relationship(
        ReactionStoichiometryORM,
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "reaction",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
