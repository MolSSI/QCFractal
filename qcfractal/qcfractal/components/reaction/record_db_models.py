from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, String, Integer, ForeignKey, CheckConstraint, Index, UniqueConstraint, event, DDL
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.optimization.record_db_models import (
    OptimizationRecordORM,
    OptimizationSpecificationORM,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.record_db_models import SinglepointRecordORM, QCSpecificationORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class ReactionComponentORM(BaseORM):
    """
    Table for storing reaction specifications
    """

    __tablename__ = "reaction_component"

    reaction_id = Column(Integer, ForeignKey("reaction_record.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column(Integer, ForeignKey("molecule.id"), primary_key=True)
    coefficient = Column(DOUBLE_PRECISION, nullable=False)

    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), nullable=True)
    optimization_id = Column(Integer, ForeignKey(OptimizationRecordORM.id), nullable=True)

    molecule = relationship(MoleculeORM, lazy="selectin")
    singlepoint_record = relationship(SinglepointRecordORM)
    optimization_record = relationship(OptimizationRecordORM)

    __table_args__ = (
        Index("ix_reaction_component_singlepoint_id", "singlepoint_id"),
        Index("ix_reaction_component_optimization_id", "optimization_id"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "reaction_id")
        return BaseORM.model_dict(self, exclude)


class ReactionSpecificationORM(BaseORM):
    """
    Table for storing reaction specifications
    """

    __tablename__ = "reaction_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String, nullable=False)
    singlepoint_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=True)
    optimization_specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=True)
    keywords = Column(JSONB, nullable=False)
    keywords_hash = Column(String, nullable=False)

    singlepoint_specification = relationship(QCSpecificationORM, lazy="joined")
    optimization_specification = relationship(OptimizationSpecificationORM, lazy="joined")

    __table_args__ = (
        UniqueConstraint(
            "program",
            "singlepoint_specification_id",
            "optimization_specification_id",
            "keywords_hash",
            name="ux_reaction_specification_keys",
        ),
        Index("ix_reaction_specification_program", "program"),
        Index("ix_reaction_specification_singlepoint_specification_id", "singlepoint_specification_id"),
        Index("ix_reaction_specification_optimization_specification_id", "optimization_specification_id"),
        CheckConstraint("program = LOWER(program)", name="ck_reaction_specification_program_lower"),
        CheckConstraint(
            "singlepoint_specification_id IS NOT NULL OR optimization_specification_id IS NOT NULL",
            name="ck_reaction_specification_specs",
        ),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(
            exclude, "id", "keywords_hash", "singlepoint_specification_id", "optimization_specification_id"
        )
        return BaseORM.model_dict(self, exclude)

    @property
    def short_description(self) -> str:
        sp_desc = (
            self.singlepoint_specification.short_description
            if self.singlepoint_specification_id is not None
            else "(none)"
        )
        opt_desc = (
            self.optimization_specification.short_description
            if self.optimization_specification_id is not None
            else "(none)"
        )
        return f"{self.program}~[{sp_desc} | {opt_desc}]"


class ReactionRecordORM(BaseRecordORM):
    """
    Table for storing reaction calculations
    """

    __tablename__ = "reaction_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(ReactionSpecificationORM.id), nullable=False)
    specification = relationship(ReactionSpecificationORM, lazy="selectin")

    total_energy = Column(DOUBLE_PRECISION, nullable=True)

    components = relationship(ReactionComponentORM, cascade="all, delete-orphan", passive_deletes=True)

    __mapper_args__ = {
        "polymorphic_identity": "reaction",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

    @property
    def short_description(self) -> str:
        rxn_mols = ",".join(x.molecule.identifiers["molecular_formula"] for x in self.components)
        return f"[{rxn_mols}] {self.specification.short_description}"


# Delete base record if this record is deleted
_del_baserecord_trigger = DDL(
    """
    CREATE TRIGGER qca_reaction_record_delete_base_tr
    AFTER DELETE ON reaction_record
    FOR EACH ROW EXECUTE PROCEDURE qca_base_record_delete();
    """
)

event.listen(ReactionRecordORM.__table__, "after_create", _del_baserecord_trigger.execute_if(dialect=("postgresql")))
