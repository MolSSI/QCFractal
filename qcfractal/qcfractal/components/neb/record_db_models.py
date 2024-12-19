from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
    UniqueConstraint,
    CheckConstraint,
    Index,
    Boolean,
    event,
    DDL,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.optimization.record_db_models import OptimizationRecordORM, OptimizationSpecificationORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM, SinglepointRecordORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class NEBOptimizationsORM(BaseORM):
    __tablename__ = "neb_optimizations"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
    optimization_id = Column(Integer, ForeignKey(OptimizationRecordORM.id), primary_key=True)
    position = Column(Integer, primary_key=True)
    ts = Column(Boolean, primary_key=True)
    optimization_record = relationship(OptimizationRecordORM)

    __table_args__ = (Index("ix_neb_optimizations_optimization_id", "optimization_id"),)

    _qcportal_model_excludes = ["neb_id"]


class NEBSinglepointsORM(BaseORM):
    __tablename__ = "neb_singlepoints"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), primary_key=True)
    chain_iteration = Column(Integer, primary_key=True)
    position = Column(Integer, primary_key=True)
    singlepoint_record = relationship(SinglepointRecordORM)

    __table_args__ = (Index("ix_neb_singlepoints_singlepoint_id", "singlepoint_id"),)

    _qcportal_model_excludes = ["neb_id"]


class NEBInitialchainORM(BaseORM):
    __tablename__ = "neb_initialchain"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id, ondelete="cascade"), primary_key=True)
    position = Column(Integer, primary_key=True)

    molecule = relationship(MoleculeORM)

    _qcportal_model_excludes = ["neb_id"]


class NEBSpecificationORM(BaseORM):
    """
    Table for storing NEB specifications
    """

    __tablename__ = "neb_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)
    specification_hash = Column(String, nullable=False)

    singlepoint_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    singlepoint_specification = relationship(QCSpecificationORM, lazy="joined", uselist=False)

    optimization_specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=True)
    optimization_specification = relationship(OptimizationSpecificationORM, lazy="joined")

    keywords = Column(JSONB, nullable=False)
    protocols = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "specification_hash",
            "singlepoint_specification_id",
            "optimization_specification_id",
            name="ux_neb_specification_keys",
        ),
        Index("ix_neb_specification_program", "program"),
        Index("ix_neb_specification_singlepoint_specification_id", "singlepoint_specification_id"),
        Index("ix_neb_specification_optimization_specification_id", "optimization_specification_id"),
        CheckConstraint("program = LOWER(program)", name="ck_neb_specification_program_lower"),
    )

    # TODO - protocols will eventually be in the model
    _qcportal_model_excludes = [
        "id",
        "specification_hash",
        "singlepoint_specification_id",
        "optimization_specification_id",
        "protocols",
    ]

    @property
    def short_description(self) -> str:
        return f"{self.program}~{self.singlepoint_specification.short_description}"


class NEBRecordORM(BaseRecordORM):
    __tablename__ = "neb_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(NEBSpecificationORM.id), nullable=False)
    specification = relationship(NEBSpecificationORM, lazy="selectin")

    initial_chain = relationship(
        NEBInitialchainORM,
        order_by=NEBInitialchainORM.position,
        collection_class=ordering_list("position"),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    singlepoints = relationship(
        NEBSinglepointsORM,
        order_by=[NEBSinglepointsORM.chain_iteration, NEBSinglepointsORM.position],
        collection_class=ordering_list("position"),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    optimizations = relationship(
        NEBOptimizationsORM,
        order_by=NEBOptimizationsORM.position,
        collection_class=ordering_list("position"),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __mapper_args__ = {
        "polymorphic_identity": "neb",
    }

    _qcportal_model_excludes = [*BaseRecordORM._qcportal_model_excludes, "specification_id"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseRecordORM.model_dict(self, exclude)

        # Return initial molecule or just the ids, depending on what we have
        if "initial_chain" in d:
            init_chain = d.pop("initial_chain")
            d["initial_chain_molecule_ids"] = [x["molecule_id"] for x in init_chain]
            if "molecule" in init_chain[0]:
                d["initial_chain"] = [x["molecule"] for x in init_chain]

        if "optimizations" in d:
            optimizations = d.pop("optimizations")

            opt_dict = {}
            for opt in optimizations:
                if opt["ts"]:
                    opt_dict["transition"] = opt
                elif opt["position"] == 0:
                    opt_dict["initial"] = opt
                else:
                    opt_dict["final"] = opt

            d["optimizations"] = opt_dict

        return d

    @property
    def short_description(self) -> str:
        n_mols = len(self.initial_chain)
        return f'{n_mols}*{self.initial_chain[0].molecule.identifiers["molecular_formula"]} {self.specification.short_description}'


# Delete base record if this record is deleted
_del_baserecord_trigger = DDL(
    """
    CREATE TRIGGER qca_neb_record_delete_base_tr
    AFTER DELETE ON neb_record
    FOR EACH ROW EXECUTE PROCEDURE qca_base_record_delete();
    """
)

event.listen(NEBRecordORM.__table__, "after_create", _del_baserecord_trigger.execute_if(dialect=("postgresql")))
