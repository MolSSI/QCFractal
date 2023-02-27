from __future__ import annotations

from typing import Dict, Optional, Iterable, Any

from sqlalchemy import Column, Integer, ForeignKey, String, UniqueConstraint, Index, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.optimization.record_db_models import OptimizationRecordORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM, SinglepointRecordORM
from qcfractal.db_socket import BaseORM


class NEBOptimizationsORM(BaseORM):

    __tablename__ = "neb_optimizations"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
    optimization_id = Column(Integer, ForeignKey(OptimizationRecordORM.id), primary_key=True)
    position = Column(Integer, primary_key=True)
    ts = Column(Boolean, primary_key=True)
    optimization_record = relationship(OptimizationRecordORM)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "neb_id")
        return BaseORM.model_dict(self, exclude)


class NEBSinglepointsORM(BaseORM):

    __tablename__ = "neb_singlepoints"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), primary_key=True)
    chain_iteration = Column(Integer, primary_key=True)
    position = Column(Integer, primary_key=True)
    singlepoint_record = relationship(SinglepointRecordORM)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "neb_id")
        return BaseORM.model_dict(self, exclude)


class NEBInitialchainORM(BaseORM):

    __tablename__ = "neb_initialchain"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id, ondelete="cascade"), primary_key=True)
    position = Column(Integer, primary_key=True)

    molecule = relationship(MoleculeORM)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "neb_id")
        return BaseORM.model_dict(self, exclude)


class NEBSpecificationORM(BaseORM):

    __tablename__ = "neb_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)

    singlepoint_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    singlepoint_specification = relationship(QCSpecificationORM, lazy="joined", uselist=False)

    keywords = Column(JSONB, nullable=False)
    keywords_hash = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program",
            "singlepoint_specification_id",
            "keywords_hash",
            name="ux_neb_specification_keys",
        ),
        Index("ix_neb_specification_program", "program"),
        Index("ix_neb_specification_singlepoint_specification_id", "singlepoint_specification_id"),
        # Enforce lowercase on some fields
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "id", "keywords_hash", "singlepoint_specification_id")
        return BaseORM.model_dict(self, exclude)


class NEBRecordORM(BaseRecordORM):

    __tablename__ = "neb_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(NEBSpecificationORM.id), nullable=False)
    specification = relationship(NEBSpecificationORM, lazy="selectin")

    initial_chain = relationship(NEBInitialchainORM, collection_class=ordering_list("position"))

    singlepoints = relationship(
        NEBSinglepointsORM,
        order_by=[NEBSinglepointsORM.chain_iteration, NEBSinglepointsORM.position],
        collection_class=ordering_list("position"),
        cascade="all, delete-orphan",
    )

    optimizations = relationship(
        NEBOptimizationsORM,
        order_by=NEBOptimizationsORM.position,
        collection_class=ordering_list("position"),
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "neb",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)
