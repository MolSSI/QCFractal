from __future__ import annotations

from typing import Dict, Optional

from sqlalchemy import select, Column, Integer, ForeignKey, String, UniqueConstraint, Index, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.singlepoint.db_models import QCSpecificationORM, SinglepointRecordORM
from qcfractal.db_socket import BaseORM


class NEBSinglepointsORM(BaseORM):

    __tablename__ = "neb_singlepoints"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
    
    chain_iteration = Column(Integer, primary_key=True)

    position = Column(Integer, primary_key=True)

    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), nullable=False)
    
    singlepoint_record = relationship(SinglepointRecordORM)

  

class NEBInitialchainORM(BaseORM):   
    
    __tablename__="neb_initialchain"

    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)

    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id, ondelete="cascade"), primary_key=True)

    position = Column(Integer, primary_key=True)
    
    moelcule = relationship(MoleculeORM)
    
 
class NEBSpecificationORM(BaseORM):

    __tablename__ = "neb_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)

    qc_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    qc_specification = relationship(QCSpecificationORM, lazy="selectin", uselist=False)

    keywords = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program",
            "qc_specification_id",
            "keywords",
            name="ux_neb_specification_keys",
        ),
        Index("ix_neb_specification_program", "program"),
        Index("ix_neb_specification_qc_specification_id", "qc_specification_id"),
        Index("ix_neb_specification_keywords", "keywords"),
        # Enforce lowercase on some fields
    )

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        r = {self.program: None}
        r.update(self.singlepoint_specification.required_programs)
        return r


class NEBRecordORM(BaseRecordORM):

    __tablename__ = "neb_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(NEBSpecificationORM.id), nullable=False)
    specification = relationship(NEBSpecificationORM, lazy="selectin")

    

    #neb_chain_id = Column(Integer, ForeignKey(NEBChainsORM.id), nullable=False)
    #neb_chain = relationship(NEBChainORM) 

    
    singlepoints = relationship(
        NEBSinglepointsORM,
        order_by=[NEBSinglepointsORM.chain_iteration, NEBSinglepointsORM.position],
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "neb",
    }

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs

#class NEBMoleculesORM(BaseORM):
#
#    __tablename__ = 'neb_molecules'
#
#    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
#
#    chain_iteration = Column(Integer, ForeignKey("neb_chains.iteration", ondelete="cascade"), primary_key=True)
#
#    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), primary_key=True)
#
#    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), primary_key=True)
#
#    position = Column(Integer, primary_key=True)
#
#    result = column_property(
#        select(SinglepointRecordORM.return_result).where(SinglepointRecordORM.id == singlepoint_id).scalar_subquery()
#    )
#
#    molecule = relationship(MoleculeORM)
#    singlepoint_record = relationship(SinglepointRecordORM)
#
#
# 
#class NEBChainsORM(BaseORM):
#
#    __tablename__ = "neb_chains"
#   
#
#    neb_id = Column(Integer, ForeignKey("neb_record.id", ondelete="cascade"), primary_key=True)
#
#    #neb_molecule_id = Column(Integer, ForeignKey('neb_molecules.id'), primary_key=True)
#
#    iteration = Column(Integer, primary_key=True)
#
#    molecules = relationship(
#        NEBMoleculesORM,
#        order_by=NEBMoleculesORM.position,
#        collection_class=ordering_list("position"),
#        cascade="all, delete-orphan",
#    )
#
#
#


