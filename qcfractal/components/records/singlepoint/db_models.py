from __future__ import annotations

from typing import Dict, Optional

from sqlalchemy import Column, Integer, ForeignKey, String, Enum, UniqueConstraint, CheckConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.keywords.db_models import KeywordsORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.db_socket.base_orm import BaseORM
from qcfractal.db_socket.column_types import MsgpackExt
from qcportal.records.singlepoint import SinglepointDriver


class QCSpecificationORM(BaseORM):
    __tablename__ = "qc_specification"

    id = Column(Integer, primary_key=True)

    # uniquely identifying a result
    program = Column(String(100), nullable=False)
    driver = Column(Enum(SinglepointDriver), nullable=False)
    method = Column(String(100), nullable=False)
    basis = Column(String(100), nullable=False)

    keywords_id = Column(Integer, ForeignKey(KeywordsORM.id), nullable=False)
    keywords = relationship(KeywordsORM, lazy="joined", uselist=False)

    protocols = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program", "driver", "method", "basis", "keywords_id", "protocols", name="ux_qc_specification_keys"
        ),
        Index("ix_qc_specification_program", "program"),
        Index("ix_qc_specification_driver", "driver"),
        Index("ix_qc_specification_method", "method"),
        Index("ix_qc_specification_basis", "basis"),
        Index("ix_qc_specification_keywords_id", "keywords_id"),
        Index("ix_qc_specification_protocols", "protocols"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_qc_specification_program_lower"),
        CheckConstraint("method = LOWER(method)", name="ck_qc_specification_method_lower"),
        CheckConstraint("basis = LOWER(basis)", name="ck_qc_specification_basis_lower"),
    )

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return {self.program: None}


class SinglepointRecordORM(BaseRecordORM):
    """
    Hold the result of an atomic single calculation
    """

    __tablename__ = "singlepoint_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="CASCADE"), primary_key=True)

    # uniquely identifying a result
    specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    specification = relationship(QCSpecificationORM, lazy="selectin", uselist=False)

    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    molecule = relationship(MoleculeORM, uselist=False)

    return_result = Column(MsgpackExt)
    properties = Column(JSONB)

    wavefunction = relationship(WavefunctionStoreORM, lazy="select", uselist=False)

    __table_args__ = (
        Index("ix_singlepoint_record_molecule_id", "molecule_id"),
        Index("ix_singlepoint_record_specification_id", "specification_id"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "singlepoint",
    }

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
