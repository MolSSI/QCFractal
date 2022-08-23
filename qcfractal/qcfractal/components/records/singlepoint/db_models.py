from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, ForeignKey, String, Enum, UniqueConstraint, CheckConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.db_socket.base_orm import BaseORM
from qcfractal.db_socket.column_types import MsgpackExt
from qcportal.singlepoint import SinglepointDriver

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class QCSpecificationORM(BaseORM):
    """
    Table for storing the core specifications of a QC calculation
    """

    __tablename__ = "qc_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)
    driver = Column(Enum(SinglepointDriver), nullable=False)
    method = Column(String(100), nullable=False)
    basis = Column(String(100), nullable=False)
    keywords = Column(JSONB, nullable=False)

    protocols = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program", "driver", "method", "basis", "keywords", "protocols", name="ux_qc_specification_keys"
        ),
        Index("ix_qc_specification_program", "program"),
        Index("ix_qc_specification_driver", "driver"),
        Index("ix_qc_specification_method", "method"),
        Index("ix_qc_specification_basis", "basis"),
        Index("ix_qc_specification_keywords", "keywords"),
        Index("ix_qc_specification_protocols", "protocols"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_qc_specification_program_lower"),
        CheckConstraint("method = LOWER(method)", name="ck_qc_specification_method_lower"),
        CheckConstraint("basis = LOWER(basis)", name="ck_qc_specification_basis_lower"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id")
        return BaseORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return {self.program: None}


class SinglepointRecordORM(BaseRecordORM):
    """
    Table for storing singlepoint calculations
    """

    __tablename__ = "singlepoint_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    specification = relationship(QCSpecificationORM, lazy="selectin")

    molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    molecule = relationship(MoleculeORM)

    return_result = Column(MsgpackExt)
    properties = Column(JSONB)

    wavefunction = relationship(WavefunctionStoreORM, uselist=False)

    __table_args__ = (
        Index("ix_singlepoint_record_molecule_id", "molecule_id"),
        Index("ix_singlepoint_record_specification_id", "specification_id"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "singlepoint",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
