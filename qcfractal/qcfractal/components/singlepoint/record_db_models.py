from __future__ import annotations

from typing import TYPE_CHECKING

from qcelemental.models.results import WavefunctionProperties
from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
    Enum,
    UniqueConstraint,
    CheckConstraint,
    Index,
    LargeBinary,
    DDL,
    event,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, deferred

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket.base_orm import BaseORM
from qcportal.compression import CompressionEnum, decompress
from qcportal.singlepoint import SinglepointDriver

if TYPE_CHECKING:
    from typing import Dict, Optional


class WavefunctionORM(BaseORM):
    """
    Table for storing wavefunction data
    """

    __tablename__ = "wavefunction_store"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("singlepoint_record.id", ondelete="cascade"), nullable=False)

    compression_type = Column(Enum(CompressionEnum), nullable=False)
    compression_level = Column(Integer, nullable=False)
    data = deferred(Column(LargeBinary, nullable=False))

    __table_args__ = (UniqueConstraint("record_id", name="ux_wavefunction_store_record_id"),)

    _qcportal_model_excludes = ["id", "record_id", "compression_level"]

    def get_wavefunction(self) -> WavefunctionProperties:
        d = decompress(self.data, self.compression_type)
        return WavefunctionProperties(**d)


class QCSpecificationORM(BaseORM):
    """
    Table for storing the core specifications of a QC calculation
    """

    __tablename__ = "qc_specification"

    id = Column(Integer, primary_key=True)
    specification_hash = Column(String, nullable=False)

    program = Column(String(100), nullable=False)
    driver = Column(Enum(SinglepointDriver), nullable=False)
    method = Column(String(100), nullable=False)
    basis = Column(String(100), nullable=False)
    keywords = Column(JSONB, nullable=False)
    protocols = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint("specification_hash", name="ux_qc_specification_specification_hash"),
        Index("ix_qc_specification_program", "program"),
        Index("ix_qc_specification_driver", "driver"),
        Index("ix_qc_specification_method", "method"),
        Index("ix_qc_specification_basis", "basis"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_qc_specification_program_lower"),
        CheckConstraint("method = LOWER(method)", name="ck_qc_specification_method_lower"),
        CheckConstraint("basis = LOWER(basis)", name="ck_qc_specification_basis_lower"),
    )

    _qcportal_model_excludes = ["id", "specification_hash"]

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return {self.program: None}

    @property
    def short_description(self) -> str:
        return f'{self.program}/{self.method or "(none)"}/{self.basis or "(none)"}'


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

    wavefunction = relationship(WavefunctionORM, uselist=False, cascade="all, delete-orphan", passive_deletes=True)

    __table_args__ = (
        Index("ix_singlepoint_record_molecule_id", "molecule_id"),
        Index("ix_singlepoint_record_specification_id", "specification_id"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "singlepoint",
    }

    _qcportal_model_excludes = [*BaseRecordORM._qcportal_model_excludes, "specification_id"]

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        programs = self.specification.required_programs
        programs["qcengine"] = None
        return programs

    @property
    def short_description(self) -> str:
        return f'{self.molecule.identifiers["molecular_formula"]} {self.specification.short_description}'


# Mark the storage of the wavefunction data_local column as external
event.listen(
    WavefunctionORM.__table__,
    "after_create",
    DDL("ALTER TABLE native_file ALTER COLUMN data SET STORAGE EXTERNAL").execute_if(dialect=("postgresql")),
)


# Delete base record if this record is deleted
_del_baserecord_trigger = DDL(
    """
    CREATE TRIGGER qca_singlepoint_record_delete_base_tr
    AFTER DELETE ON singlepoint_record
    FOR EACH ROW EXECUTE PROCEDURE qca_base_record_delete();
    """
)

event.listen(SinglepointRecordORM.__table__, "after_create", _del_baserecord_trigger.execute_if(dialect=("postgresql")))
