from sqlalchemy import Column, Integer, ForeignKey, String, Enum, UniqueConstraint, CheckConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.keywords.db_models import KeywordsORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseResultORM
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.db_socket import BaseORM, MsgpackExt
from qcfractal.interface.models import DriverEnum


class SinglePointSpecificationORM(BaseORM):
    __tablename__ = "singlepoint_specification"

    id = Column(Integer, primary_key=True)

    # uniquely identifying a result
    program = Column(String(100), nullable=False)
    driver = Column(Enum(DriverEnum), nullable=False)
    method = Column(String(100), nullable=False)
    basis = Column(String(100), nullable=False)
    keywords_id = Column(Integer, ForeignKey(KeywordsORM.id), nullable=False)
    protocols = Column(JSONB, nullable=False)

    keywords = relationship(KeywordsORM, lazy="joined")

    __table_args__ = (
        UniqueConstraint(
            "program", "driver", "method", "basis", "keywords_id", "protocols", name="ux_singlepoint_specification_keys"
        ),
        Index("ix_singlepoint_specification_program", "program"),
        Index("ix_singlepoint_specification_driver", "driver"),
        Index("ix_singlepoint_specification_method", "method"),
        Index("ix_singlepoint_specification_basis", "basis"),
        Index("ix_singlepoint_specification_keywords_id", "keywords_id"),
        Index("ix_singlepoint_specification_protocols", "protocols"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_singlepoint_specification_program_lower"),
        CheckConstraint("method = LOWER(method)", name="ck_singlepoint_specification_method_lower"),
        CheckConstraint("basis = LOWER(basis)", name="ck_singlepoint_specification_basis_lower"),
    )


class ResultORM(BaseResultORM):
    """
    Hold the result of an atomic single calculation
    """

    __tablename__ = "singlepoint_record"

    id = Column(Integer, ForeignKey(BaseResultORM.id, ondelete="CASCADE"), primary_key=True)

    # uniquely identifying a result
    specification_id = Column(Integer, ForeignKey(SinglePointSpecificationORM.id), nullable=False)
    specification = relationship(SinglePointSpecificationORM, lazy="selectin", uselist=False)

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
