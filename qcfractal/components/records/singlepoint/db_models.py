from sqlalchemy import Column, Integer, ForeignKey, String, Enum, JSON, UniqueConstraint, CheckConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.keywords.db_models import KeywordsORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseResultORM
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.interface.models import DriverEnum, ObjectId
from qcfractal.db_socket import BaseORM, MsgpackExt

from typing import Optional, Dict, Any, Iterable


class ResultORM(BaseResultORM):
    """
    Hold the result of an atomic single calculation
    """

    __tablename__ = "result"

    id = Column(Integer, ForeignKey("base_result.id", ondelete="CASCADE"), primary_key=True)

    # uniquely identifying a result
    program = Column(String(100), nullable=False)  # example "rdkit", is it the same as program in keywords?
    driver = Column(String(100), Enum(DriverEnum), nullable=False)
    method = Column(String(100), nullable=False)  # example "uff"
    basis = Column(String(100))
    molecule = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    molecule_obj = relationship(MoleculeORM, lazy="select")

    # This is a special case where KeywordsORM are denormalized intentionally as they are part of the
    # lookup for a single result and querying a result will not often request the keywords (LazyReference)
    keywords = Column(Integer, ForeignKey("keywords.id"))
    keywords_obj = relationship(KeywordsORM, lazy="select")

    # Primary Result output
    return_result = Column(MsgpackExt)
    properties = Column(JSON)  # TODO: may use JSONB in the future

    # Wavefunction data
    wavefunction = Column(JSONB, nullable=True)
    wavefunction_data_id = Column(Integer, ForeignKey("wavefunction_store.id"), nullable=True)
    wavefunction_data_obj = relationship(
        WavefunctionStoreORM,
        lazy="select",
        foreign_keys=wavefunction_data_id,
        cascade="all, delete-orphan",
        single_parent=True,
    )

    __table_args__ = (
        # TODO: optimize indexes
        # A multicolumn GIN index can be used with query conditions that involve any subset of
        # the index's columns. Unlike B-tree or GiST, index search effectiveness is the same
        # regardless of which index column(s) the query conditions use.
        # Index('ix_result_combined', "program", "driver", "method", "basis",
        #       "keywords", postgresql_using='gin'),  # gin index
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name="uix_results_keys"),
        Index("ix_results_program", "program"),
        Index("ix_results_driver", "driver"),
        Index("ix_results_method", "method"),
        Index("ix_results_basis", "basis"),
        Index("ix_results_keywords", "keywords"),
        Index("ix_results_molecule", "molecule"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_result_program_lower"),
        CheckConstraint("driver = LOWER(driver)", name="ck_result_driver_lower"),
        CheckConstraint("method = LOWER(method)", name="ck_result_method_lower"),
        CheckConstraint("basis = LOWER(basis)", name="ck_result_basis_lower"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "result",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseORM.dict(self, exclude)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d
