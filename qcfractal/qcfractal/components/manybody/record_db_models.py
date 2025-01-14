from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    UniqueConstraint,
    Index,
    CheckConstraint,
    event,
    DDL,
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_keyed_dict

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.record_db_models import SinglepointRecordORM, QCSpecificationORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class ManybodyClusterORM(BaseORM):
    """
    Table for storing fragment clusters in manybody calculations
    """

    __tablename__ = "manybody_cluster"

    id = Column(Integer, primary_key=True)
    manybody_id = Column(Integer, ForeignKey("manybody_record.id", ondelete="cascade"))
    molecule_id = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    mc_level = Column(String, nullable=False)
    fragments = Column(ARRAY(Integer), nullable=False)
    basis = Column(ARRAY(Integer), nullable=False)

    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), nullable=True)

    molecule = relationship(MoleculeORM)
    singlepoint_record = relationship(SinglepointRecordORM)

    __table_args__ = (
        CheckConstraint("array_length(fragments, 1) > 0", name="ck_manybody_cluster_fragments"),
        CheckConstraint("array_length(basis, 1) > 0", name="ck_manybody_cluster_basis"),
        UniqueConstraint("manybody_id", "mc_level", "fragments", "basis", name="ux_manybody_cluster_unique"),
        Index("ix_manybody_cluster_molecule_id", "molecule_id"),
        Index("ix_manybody_cluster_singlepoint_id", "singlepoint_id"),
    )

    _qcportal_model_excludes = ["manybody_id", "id"]


class ManybodySpecificationORM(BaseORM):
    """
    Table for storing manybody specifications
    """

    __tablename__ = "manybody_specification"

    id = Column(Integer, primary_key=True)
    specification_hash = Column(String, nullable=False)

    program = Column(String, nullable=False)
    bsse_correction = Column(ARRAY(String), nullable=False)

    keywords = Column(JSONB, nullable=False)
    protocols = Column(JSONB, nullable=False)

    levels = relationship(
        "ManybodySpecificationLevelsORM", lazy="selectin", collection_class=attribute_keyed_dict("level")
    )

    # Note - specification_hash will not be unique because of the different levels!
    # The levels are stored in another table with FK to this table, so seemingly
    # duplicate rows in this table could have different rows in the levels table
    __table_args__ = (
        Index("ix_manybody_specification_program", "program"),
        CheckConstraint("program = LOWER(program)", name="ck_manybody_specification_program_lower"),
    )

    _qcportal_model_excludes = ["id", "specification_hash"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        # Levels should just be key -> specification
        # map -1 for levels to 'supersystem'
        d["levels"] = {k if k != -1 else "supersystem": v["singlepoint_specification"] for k, v in d["levels"].items()}

        return d

    @property
    def short_description(self) -> str:
        return f"{self.program}~{sorted(self.levels.keys())}"


class ManybodySpecificationLevelsORM(BaseORM):
    """
    Association table for storing singlepoint specifications that are part of a manybody specification
    """

    __tablename__ = "manybody_specification_levels"

    id = Column(Integer, primary_key=True)

    manybody_specification_id = Column(Integer, ForeignKey(ManybodySpecificationORM.id), nullable=False)

    level = Column(Integer, nullable=False)
    singlepoint_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)

    singlepoint_specification = relationship(QCSpecificationORM, lazy="joined")

    __table_args__ = (
        UniqueConstraint("manybody_specification_id", "level", name="ux_manybody_specification_levels_unique"),
        Index("ix_manybody_specifications_levels_manybody_specification_id", "manybody_specification_id"),
        Index("ix_manybody_specifications_levels_singlepoint_specification_id", "singlepoint_specification_id"),
    )

    _qcportal_model_excludes = ["id"]


class ManybodyRecordORM(BaseRecordORM):
    """
    Table for storing manybody calculations
    """

    __tablename__ = "manybody_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    specification_id = Column(Integer, ForeignKey(ManybodySpecificationORM.id), nullable=False)

    specification = relationship(ManybodySpecificationORM, lazy="selectin")
    initial_molecule = relationship(MoleculeORM)

    clusters = relationship(ManybodyClusterORM, cascade="all, delete-orphan", passive_deletes=True)

    __mapper_args__ = {
        "polymorphic_identity": "manybody",
    }

    _qcportal_model_excludes = [*BaseRecordORM._qcportal_model_excludes, "specification_id"]

    @property
    def short_description(self) -> str:
        return f'{self.initial_molecule.identifiers["molecular_formula"]} {self.specification.short_description}'


# Delete base record if this record is deleted
_del_baserecord_trigger = DDL(
    """
    CREATE TRIGGER qca_manybody_record_delete_base_tr
    AFTER DELETE ON manybody_record
    FOR EACH ROW EXECUTE PROCEDURE qca_base_record_delete();
    """
)

event.listen(ManybodyRecordORM.__table__, "after_create", _del_baserecord_trigger.execute_if(dialect=("postgresql")))
