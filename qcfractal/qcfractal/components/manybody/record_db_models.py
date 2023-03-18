from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, String, Integer, ForeignKey, UniqueConstraint, Index, CheckConstraint, event, DDL
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship

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

    manybody_id = Column(Integer, ForeignKey("manybody_record.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column(Integer, ForeignKey("molecule.id"), primary_key=True)
    fragments = Column(ARRAY(Integer), nullable=False)
    basis = Column(ARRAY(Integer), nullable=False)
    degeneracy = Column(Integer, nullable=False)

    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), nullable=True)

    molecule = relationship(MoleculeORM)
    singlepoint_record = relationship(SinglepointRecordORM)

    __table_args__ = (
        CheckConstraint("degeneracy > 0", name="ck_manybody_cluster_degeneracy"),
        CheckConstraint("array_length(fragments, 1) > 0", name="ck_manybody_cluster_fragments"),
        CheckConstraint("array_length(basis, 1) > 0", name="ck_manybody_cluster_basis"),
        Index("ix_manybody_cluster_molecule_id", "molecule_id"),
        Index("ix_manybody_cluster_singlepoint_id", "singlepoint_id"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "manybody_id")
        return BaseORM.model_dict(self, exclude)


class ManybodySpecificationORM(BaseORM):
    """
    Table for storing manybody specifications
    """

    __tablename__ = "manybody_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String, nullable=False)
    singlepoint_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    keywords = Column(JSONB, nullable=False)
    keywords_hash = Column(String, nullable=False)

    singlepoint_specification = relationship(QCSpecificationORM, lazy="joined")

    __table_args__ = (
        UniqueConstraint(
            "program",
            "singlepoint_specification_id",
            "keywords_hash",
            name="ux_manybody_specification_keys",
        ),
        Index("ix_manybody_specification_program", "program"),
        Index("ix_manybody_specification_singlepoint_specification_id", "singlepoint_specification_id"),
        CheckConstraint("program = LOWER(program)", name="ck_manybody_specification_program_lower"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "keywords_hash", "singlepoint_specification_id")
        return BaseORM.model_dict(self, exclude)

    @property
    def short_description(self) -> str:
        return f"{self.program}~{self.singlepoint_specification.short_description}"


class ManybodyRecordORM(BaseRecordORM):
    """
    Table for storing manybody calculations
    """

    __tablename__ = "manybody_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    specification_id = Column(Integer, ForeignKey(ManybodySpecificationORM.id), nullable=False)
    results = Column(JSONB)

    specification = relationship(ManybodySpecificationORM, lazy="selectin")
    initial_molecule = relationship(MoleculeORM)

    clusters = relationship(ManybodyClusterORM, cascade="all, delete-orphan", passive_deletes=True)

    __mapper_args__ = {
        "polymorphic_identity": "manybody",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

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
