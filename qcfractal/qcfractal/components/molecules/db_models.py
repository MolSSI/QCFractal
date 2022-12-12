from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, String, JSON, Float, Index, CHAR, Boolean, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB

from qcfractal.db_socket.base_orm import BaseORM
from qcfractal.db_socket.column_types import MsgpackExt

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class MoleculeORM(BaseORM):
    """
    Table for storing molecules
    """

    __tablename__ = "molecule"

    id = Column(Integer, primary_key=True)

    molecule_hash = Column(CHAR(40))  # sha1 is always 40 chars

    # Required data
    schema_name = Column(String)
    schema_version = Column(Integer, default=2)
    symbols = Column(MsgpackExt, nullable=False)
    geometry = Column(MsgpackExt, nullable=False)

    # Molecule data
    name = Column(String, default="")
    identifiers = Column(JSONB)
    comment = Column(String)
    molecular_charge = Column(Float, default=0)
    molecular_multiplicity = Column(Integer, default=1)

    # Atom data
    masses = Column(MsgpackExt)
    real = Column(MsgpackExt)
    atom_labels = Column(MsgpackExt)
    atomic_numbers = Column(MsgpackExt)
    mass_numbers = Column(MsgpackExt)

    # Fragment and connection data
    connectivity = Column(JSON)
    fragments = Column(MsgpackExt)
    fragment_charges = Column(JSON)  # Column(ARRAY(Float))
    fragment_multiplicities = Column(JSON)  # Column(ARRAY(Integer))

    # Orientation & symmetry
    fix_com = Column(Boolean, nullable=False, default=True)
    fix_orientation = Column(Boolean, nullable=False, default=True)
    fix_symmetry = Column(String)

    # Extra
    provenance = Column(JSON)
    extras = Column(JSON)

    __table_args__ = (
        Index("ix_molecule_identifiers", "identifiers", postgresql_using="gin"),
        UniqueConstraint("molecule_hash", name="ux_molecule_molecule_hash"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # molecule_hash is only used for indexing. It is otherwise stored in identifiers
        exclude = self.append_exclude(exclude, "molecule_hash")

        d = BaseORM.model_dict(self, exclude)

        # TODO - this is because the pydantic models are goofy
        return {k: v for k, v in d.items() if v is not None}
