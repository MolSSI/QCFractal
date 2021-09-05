from sqlalchemy import Column, Integer, String, JSON, Float, Boolean, Index

from qcfractal.interface.models import ObjectId
from qcfractal.db_socket.base_orm import Base
from qcfractal.db_socket.column_types import MsgpackExt

from typing import Dict, Any, Optional, Iterable


class MoleculeORM(Base):
    """
    The molecule DB collection is managed by pymongo, so far
    """

    __tablename__ = "molecule"

    id = Column(Integer, primary_key=True)
    molecular_formula = Column(String)

    # TODO - hash can be stored more efficiently (ie, byte array)
    molecule_hash = Column(String)

    # Required data
    schema_name = Column(String)
    schema_version = Column(Integer, default=2)
    symbols = Column(MsgpackExt, nullable=False)
    geometry = Column(MsgpackExt, nullable=False)

    # Molecule data
    name = Column(String, default="")
    identifiers = Column(JSON)
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

    # Orientation
    fix_com = Column(Boolean, default=False)
    fix_orientation = Column(Boolean, default=False)
    fix_symmetry = Column(String)

    # Extra
    provenance = Column(JSON)
    extras = Column(JSON)

    __table_args__ = (Index("ix_molecule_hash", "molecule_hash", unique=False),)

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = Base.dict(self, exclude)

        # TODO - remove this eventually
        # right now, a lot of code depends on these fields not being here
        # but that is not right. After changing the client code to be more dict-oriented,
        # then we can add these back
        d.pop("molecule_hash", None)
        d.pop("molecular_formula", None)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        # Also, remove any values that are None/Null in the db
        # The molecule pydantic model cannot always handle these, so let the model handle the defaults
        return {k: v for k, v in d.items() if v is not None}
