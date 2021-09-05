from __future__ import annotations

from sqlalchemy import Column, Integer, Boolean

from qcfractal.interface.models import ObjectId
from qcfractal.db_socket import BaseORM, MsgpackExt
from typing import Optional, Any, Dict, Iterable


class WavefunctionStoreORM(BaseORM):

    __tablename__ = "wavefunction_store"

    id = Column(Integer, primary_key=True)

    # Sparsity is very cheap
    basis = Column(MsgpackExt, nullable=False)
    restricted = Column(Boolean, nullable=False)

    # Core Hamiltonian
    h_core_a = Column(MsgpackExt, nullable=True)
    h_core_b = Column(MsgpackExt, nullable=True)
    h_effective_a = Column(MsgpackExt, nullable=True)
    h_effective_b = Column(MsgpackExt, nullable=True)

    # SCF Results
    scf_orbitals_a = Column(MsgpackExt, nullable=True)
    scf_orbitals_b = Column(MsgpackExt, nullable=True)
    scf_density_a = Column(MsgpackExt, nullable=True)
    scf_density_b = Column(MsgpackExt, nullable=True)
    scf_fock_a = Column(MsgpackExt, nullable=True)
    scf_fock_b = Column(MsgpackExt, nullable=True)
    scf_eigenvalues_a = Column(MsgpackExt, nullable=True)
    scf_eigenvalues_b = Column(MsgpackExt, nullable=True)
    scf_occupations_a = Column(MsgpackExt, nullable=True)
    scf_occupations_b = Column(MsgpackExt, nullable=True)

    # Extras
    extras = Column(MsgpackExt, nullable=True)

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseORM.dict(self, exclude)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d
