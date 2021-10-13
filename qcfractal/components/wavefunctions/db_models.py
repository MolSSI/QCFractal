from typing import Dict, Any, Optional, Iterable

from sqlalchemy import Column, Integer, Boolean, String

from qcfractal.db_socket import BaseORM, MsgpackExt


class WavefunctionStoreORM(BaseORM):

    __tablename__ = "wavefunction_store"

    id = Column(Integer, primary_key=True)

    # Sparsity is very cheap
    basis = Column(MsgpackExt, nullable=False)
    restricted = Column(Boolean, nullable=False)

    # Core Hamiltonian
    h_core_a = Column(MsgpackExt)
    h_core_b = Column(MsgpackExt)
    h_effective_a = Column(MsgpackExt)
    h_effective_b = Column(MsgpackExt)

    # SCF Results
    scf_orbitals_a = Column(MsgpackExt)
    scf_orbitals_b = Column(MsgpackExt)
    scf_density_a = Column(MsgpackExt)
    scf_density_b = Column(MsgpackExt)
    scf_fock_a = Column(MsgpackExt)
    scf_fock_b = Column(MsgpackExt)
    scf_eigenvalues_a = Column(MsgpackExt)
    scf_eigenvalues_b = Column(MsgpackExt)
    scf_occupations_a = Column(MsgpackExt)
    scf_occupations_b = Column(MsgpackExt)

    # Return results
    orbitals_a = Column(String)
    orbitals_b = Column(String)
    density_a = Column(String)
    density_b = Column(String)
    fock_a = Column(String)
    fock_b = Column(String)
    eigenvalues_a = Column(String)
    eigenvalues_b = Column(String)
    occupations_a = Column(String)
    occupations_b = Column(String)

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseORM.dict(self, exclude)

        # TODO - this is because the pydantic models are goofy
        return {k: v for k, v in d.items() if v is not None}
