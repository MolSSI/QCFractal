"""
A model for TorsionDrive
"""

import copy
import json

from typing import Any, Dict, List, Tuple, Union
from pydantic import BaseModel

from .common_models import QCMeta, Provenance, Molecule, json_encoders, hash_dictionary

__all__ = ["TorsionDriveInput", "TorsionDrive"]


class TorsionDriveInput(BaseModel):
    """
    A TorsionDrive Input base class
    """

    class TDOptions(BaseModel):
        """
        TorsionDrive options
        """
        dihedrals: List[Tuple[int, int, int, int]]
        grid_spacing: List[float]

        class Config:
            allow_extra = True
            allow_mutation = False

    class OptOptions(BaseModel):
        """
        TorsionDrive options
        """
        program: str

        class Config:
            allow_extra = True
            allow_mutation = False

    program: str = "torsiondrive"
    procedure: str = "torsiondrive"
    initial_molecule: Molecule
    torsiondrive_meta: TDOptions
    optimization_meta: OptOptions
    qc_meta: QCMeta

    class Config:
        allow_mutation = False
        json_encoders = json_encoders

    def get_hash_index(self):
        if self.initial_molecule.id is None:
            raise ValueError("Cannot get the hash_index without a valid intial_molecule.id field.")

        data = self.dict(include=["program", "procedure", "torsiondrive_meta", "optimization_meta", "qc_meta"])
        data["initial_molecule"] = self.initial_molecule.id

        return hash_dictionary(data)


class TorsionDrive(TorsionDriveInput):
    """
    A interface to the raw JSON data of a TorsionDrive torsion scan run.
    """

    id: str = None
    success: bool = False
    status: str = "INCOMPLETE"
    hash_index: str
    provenance: Provenance

    optimization_history: Any
    initial_molecule: Union[str, Molecule]
    final_molecule: Union[str, Molecule]

    final_energies: Dict[str, float]
    minimum_positions: Dict[str, int]

    class Config:
        allow_mutation = False
        json_encoders = json_encoders

    def json_dict(self):
        return json.loads(self.json())
