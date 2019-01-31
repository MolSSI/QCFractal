"""
A model for TorsionDrive
"""

import copy
import json

from typing import Any, Dict, List, Tuple, Union
from pydantic import BaseModel

from .common_models import QCMeta, Provenance, Molecule, json_encoders

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
            allow_mutation = False

    class OptOptions(BaseModel):
        """
        TorsionDrive options
        """
        program: str

        class Config:
            allow_extra = True
            allow_mutation = False

    initial_molecule: Molecule
    torsiondrive_meta: TDOptions
    optimization_meta: OptOptions
    qc_meta: QCMeta

    class Config:
        allow_mutation = False
        json_encoders = json_encoders


class TorsionDrive(TorsionDriveInput):
    """
    A interface to the raw JSON data of a TorsionDrive torsion scan run.
    """

    id: str
    success: bool
    hash_index: str
    provenance: Provenance

    optimization_history: Any
    initial_molecule: Union[str, Molecule]
    final_molecule: Union[str, Molecule]

    final_energies: Dict[str, float]
    minimum_positions: List[int]
