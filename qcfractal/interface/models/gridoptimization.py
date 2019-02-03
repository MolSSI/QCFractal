"""
A model for GridOptimization
"""

import copy
import json
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel

from .common_models import QCMeta, Provenance, Molecule, json_encoders, hash_dictionary

__all__ = ["GridOptimizationInput", "GridOptimization"]


class GridOptimizationInput(BaseModel):
    """
    A GridOptimization Input base class
    """

    class ScanDimension(BaseModel):
        type: str
        indices: List[int]
        steps: List[float]

        @validator('type')
        def check_type(cls, v, values, **kwargs):
            possibilities = {"bond", "angle", "dihedral"}
            if v not in possibilities:
                raise TypeError("Type '{}' found, can only be one of {}.".format(v, possibilities))

            return v

        @validator('indices')
        def check_type(cls, v, values, **kwargs):
            sizes = {"bond": 2, "angle": 3, "dihedral": 4}
            if sizes[values["type"]] != len(v):
                raise ValueError("ScanDimension of type {} must have {} values, found {}.".format(
                    values["type"], sizes[values["type"]], len(v)))

        @validator('indices')
        def check_steps_monotonic(cls, v):
            if not all(x < y for x, y in zip(L, L[1:])):
                raise ValueError("Steps are not monotonically increasing.")

            return v

    class GOOptions(BaseModel):
        """
        GridOptimization options
        """
        scans: List[ScanDimension]
        grid_spacing: List[float]

        class Config:
            allow_extra = True
            allow_mutation = False

    class OptOptions(BaseModel):
        """
        GridOptimization options
        """
        program: str

        class Config:
            allow_extra = True
            allow_mutation = False

    program: str = "gridoptimization"
    procedure: str = "gridoptimization"
    initial_molecule: Molecule
    gridoptimization_meta: GOOptions
    optimization_meta: OptOptions
    qc_meta: QCMeta

    class Config:
        allow_mutation = False
        json_encoders = json_encoders

    def get_hash_index(self):
        if isinstance(self.initial_molecule, str):
            mol_id = self.initial_molecule

        else:
            if self.initial_molecule.id is None:
                raise ValueError("Cannot get the hash_index without a valid intial_molecule.id field.")

            mol_id = self.initial_molecule.id

        data = self.dict(include=["program", "procedure", "gridoptimization_meta", "optimization_meta", "qc_meta"])
        data["initial_molecule"] = mol_id

        return hash_dictionary(data)


class GridOptimization(GridOptimizationInput):
    """
    A interface to the raw JSON data of a GridOptimization torsion scan run.
    """

    # Client and local data
    client: Any = None
    cache: Dict[str, Any] = {}

    # Identification
    id: str = None
    success: bool = False
    status: str = "INCOMPLETE"
    hash_index: str = None

    provenance: Provenance

    # Data pointers
    initial_molecule: str
    final_energy_dict: Dict[str, float]
    grid_optimizations: Dict[str, str]

    class Config:
        allow_mutation = False
        json_encoders = json_encoders

    def __init__(self, **data):
        super().__init__(**data)

        # Set hash index if not present
        if self.hash_index is None:
            self.__values__["hash_index"] = self.get_hash_index()

    def __str__(self):
        """
        Simplified gridoptimization string representation.

        Returns
        -------
        ret : str
            A representation of the current GridOptimization status.

        Examples
        --------

        >>> repr(torsiondrive_obj)
        GridOptimization(id='5b7f1fd57b87872d2c5d0a6d', success=True, molecule_id='5b7f1fd57b87872d2c5d0a6c')
        """

        ret = "GridOptimization("
        ret += "id='{}', ".format(self.id)
        ret += "success='{}', ".format(self.success)
        ret += "initial_molecule='{}')".format(self.initial_molecule)

        return ret

## Utility

    def dict(self, exclude=set(), include=None, by_alias=False):
        exclude |= {"client", "cache"}
        return super().dict(include=include, exclude=exclude, by_alias=by_alias)

    def json_dict(self):
        return json.loads(self.json())
