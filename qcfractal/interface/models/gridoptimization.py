"""
A model for GridOptimization
"""

import json
from typing import Any, Dict, List, Tuple, Union

from pydantic import BaseModel, validator

from .common_models import (Molecule, OptimizationSpecification, Provenance, QCSpecification, hash_dictionary,
                            json_encoders)

__all__ = ["GridOptimizationInput", "GridOptimization"]


class ScanDimension(BaseModel):
    """
    A dimension to scan over
    """
    type: str
    indices: List[int]
    steps: List[float]
    step_type: str

    @validator('type')
    def check_type(cls, v):
        possibilities = {"distance", "angle", "dihedral"}
        if v not in possibilities:
            raise TypeError("Type '{}' found, can only be one of {}.".format(v, possibilities))

        return v

    @validator('indices', whole=True)
    def check_indices(cls, v, values, **kwargs):
        sizes = {"distance": 2, "angle": 3, "dihedral": 4}
        if sizes[values["type"]] != len(v):
            raise ValueError("ScanDimension of type {} must have {} values, found {}.".format(
                values["type"], sizes[values["type"]], len(v)))

        return v

    @validator('steps', whole=True)
    def check_steps_monotonic(cls, v):
        if not (all(x < y for x, y in zip(v, v[1:])) or all(x > y for x, y in zip(v, v[1:]))):
            raise ValueError("Steps are not strictly monotonically increasing or decreasing.")

        return v

    @validator('step_type')
    def check_step_type(cls, v):
        v = v.lower()
        if v not in ["absolute", "relative"]:
            raise KeyError("Keyword 'step_type' must either be absolute or relative.")

        return v

    class Config:
        extra = "forbid"
        allow_mutation = False


class GOKeywords(BaseModel):
    """
    GridOptimization options
    """
    scans: List[ScanDimension]
    preoptimization: bool = True

    class Config:
        extra = "forbid"
        allow_mutation = False


class GridOptimizationInput(BaseModel):
    """
    A GridOptimization Input base class
    """

    program: str = "qcfractal"
    procedure: str = "gridoptimization"
    initial_molecule: Union[str, Molecule]
    keywords: GOKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

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

        data = self.dict(include={"program", "procedure", "keywords", "optimization_meta", "qc_meta"})
        data["initial_molecule"] = mol_id

        return hash_dictionary(data)

    def serialize_key(self, key):
        if isinstance(key, (int, float)):
            key = (int(key), )

        return json.dumps(key)

    def deserialize_key(self, key):

        return tuple(json.loads(key))

    def get_scan_value(self, key: Union[str, Tuple]) -> Tuple:
        """
        Obtains the scan parameters at a given grid point.
        """
        if isinstance(key, str):
            key = self.deserialize_key(key)

        ret = []
        for n, idx in enumerate(key):
            ret.append(self.keywords.scans[n].steps[idx])

        return tuple(ret)

    def get_scan_dimensions(self) -> Tuple:
        """
        Returns the overall dimensions of the scan.
        """
        ret = []
        for scan in self.keywords.scans:
            ret.append(len(scan.steps))

        return tuple(ret)


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
    starting_molecule: str
    final_energy_dict: Dict[str, float]
    grid_optimizations: Dict[str, str]
    starting_grid: tuple

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

    def dict(self, *args, **kwargs):
        kwargs["exclude"] = (kwargs.pop("exclude", None) or set()) | {"client", "cache"}
        return super().dict(*args, **kwargs)

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))

## Query

    def final_energies(self, key=None):

        if key is None:
            return {self.deserialize_key(k): v for k, v in self.final_energy_dict.items()}
        else:
            return self.final_energy_dict[self.serialize_key(key)]
