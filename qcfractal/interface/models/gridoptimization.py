"""
A model for GridOptimization
"""

import json
from typing import Dict, List, Tuple, Union

from pydantic import BaseModel, constr, validator

from .common_models import Molecule, ObjectId, OptimizationSpecification, QCSpecification
from .model_utils import json_encoders, recursive_normalizer
from .records import RecordBase

__all__ = ["GridOptimizationInput", "GridOptimizationRecord"]


class ScanDimension(BaseModel):
    """
    A dimension to scan over
    """
    type: str
    indices: List[int]
    steps: List[float]
    step_type: str

    class Config:
        extra = "forbid"
        allow_mutation = False

    @validator('type')
    def check_type(cls, v):
        v = v.lower()
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
    def check_steps(cls, v):
        if not (all(x < y for x, y in zip(v, v[1:])) or all(x > y for x, y in zip(v, v[1:]))):
            raise ValueError("Steps are not strictly monotonically increasing or decreasing.")

        v = recursive_normalizer(v)

        return v

    @validator('step_type')
    def check_step_type(cls, v):
        v = v.lower()
        if v not in ["absolute", "relative"]:
            raise KeyError("Keyword 'step_type' must either be absolute or relative.")

        return v


class GOKeywords(BaseModel):
    """
    GridOptimizationRecord options
    """
    scans: List[ScanDimension]
    preoptimization: bool = True

    class Config:
        extra = "forbid"
        allow_mutation = False


_gridopt_constr = constr(strip_whitespace=True, regex="gridoptimization")
_qcfractal_constr = constr(strip_whitespace=True, regex="qcfractal")


class GridOptimizationInput(BaseModel):
    """
    A GridOptimizationRecord Input base class
    """

    program: _qcfractal_constr = "qcfractal"
    procedure: _gridopt_constr = "gridoptimization"
    initial_molecule: Union[ObjectId, Molecule]
    keywords: GOKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    class Config:
        allow_mutation = False
        json_encoders = json_encoders


class GridOptimizationRecord(RecordBase):
    """
    A interface to the raw JSON data of a GridOptimizationRecord torsion scan run.
    """

    # Classdata
    _hash_indices = {"initial_molecule", "keywords", "optimization_meta", "qc_meta"}

    # Version data
    version: int = 1
    procedure: _gridopt_constr = "gridoptimization"
    program: _qcfractal_constr = "qcfractal"

    # Input data
    initial_molecule: ObjectId
    keywords: GOKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    # Output data
    starting_molecule: ObjectId
    final_energy_dict: Dict[str, float]
    grid_optimizations: Dict[str, ObjectId]
    starting_grid: tuple

    class Config(RecordBase.Config):
        pass

    def __str__(self):
        """
        Simplified gridoptimization string representation.

        Returns
        -------
        ret : str
            A representation of the current GridOptimizationRecord status.

        Examples
        --------

        >>> repr(torsiondrive_obj)
        GridOptimizationRecord(id='5b7f1fd57b87872d2c5d0a6d', success=True, molecule_id='5b7f1fd57b87872d2c5d0a6c')
        """

        ret = "GridOptimizationRecord("
        ret += "id='{}', ".format(self.id)
        ret += "status='{}', ".format(self.status)
        ret += "initial_molecule='{}')".format(self.initial_molecule)

        return ret

## Utility

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


## Query

    def final_energies(self, key=None):

        if key is None:
            return {self.deserialize_key(k): v for k, v in self.final_energy_dict.items()}
        else:
            return self.final_energy_dict[self.serialize_key(key)]
