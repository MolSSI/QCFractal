"""
A model for GridOptimization
"""
import copy
import json
from typing import Any, Dict, List, Tuple, Union

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
    _hash_indices = {"initial_molecule", "keywords", "optimization_meta", "qc_spec"}

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

## Utility

    def _organize_return(self, data: Dict[str, Any], key: Union[int, str, None]) -> Dict[str, Any]:

        if key is None:
            return {self.deserialize_key(k): copy.deepcopy(v) for k, v in data.items()}
        else:
            key = self.serialize_key(key)

        return copy.deepcopy(data[key])

    def serialize_key(self, key: Union[int, Tuple[int]]) -> str:
        """Serializes the key ot map to the internal keys.

        Parameters
        ----------
        key : Union[int, Tuple[int]]
            A integer or list of integers denoting the position in the grid
            to find.

        Returns
        -------
        str
            The internal key value.
        """
        if isinstance(key, (int, float)):
            key = (int(key), )

        return json.dumps(key)

    def deserialize_key(self, key:str)->Tuple[int]:
        """Unpacks a string key to a python object.

        Parameters
        ----------
        key : str
            The input key

        Returns
        -------
        Tuple[int]
            The unpacked key.
        """
        return tuple(json.loads(key))

    def get_scan_value(self, scan_number: int) -> Tuple[List[float]]:
        """
        Obtains the scan parameters at a given grid point.

        Parameters
        ----------
        key : Union[str, int, Tuple[int]]
            The key of the scan.

        Returns
        -------
        Tuple[List[float]]
            Description
        """
        if isinstance(key, str):
            key = self.deserialize_key(key)

        ret = []
        for n, idx in enumerate(key):
            ret.append(self.keywords.scans[n].steps[idx])

        return tuple(ret)

    def get_scan_dimensions(self) -> Tuple[List[float]]:
        """
        Returns the overall dimensions of the scan.

        Returns
        -------
        Tuple[List[float]]
            The size of each dimension in the scan.
        """
        ret = []
        for scan in self.keywords.scans:
            ret.append(len(scan.steps))

        return tuple(ret)

## Query

    def get_final_energies(self, key: Union[int, str, None]=None) -> Dict[str, float]:
        """
        Provides the final optimized energies at each grid point.

        Parameters
        ----------
        key : Union[int, str, None], optional
            Specifies a single entry to pull from.

        Returns
        -------
        energy : Dict[str, float]
            Returns energies at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------
        >>> grid_optimization_record.get_final_energies()
        {(-90,): -148.7641654446243, (180,): -148.76501336993732, (0,): -148.75056290106735, (90,): -148.7641654446148}

        >>> grid_optimization_record.get_final_energies((-90,))
        -148.7641654446243

        """

        return self._organize_return(self.final_energy_dict, key)


    def get_final_molecules(self, key: Union[int, str, None]=None) -> Dict[str, 'Molecule']:
        """
        Provides the final optimized molecules at each grid point.

        Parameters
        ----------
        key : Union[int, str, None], optional
            Specifies a single entry to pull from.


        Returns
        -------
        final_molecules : Dict[str, 'Molecule']
            Returns energies at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------

        >>> mols = grid_optimization_record.get_final_molecules()
        >>> type(mols[(-90, )])
        qcelemental.models.molecule.Molecule

        >>> type(grid_optimization_record.get_final_molecules((-90,)))
        qcelemental.models.molecule.Molecule

        """

        if "final_molecules" not in self.cache:
            ret = {}
            for k, task_id in self.grid_optimizations.items():
                task = self.client.query_procedures(id=task_id)[0]
                ret[k] = task.get_final_molecule()
            self.cache["final_molecules"] = ret

        data = self.cache["final_molecules"]
        return self._organize_return(data, key)


    def get_final_results(self, key: Union[int, Tuple[int, ...], str]=None) -> Dict[str, 'ResultRecord']:
        """Returns the final opt gradient result records at each grid point.

        Parameters
        ----------
        key : Union[int, Tuple[int, ...], str], optional
            Specifies a single entry to pull from.

        Returns
        -------
        final_results : Dict[str, 'ResultRecord']
            Returns ResultRecord at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------
        >>> mols = grid_optimization_record.get_final_results()
        >>> type(mols[(-90, )])
        qcfractal.interface.models.records.ResultRecord

        >>> type(grid_optimization_record.get_final_results((-90,)))
        qcfractal.interface.models.records.ResultRecord

        """

        if "final_results" not in self.cache:
            map_id_key = {}
            ret = {}
            for k, task_id in self.grid_optimizations.items():
                task = self.client.query_procedures(id=task_id)[0]
                if len(task.trajectory) > 0:
                    final_grad_record_id = task.trajectory[-1]
                    # store the id -> grid id mapping
                    map_id_key[final_grad_record_id] = k
            # combine the ids into one query
            query_result_ids = list(map_id_key.keys())
            # run the query on this batch
            for grad_result_record in self.client.query_results(id=query_result_ids):
                k = map_id_key[grad_result_record.id]
                ret[k] = grad_result_record

            self.cache["final_results"] = ret

        data = self.cache["final_results"]

        return self._organize_return(data, key)
