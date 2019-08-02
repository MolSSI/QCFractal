"""
A model for GridOptimization
"""
import copy
import json
from enum import Enum
from typing import Any, Dict, List, Tuple, Union

from pydantic import BaseModel, constr, validator, Schema

from .common_models import Molecule, ObjectId, OptimizationSpecification, QCSpecification
from .model_utils import json_encoders, recursive_normalizer
from .records import RecordBase

__all__ = ["GridOptimizationInput", "GridOptimizationRecord"]


class ScanTypeEnum(str, Enum):
    """
    The scan types allowed by the scan dimensions.
    """
    distance = 'distance'
    angle = 'angle'
    dihedral = 'dihedral'


class StepTypeEnum(str, Enum):
    """
    The scan types allowed by the scan dimensions.
    """
    absolute = 'absolute'
    relative = 'relative'


class ScanDimension(BaseModel):
    """
    A dimension to scan over
    """
    type: ScanTypeEnum = Schema(
        ...,
        description="What measurement to scan along"
    )
    indices: List[int] = Schema(
        ...,
        description="The indices of atoms to select for the scan. The size of this is a function of the type. e.g. "
                    "distances take 2 atoms, angles take 3, and dihedrals take 4. "
    )
    steps: List[float] = Schema(
        ...,
        description="Step sizes to scan in relative to your current location in the scan. This must be a strictly "
                    "monotonic series."
    )
    step_type: StepTypeEnum = Schema(
        ...,
        description="How to interpret the ``steps`` values in either an absolute or relative scale."
    )

    class Config:
        extra = "forbid"
        allow_mutation = False

    @validator('type', 'step_type', pre=True)
    def check_lower_type_step_type(cls, v):
        return v.lower()

    @validator('indices', whole=True)
    def check_indices(cls, v, values, **kwargs):
        sizes = {ScanTypeEnum.distance: 2, ScanTypeEnum.angle: 3, ScanTypeEnum.dihedral: 4}
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


class GOKeywords(BaseModel):
    """
    GridOptimizationRecord options
    """
    scans: List[ScanDimension] = Schema(
        ...,
        description="Which dimensions to scan along (along with their options) for the full G.O. operation"
    )
    preoptimization: bool = Schema(
        True,
        description="Whether or not to try to pre-optimize the scan with initial values."
    )

    class Config:
        extra = "forbid"
        allow_mutation = False


_gridopt_constr = constr(strip_whitespace=True, regex="gridoptimization")
_qcfractal_constr = constr(strip_whitespace=True, regex="qcfractal")


class GridOptimizationInput(BaseModel):
    """
    A GridOptimizationRecord Input base class
    """

    program: _qcfractal_constr = Schema(
        "qcfractal",
        description="The name of the source program which initializes the Grid Optimization. This is a constant "
                    "and is used for provenance information."
    )
    procedure: _gridopt_constr = Schema(
        "gridoptimization",
        description="The name of the procedure being run, which is Grid Optimization. This is a constant "
                    "and is used for provenance information."
    )
    initial_molecule: Union[ObjectId, Molecule] = Schema(
        ...,
        description="The Molecule to start the Grid Optimization with. This can either be an existing Molecule in "
                    "the database (through its :class:`ObjectId`) or a fully specified :class:`Molecule` model."
    )
    keywords: GOKeywords = Schema(
        ...,
        description="The keyword options to run the Grid Optimization with."
    )
    optimization_spec: OptimizationSpecification = Schema(
        ...,
        description="The spec to run the underlying optimization through at each grid point"
    )
    qc_spec: QCSpecification = Schema(
        ...,
        description="The specification for each of the actual quantum chemistry calculations run in each optimization "
                    "at each of the grid points."
    )

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
    version: int = Schema(
        1,
        description="The version number of the Record itself."
    )
    procedure: _gridopt_constr = Schema(
        "gridoptimization",
        description="The name of the procedure being run, which is Grid Optimization. This is a constant "
                    "and is used for provenance information."
    )
    program: _qcfractal_constr = Schema(
        "qcfractal",
        description="The name of the source program which initializes the Grid Optimization. This is a constant "
                    "and is used for provenance information."
    )

    # Input data
    initial_molecule: ObjectId = Schema(
        ...,
        description="ID of the intial molecule in the database which this record references."
    )
    keywords: GOKeywords = Schema(
        ...,
        description="The keyword options to run the Grid Optimization with."
    )
    optimization_spec: OptimizationSpecification = Schema(
        ...,
        description="The spec to run the underlying optimization through at each grid point"
    )
    qc_spec: QCSpecification = Schema(
        ...,
        description="The specification for each of the actual quantum chemistry calculations run in each optimization "
                    "at each of the grid points."
    )

    # Output data
    starting_molecule: ObjectId = Schema(
        ...,
        description="ID of the molecule in the database which was selected for the starting optimization on the grid. "
                    "This *CAN* be the same is the same ID as the initial_molecule, but in many cases is not."
    )
    final_energy_dict: Dict[str, float] = Schema(
        ...,
        description="Map of the final energy outputs from the grid optimization at each point"
    )
    grid_optimizations: Dict[str, ObjectId] = Schema(
        ...,
        description="Full record of the ID of each optimization at each of the grid points"
    )
    starting_grid: tuple = Schema(
        ...,
        description="Initial grid layout provided to the Grid Optimization procedure under which the scans were "
                    "performed"
    )

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
