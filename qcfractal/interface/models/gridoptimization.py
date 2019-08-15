"""
A model for GridOptimization
"""
import copy
import json
from enum import Enum
from typing import Any, Dict, List, Tuple, Union

from pydantic import Schema, constr, validator

from .common_models import Molecule, ObjectId, OptimizationSpecification, ProtoModel, QCSpecification
from .model_utils import recursive_normalizer
from .records import RecordBase

__all__ = ["GridOptimizationInput", "GridOptimizationRecord"]


class ScanTypeEnum(str, Enum):
    """
    The type of scan to perform. This choices is limited to the scan types allowed by the scan dimensions.
    """
    distance = 'distance'
    angle = 'angle'
    dihedral = 'dihedral'


class StepTypeEnum(str, Enum):
    """
    The types of steps to take in a scan dimension: either in absolute or relative terms. ``relative`` indicates that
    the values are relative to the starting value (e.g., a bond starts as 2.1 Bohr, relative steps of [-0.1, 0, 1.0]
    indicate grid points of [2.0, 2.1, 3.1] Bohr. An ``absolute`` ``step_type`` will be exactly those values instead."
    """
    absolute = 'absolute'
    relative = 'relative'


class ScanDimension(ProtoModel):
    """
    A full description of a dimension to scan over.
    """
    type: ScanTypeEnum = Schema(
        ...,
        description=str(ScanTypeEnum.__doc__)
    )
    indices: List[int] = Schema(
        ...,
        description="The indices of atoms to select for the scan. The size of this is a function of the type. e.g., "
                    "distances, angles and dihedrals require 2, 3, and 4 atoms, respectively."
    )
    steps: List[float] = Schema(
        ...,
        description="Step sizes to scan in relative to your current location in the scan. This must be a strictly "
                    "monotonic series.",
        units=["Bohr", "degrees"]
    )
    step_type: StepTypeEnum = Schema(
        ...,
        description=str(StepTypeEnum.__doc__)
    )

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


class GOKeywords(ProtoModel):
    """
    GridOptimizationRecord options.
    """
    scans: List[ScanDimension] = Schema(
        ...,
        description="The dimensions to scan along (along with their options) for the GridOptimization."
    )
    preoptimization: bool = Schema(
        True,
        description="If ``True``, first runs an unrestricted optimization before starting the grid computations. "
                    "This is especially useful when combined with ``relative`` ``step_types``."
    )



_gridopt_constr = constr(strip_whitespace=True, regex="gridoptimization")
_qcfractal_constr = constr(strip_whitespace=True, regex="qcfractal")


class GridOptimizationInput(ProtoModel):
    """
    The input to create a GridOptimization Service with.

    """

    program: _qcfractal_constr = Schema(
        "qcfractal",
        description="The name of the source program which initializes the Grid Optimization. This is a constant "
                    "and is used for provenance information."
    )
    procedure: _gridopt_constr = Schema(
        "gridoptimization",
        description="The name of the procedure being run. This is a constant and is used for provenance information."
    )
    initial_molecule: Union[ObjectId, Molecule] = Schema(
        ...,
        description="The Molecule to begin the Grid Optimization with. This can either be an existing Molecule in "
                    "the database (through its :class:`ObjectId`) or a fully specified :class:`Molecule` model."
    )
    keywords: GOKeywords = Schema(
        ...,
        description="The keyword options to run the Grid Optimization."
    )
    optimization_spec: OptimizationSpecification = Schema(
        ...,
        description="The specification to run the underlying optimization through at each grid point."
    )
    qc_spec: QCSpecification = Schema(
        ...,
        description="The specification for each of the quantum chemistry calculations run in each geometry "
                    "optimization."
    )


class GridOptimizationRecord(RecordBase):
    """
    The record of a GridOptimization service result.

    A GridOptimization is a type of constrained optimization in which a set of dimension are scanned over. An
    is to compute the

    """

    # Classdata
    _hash_indices = {"initial_molecule", "keywords", "optimization_meta", "qc_spec"}

    # Version data
    version: int = Schema(
        1,
        description="The version number of the Record."
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
        description="Id of the initial molecule in the database."
    )
    keywords: GOKeywords = Schema(
        ...,
        description="The keywords for this Grid Optimization."
    )
    optimization_spec: OptimizationSpecification = Schema(
        ...,
        description="The specification of each geometry optimization."
    )
    qc_spec: QCSpecification = Schema(
        ...,
        description="The specification for each of the quantum chemistry computations used by the geometry "
                    "optimizations."
    )

    # Output data
    starting_molecule: ObjectId = Schema(
        ...,
        description="Id of the molecule in the database begins the grid optimization. "
                    "This will differ from the ``initial_molecule`` if ``preoptimization`` is True."
    )
    final_energy_dict: Dict[str, float] = Schema(
        ...,
        description="Map of the final energy from the grid optimization at each grid point."
    )
    grid_optimizations: Dict[str, ObjectId] = Schema(
        ...,
        description="The Id of each optimization at each grid point."
    )
    starting_grid: tuple = Schema(
        ...,
        description="Initial grid point from which the Grid Optimization started. This grid point is the closest in "
                    "structure to the ``starting_molecule``."
    ) # yapf: disable

## Utility

    def _organize_return(self, data: Dict[str, Any], key: Union[int, str, None]) -> Dict[str, Any]:

        if key is None:
            return {self.deserialize_key(k): copy.deepcopy(v) for k, v in data.items()}
        else:
            key = self.serialize_key(key)

        return copy.deepcopy(data[key])

    @staticmethod
    def serialize_key(key: Union[int, Tuple[int]]) -> str:
        """Serializes the key to map to the internal keys.

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

    @staticmethod
    def deserialize_key(key: str) -> Tuple[int]:
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

    def get_scan_value(self, scan_number: Union[str, int, Tuple[int]]) -> Tuple[float, ...]:
        """
        Obtains the scan parameters at a given grid point.

        Parameters
        ----------
        scan_number : Union[str, int, Tuple[int]]
            The key of the scan.

        Returns
        -------
        Tuple[float, ...]
            Description
        """
        if isinstance(scan_number, str):
            scan_number = self.deserialize_key(scan_number)

        ret = []
        for n, idx in enumerate(scan_number):
            ret.append(self.keywords.scans[n].steps[idx])

        return tuple(ret)

    def get_scan_dimensions(self) -> Tuple[float, ...]:
        """
        Returns the overall dimensions of the scan.

        Returns
        -------
        Tuple[float, ...]
            The size of each dimension in the scan.
        """
        ret = []
        for scan in self.keywords.scans:
            ret.append(len(scan.steps))

        return tuple(ret)

## Query

    def get_final_energies(self, key: Union[int, str, None] = None) -> Dict[str, float]:
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
