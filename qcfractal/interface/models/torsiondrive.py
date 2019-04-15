"""
A model for TorsionDrive
"""

import copy
import json
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, constr, validator

from .common_models import Molecule, ObjectId, OptimizationSpecification, QCSpecification
from .model_utils import json_encoders, recursive_normalizer
from .records import RecordBase

__all__ = ["TorsionDriveInput", "TorsionDriveRecord"]


class TDKeywords(BaseModel):
    """
    TorsionDriveRecord options
    """
    dihedrals: List[Tuple[int, int, int, int]]
    grid_spacing: List[int]

    def __init__(self, **kwargs):
        super().__init__(**recursive_normalizer(kwargs))

    class Config:
        extra = "allow"
        allow_mutation = False


_td_constr = constr(strip_whitespace=True, regex="torsiondrive")
_qcfractal_constr = constr(strip_whitespace=True, regex="qcfractal")


class TorsionDriveInput(BaseModel):
    """
    A TorsionDriveRecord Input base class
    """

    program: _td_constr = "torsiondrive"
    procedure: _td_constr = "torsiondrive"
    initial_molecule: List[Union[ObjectId, Molecule, int]]
    keywords: TDKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    @validator('initial_molecule', pre=True, whole=True)
    def check_initial_molecules(cls, v):
        if isinstance(v, (str, dict, Molecule)):
            v = [v]
        return v

    class Config:
        extras = "forbid"
        allow_mutation = False
        json_encoders = json_encoders


class TorsionDriveRecord(RecordBase):
    """
    A interface to the raw JSON data of a TorsionDriveRecord torsion scan run.
    """

    # Classdata
    _hash_indices = {"initial_molecule", "keywords", "optimization_spec", "qc_spec"}

    # Version data
    version: int = 1
    procedure: _td_constr = "torsiondrive"
    program: _td_constr = "torsiondrive"

    # Input data
    initial_molecule: List[Union[ObjectId, int]]
    keywords: TDKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    # Output data
    final_energy_dict: Dict[str, float]

    optimization_history: Dict[str, List[Union[ObjectId, int]]]
    minimum_positions: Dict[str, int]

    class Config(RecordBase.Config):
        pass

## Utility

    def _serialize_key(self, key):
        if isinstance(key, str):
            return key
        elif isinstance(key, (int, float)):
            key = (int(key), )

        return json.dumps(key)

    def _deserialize_key(self, key: str) -> Tuple[int, ...]:
        return tuple(json.loads(key))

    def _organize_return(self, data: Dict[str, Any], key: Union[int, str, None],
                         minimum: bool=False) -> Dict[str, Any]:

        if key is None:
            return {self._deserialize_key(k): copy.deepcopy(v) for k, v in data.items()}

        key = self._serialize_key(key)

        if minimum:
            minpos = self.minimum_positions[self._serialize_key(key)]
            return copy.deepcopy(data[key][minpos])
        else:
            return copy.deepcopy(data[key])

## Query

    def get_history(self, key: Union[int, Tuple[int, ...], str]=None, minimum: bool=False) -> Dict[str, Any]:
        """Queries the server for all optimization trajectories.

        Parameters
        ----------
        key : Union[int, Tuple[int, ...], str], optional
            Specifies a single entry to pull from.
        minimum : bool, optional
            If true only returns the minimum optimization, otherwise returns all trajectories.

        Returns
        -------
        Dict[str, Any]
            The optimization history

        """

        if "history" not in self.cache:

            # Grab procedures
            needed_ids = [x for v in self.optimization_history.values() for x in v]
            objects = self.client.query_procedures(id=needed_ids)
            procedures = {v.id: v for v in objects}

            # Move procedures into the correct order
            ret = {}
            for okey, hashes in self.optimization_history.items():
                tmp = []
                for h in hashes:
                    tmp.append(procedures[h])
                ret[okey] = tmp

            self.cache["history"] = ret

        data = self.cache["history"]

        return self._organize_return(data, key, minimum=minimum)

    def get_final_energies(self, key: Union[int, Tuple[int, ...], str]=None) -> Dict[str, Any]:
        """
        Provides the final optimized energies at each grid point.

        Parameters
        ----------
        key : Union[int, str, None], optional
            Specifies a single entry to pull from.


        Returns
        -------
        energy : Dict[str, Any]
            Returns energies at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------

        >>> torsiondrive_obj.get_final_energies()
        {(-90,): -148.7641654446243, (180,): -148.76501336993732, (0,): -148.75056290106735, (90,): -148.7641654446148}
        """

        return self._organize_return(self.final_energy_dict, key)

    def get_final_molecules(self, key: Union[int, Tuple[int, ...], str]=None) -> Dict[str, Any]:
        """Returns the optimized molecules at each grid point

        Parameters
        ----------
        key : Union[int, str, None], optional
            Specifies a single entry to pull from.


        Returns
        -------
        energy : Dict[str, Any]
            Returns molecule at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------

        >>> torsiondrive_obj.get_final_energies()
        {(-90,):{'symbols': ['H', 'O', 'O', 'H'], 'geometry': [1.72669422, 1.28135788, ... }
        """

        if "final_molecules" not in self.cache:

            ret = {}
            for k, tasks in self.get_history().items():
                k = self._serialize_key(k)
                minpos = self.minimum_positions[k]

                ret[k] = tasks[minpos].get_final_molecule()

            self.cache["final_molecules"] = ret

        data = self.cache["final_molecules"]

        return self._organize_return(data, key)
