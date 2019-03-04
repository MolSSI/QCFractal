"""
A model for TorsionDrive
"""

import copy
import json
from typing import Dict, List, Tuple, Union

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
    initial_molecule: List[Union[ObjectId, Molecule]]
    keywords: TDKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    @validator('initial_molecule', pre=True, whole=True)
    def check_initial_molecules(cls, v):
        if isinstance(v, (str, dict, Molecule)):
            v = [v]
        return v

    class Config:
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
    initial_molecule: List[ObjectId]
    keywords: TDKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    # Output data
    final_energy_dict: Dict[str, float]
    optimization_history: Dict[str, List[ObjectId]]
    minimum_positions: Dict[str, int]

    class Config(RecordBase.Config):
        pass

    def __str__(self):
        """
        Simplified torsiondrive string representation.

        Returns
        -------
        ret : str
            A representation of the current TorsionDriveRecord status.

        Examples
        --------

        >>> repr(torsiondrive_obj)
        TorsionDriveRecord(id='5b7f1fd57b87872d2c5d0a6d', success=True, molecule_id='5b7f1fd57b87872d2c5d0a6c')
        """

        ret = "TorsionDriveRecord("
        ret += "id='{}', ".format(self.id)
        ret += "status='{}', ".format(self.status)
        ret += "initial_molecule='{}')".format(self.initial_molecule)

        return ret

## Utility

    def _serialize_key(self, key):
        if isinstance(key, (int, float)):
            key = (int(key), )

        return json.dumps(key)

    def _deserialize_key(self, key):
        return tuple(json.loads(key))


## Query

    def get_history(self):
        """Pulls all optimization trajectories to local data.

        Returns
        -------
        dict
            The optimization history
        """

        if "history" not in self.cache:

            # Grab procedures
            needed_ids = [x for v in self.optimization_history.values() for x in v]
            objects = self.client.get_procedures({"id": needed_ids})
            procedures = {v.id: v for v in objects}

            # Move procedures into the correct order
            ret = {}
            for key, hashes in self.optimization_history.items():
                tmp = []
                for h in hashes:
                    tmp.append(procedures[h])
                ret[key] = tmp

            self.cache["history"] = ret

        return self.cache["history"]

    def final_energies(self, key=None):
        """
        Provides the final optimized energies at each grid point.

        Parameters
        ----------
        key : None, optional
            Specifies a single entry to pull from.


        Returns
        -------
        energy : float, dict
            Returns energies at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------

        >>> torsiondrive_obj.final_energies()
        {(-90,): -148.7641654446243, (180,): -148.76501336993732, (0,): -148.75056290106735, (90,): -148.7641654446148}
        """

        if key is None:
            return {self._deserialize_key(k): v for k, v in self.final_energy_dict.items()}
        else:

            return self.final_energy_dict[self._serialize_key(key)]

    def final_molecules(self, key=None):
        """Returns the optimized molecules at each grid point

        Parameters
        ----------
        key : None, optional
            Specifies a single entry to pull from.


        Returns
        -------
        energy : dict
            Returns molecule at each grid point in a dictionary or at a
            single point if a key is specified.

        Examples
        --------

        >>> torsiondrive_obj.final_energies()
        {(-90,):{'symbols': ['H', 'O', 'O', 'H'], 'geometry': [1.72669422, 1.28135788, ... }
        """

        if "final_molecules" not in self.cache:

            ret = {}
            for k, tasks in self.get_history().items():
                minpos = self.minimum_positions[k]

                ret[k] = tasks[minpos].get_final_molecule()

            self.cache["final_molecules"] = ret

        data = self.cache["final_molecules"]

        if key is None:
            return {self._deserialize_key(k): copy.deepcopy(v) for k, v in data.items()}
        else:

            return data[self._serialize_key(key)]
