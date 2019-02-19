"""
A model for TorsionDrive
"""

import copy
import json
from typing import Any, Dict, List, Tuple, Union

from pydantic import BaseModel

from .common_models import (Molecule, OptimizationSpecification, Provenance, QCSpecification, hash_dictionary,
                            json_encoders)

__all__ = ["TorsionDriveInput", "TorsionDrive"]


class TorsionDriveInput(BaseModel):
    """
    A TorsionDrive Input base class
    """

    class TDKeywords(BaseModel):
        """
        TorsionDrive options
        """
        dihedrals: List[Tuple[int, int, int, int]]
        grid_spacing: List[int]

        class Config:
            extra = "allow"
            allow_mutation = False

    program: str = "torsiondrive"
    procedure: str = "torsiondrive"
    initial_molecule: List[Union[str, Molecule]]
    keywords: TDKeywords
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification

    def __init__(self, **data):
        mol = data["initial_molecule"]
        if isinstance(mol, (str, dict, Molecule)):
            data["initial_molecule"] = [mol]

        BaseModel.__init__(self, **data)

    class Config:
        allow_mutation = False
        json_encoders = json_encoders


class TorsionDrive(TorsionDriveInput):
    """
    A interface to the raw JSON data of a TorsionDrive torsion scan run.
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
    initial_molecule: List[str]
    final_energy_dict: Dict[str, float]
    optimization_history: Dict[str, List[str]]
    minimum_positions: Dict[str, int]

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
        Simplified torsiondrive string representation.

        Returns
        -------
        ret : str
            A representation of the current TorsionDrive status.

        Examples
        --------

        >>> repr(torsiondrive_obj)
        TorsionDrive(id='5b7f1fd57b87872d2c5d0a6d', success=True, molecule_id='5b7f1fd57b87872d2c5d0a6c')
        """

        ret = "TorsionDrive("
        ret += "id='{}', ".format(self.id)
        ret += "success='{}', ".format(self.success)
        ret += "initial_molecule='{}')".format(self.initial_molecule)

        return ret

## Utility

    def _serialize_key(self, key):
        if isinstance(key, (int, float)):
            key = (int(key), )

        return json.dumps(key)

    def _deserialize_key(self, key):
        return tuple(json.loads(key))

    def dict(self, *args, **kwargs):
        kwargs["exclude"] = (kwargs.pop("exclude", None) or set()) | {"client", "cache"}
        return super().dict(*args, **kwargs)

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))

    def get_hash_index(self):

        data = self.dict(
            include={"initial_molecule", "program", "procedure", "keywords", "optimization_spec", "qc_spec"})

        return hash_dictionary(data)

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
            procedures = {v._id: v for v in objects}

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

                ret[k] = tasks[minpos].final_molecule()

            self.cache["final_molecules"] = ret

        data = self.cache["final_molecules"]

        if key is None:
            return {self._deserialize_key(k): copy.deepcopy(v) for k, v in data.items()}
        else:

            return data[self._serialize_key(key)]
