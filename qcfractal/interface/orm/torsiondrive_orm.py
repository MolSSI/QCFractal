"""
A ORM for TorsionDrive
"""

import copy
import json

__all__ = ["TorsionDriveORM"]

class TorsionDriveORM:
    """
    A interface to the raw JSON data of a TorsionDrive torsion scan run.
    """

    # Maps {internal_status : FractalServer status}
    __json_mapper = {
        "_id": "id",
        "_success": "success",
        "_hash_index": "hash_index",

        # Options
        "_optimization_history": "optimization_history",
        "_initial_molecule_id": "initial_molecule",
        "_final_molecule_id": "final_molecule",
        "_torsiondrive_options": "torsiondrive_meta",
        "_geometric_options": "geometric_meta",
        "_qc_options": "qc_meta",

        # Energies
        "_final_energies": "final_energies",
    }

    def __init__(self, initial_molecule, **kwargs):
        """Initializes a TorsionDriveORM object, from local data.

        This object may be able to submit jobs to the server in the future.

        *Prototype object, may change in the future.

        Parameters
        ----------
        initial_molecule : TYPE
            Description
        kwargs:
            See TorsionDriveORM.from_json

        """
        self._initial_molecule = initial_molecule
        self._client = kwargs.pop("client", None)

        # Set kwargs
        for k in self.__json_mapper.keys():
            setattr(self, k, kwargs.get(k[1:], None))

        self._cache = {}

    @classmethod
    def from_json(cls, data, client=None):
        """
        Creates a TorsionDriveORM object from FractalServer data.

        Parameters
        ----------
        data : dict
            A JSON blob from FractalServer:
                - "id": The service id of the blob
                - "success": If the procedure was successful or not.
                - "initial_molecule": The id of the submitted molecule
                - "torsiondrive_meta": The option submitted to the TorsionDrive method
                - "geometric_meta": The options submitted to the Geometric method called by TorsionDrive
                - "qc_meta": The program, options, method, and basis to be run by Geometric.
                - "final_energies": A dictionary of final energies if the TorsionDrive service is finished
        client : FractalClient
            A server connection to

        Returns
        -------
        torsiondrive_obj : TorsionDriveORM
            A TorsionDriveORM object from the specified JSON.

        """
        kwargs = {}
        for k, v in TorsionDriveORM.__json_mapper.items():
            if v in data:
                kwargs[k[1:]] = data[v]
            else:
                kwargs[k[1:]] = None

        if ("final_energies" in kwargs) and (kwargs["final_energies"] is not None):
            kwargs["final_energies"] = {tuple(json.loads(k)): v for k, v in kwargs["final_energies"].items()}

        self._client = client

        return cls(None, **kwargs)

    def _check_success(self):
        if not self._success:
            raise KeyError("{} has not completed or failed. Unable to process request.".format(self))

    def _check_client(self):
        if self._client is None:
            raise KeyError("{} requires a FractalClient to aquire the requested information.".format(self))

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
        TorsionDrive(id='5b7f1fd57b87872d2c5d0a6d', success=True, molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
        """

        ret = "TorsionDrive("
        ret += "id='{}', ".format(self._id)
        ret += "success='{}', ".format(self._success)
        ret += "molecule_id='{}', ".format(self._initial_molecule_id)

        name = None
        if self._initial_molecule:
            name = self._initial_molecule.name()

        ret += "molecule_name='{}')".format(name)

        return ret

    def final_energies(self, key=None):
        """
        Provides the final optimized energies at each grid point.

        Parameters
        ----------
        key : None, optional
            Returns the final energy at a single grid point.


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

        self._check_success()

        if key is None:
            return self._final_energies.copy()
        else:
            if isinstance(key, (int, float)):
                key = (int(key), )

            return self._final_energies[key]

    def final_molecule(self):
        """Returns the optimized molecule

        Returns
        -------
        Molecule
            The optimized molecule
        """
        self._check_success()
        self._check_client()

        if "final_molecule" not in self._cache:
            self._cache["final_molecule"] = self._client.get_molecules({"mol": self._final_molecule_id}, index="id")["mol"]

        return copy.deepcopy(self._cache["final_molecule"])

