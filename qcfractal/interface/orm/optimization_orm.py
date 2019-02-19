"""
A ORM for Optimization results
"""

import json


class OptimizationORM:
    """
    A interface to the raw JSON data of a Optimization result.
    """

    # Maps {internal_status : FractalServer status}
    __json_mapper = {
        "_id": "id",
        "_success": "success",
        "hash_index": "hash_index",

        # Keywords
        "_program": "program",
        "_qc_options": "qc_meta",
        "_initial_molecule_id": "initial_molecule",
        "_final_molecule_id": "final_molecule",
        "_trajectory": "trajectory",
        "_energies": "energies",
    }

    def __init__(self, initial_molecule, **kwargs):
        """Initializes a OptimizationORM object, from local data.

        This object may be able to submit tasks to the server in the future.

        *Prototype object, may change in the future.

        Parameters
        ----------
        initial_molecule : TYPE
            Description
        kwargs:
            See OptimizationORM.from_json

        """
        self._initial_molecule = initial_molecule
        self._client = kwargs.pop("client", None)

        # Set kwargs
        for k in self.__json_mapper.keys():
            setattr(self, k, kwargs.get(k[1:], None))

    @classmethod
    def from_json(cls, data, client=None):
        """
        Creates a OptimizationORM object from FractalServer data.

        Parameters
        ----------
        data : dict
            A JSON blob from FractalServer:
                - "id": The service id of the blob
                - "success": If the optimization result was successful or not.
                - "program": The program used for the optimization run.
                - "qc_meta": The quantum chemistry options identified..
                - "initial_molecule_id": The id of the initial (submitted) molecule.
                - "final_molecule_id": The id of the optimizated molecule.
                - "trajectory": QC results for each step in the geometry optimization.
                - "energies": The final energies for each step in the geometry optimization.
        client : FractalClient, optional
            A activate server connection.

        Returns
        -------
        optimization_obj : OptimizationORM
            A OptimizationORM object from the specified JSON.

        """
        kwargs = {}
        for k, v in OptimizationORM.__json_mapper.items():
            if v in data:
                kwargs[k[1:]] = data[v]

        if ("final_energies" in kwargs) and (kwargs["final_energies"] is not None):
            kwargs["final_energies"] = {tuple(json.loads(k)): v for k, v in kwargs["final_energies"].items()}

        kwargs["client"] = client
        return cls(None, **kwargs)

    def __str__(self):
        """
        Simplified optimization string representation.

        Returns
        -------
        ret : str
            A representation of the current Optimization status.

        Examples
        --------

        >>> repr(optimization_obj)
        Optimization(id='5b7f1fd57b87872d2c5d0a6d', status='FINISHED', molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
        """

        ret = "Optimization("
        ret += "id='{}', ".format(self._id)
        ret += "success='{}', ".format(self._success)
        ret += "initial_molecule_id='{}', ".format(self._initial_molecule_id)

        name = None
        if self._initial_molecule:
            name = self._initial_molecule.name()

        ret += "initial_molecule_name='{}')".format(name)

        return ret

    @property
    def id(self):
        return self._id

    def energies(self):
        """A list of energies along the trajectory path.

        Returns
        -------
        list of float
            The energy of each point in [Eh]
        """
        return self._energies[:]

    def final_energy(self):
        """The final energy of the geometry optimization.

        Returns
        -------
        float
            The optimization molecular energy.
        """
        return self._energies[-1]

    def get_trajectory(self, projection=None):
        """Returns the raw documents for each gradient evaluation in the trajectory.

        Parameters
        ----------
        client : qcportal.FractalClient
            A active client connected to a server.
        projection : None, optional
            A dictionary of the project to apply to the document

        Returns
        -------
        list of dict
            A list of results documents
        """

        return self._client.get_results(id=self._trajectory)

    def final_molecule(self):
        """Returns the optimized molecule

        Returns
        -------
        Molecule
            The optimized molecule
        """

        ret = self._client.get_molecules([self._final_molecule_id], index="id")
        return ret[0]
