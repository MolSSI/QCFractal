"""
A ORM for Crank
"""

import json


class CrankORM:
    """
    A interface to the raw JSON data of a Crank torsion scan run.
    """

    # Maps {internal_state : FractalServer state}
    __json_mapper = {
        "_id": "id",
        "_state": "state",

        # Options
        "_optimization_history": "optimization_history",
        "_initial_molecule_id": "initial_molecule",
        "_crank_options": "crank_meta",
        "_geometric_options": "geometric_meta",
        "_qc_options": "qc_meta",

        # Energies
        "_final_energies": "final_energies",
    }

    def __init__(self, initial_molecule, **kwargs):
        """Initializes a CrankORM object, from local data.

        This object may be able to submit jobs to the server in the future.

        *Prototype object, may change in the future.

        Parameters
        ----------
        initial_molecule : TYPE
            Description
        kwargs:
            See CrankORM.from_json

        """
        self._initial_molecule = initial_molecule

        # Set kwargs
        for k in self.__json_mapper.keys():
            setattr(self, k, kwargs.get(k[1:], None))

    @classmethod
    def from_json(cls, data):
        """
        Creates a CrankORM object from FractalServer data.

        Parameters
        ----------
        data : dict
            A JSON blob from FractalServer:
                - "id": The service id of the blob
                - "state": The current state of the job ("WAITING", "RUNNING", "FINISHED")
                - "initial_molecule": The id of the submitted molecule
                - "crank_meta": The option submitted to the Crank method
                - "geometric_meta": The options submitted to the Geometric method called by Crank
                - "qc_meta": The program, options, method, and basis to be run by Geometric.
                - "final_energies": A dictionary of final energies if the Crank service is finished

        Returns
        -------
        crank_obj : CrankORM
            A CrankORM object from the specified JSON.

        """
        kwargs = {}
        for k, v in CrankORM.__json_mapper.items():
            if v in data:
                kwargs[k[1:]] = data[v]

        if ("final_energies" in kwargs) and (kwargs["final_energies"] is not None):
            kwargs["final_energies"] = {tuple(json.loads(k)): v for k, v in kwargs["final_energies"].items()}

        return cls(None, **kwargs)

    def __repr__(self):
        """
        Simplified crank string representation.

        Returns
        -------
        ret : str
            A representation of the current Crank state.

        Examples
        --------

        >>> repr(crank_obj)
        Crank(id='5b7f1fd57b87872d2c5d0a6d', state='FINISHED', molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
        """

        ret = "Crank("
        ret += "id='{}', ".format(self._id)
        ret += "state='{}', ".format(self._state)
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

        >>> crank_obj.final_energies()
        {(-90,): -148.7641654446243, (180,): -148.76501336993732, (0,): -148.75056290106735, (90,): -148.7641654446148}
        """

        if self._state != "FINISHED":
            raise KeyError("{} has not completed. Unable to show final energies.".format(self))

        if key is None:
            return self._final_energies.copy()
        else:
            if isinstance(key, (int, float)):
                key = (int(key), )

            return self._final_energies[key]
