"""
A ORM for Crank
"""

import json

class CrankORM:

    __json_mapper = {
        "_id": "id",
        "_state": "state",

        # Options
        "_initial_molecule_id": "initial_molecule",
        "_crank_options": "crank_meta",
        "_geometric_options": "geometric_meta",
        "_qc_options": "qc_meta",

        # Energies
        "_final_energies": "final_energies",
    }

    def __init__(self, initial_molecule, **kwargs):

        self.initial_molecule = initial_molecule

        # Set kwargs
        for k in self.__json_mapper.keys():
            setattr(self, k, kwargs.get(k[1:], None))

    @classmethod
    def from_json(cls, data):

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
        """

        ret = "Crank("
        ret += "id={}, ".format(self._id)
        ret += "state={}, ".format(self._state)

        name = None
        if self.initial_molecule:
            name = self.initial_molecule.name()

        ret += "initial_molecule={})".format(name)

        return ret

    def final_energies(self, key=None):

        if self._state != "FINISHED":
            raise KeyError("{} has not completed. Unable to show final energies.".format(self))

        if key is None:
            return self._final_energies.copy()
        else:
            if isinstance(key, (int, float)):
                key = (int(key), )

            return self._final_energies[key]
