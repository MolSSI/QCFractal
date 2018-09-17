"""Mongo QCDB Fragment object and helpers
"""

import json
import copy

from .collection import Collection


class OpenFFWorkflow(Collection):
    """
    This is a QCA OpenFFWorkflow class.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    """

    __required_fields = {"initial_molecule"}

    def __init__(self, name, options=None, **kwargs):
        """
        Initializer for the OpenFFWorkflow object. If no Portal is supplied or the database name
        is not present on the server that the Portal is connected to a blank database will be
        created.

        Parameters
        ----------
        name : str
            The name of the OpenFFWorkflow
        client : client.FractalClient, optional
            A Portal client to connect to a server

        """
        super().__init__(name, client=client, initial_molecule=initial_molecule)

        self._option_sets = {
            "enumerate_states", "enumerate_fragments", "torsiondrive_input", "torsiondrive_meta", "optimization_meta",
            "qc_meta"
        }

    def _init_collection_data(self, additional_args):
        options = additional_args.get("options", None)
        if options is None:
            raise KeyError("No record of OpenFFWorkflow {} found and no initial options passed in.".format(name))

        if len(options.keys() ^ self._option_sets):
            raise KeyError("'options' kwargs must have keys {}".format(self._option_sets))

        ret = copy.deepcopy(options)
        ret["fragments"] = [] # No known fragments
        ret["molecules"] = []

        return ret

    def _pre_save_prep(self, client):
        pass

    def get_options(self, key):
        if key not in self._option_sets:
            raise KeyError("Key `{}` not understood, key must be in {}.".format(self._option_sets))

        return copy.deepcopy(self._option_sets[key])

    def list_fragments(self):
        return copy.deepcopy(self.data["fragments"])

    def list_initial_molecules(self):
        return copy.deepcopy(self.data["molecules"])



