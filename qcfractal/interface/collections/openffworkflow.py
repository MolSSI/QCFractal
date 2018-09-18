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

    __required_fields = {
        "enumerate_states", "enumerate_fragments", "torsiondrive_input", "torsiondrive_meta", "optimization_meta",
        "qc_meta"
    }

    def __init__(self, name, options=None, client=None, **kwargs):
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
        super().__init__(name, client=client, options=options, **kwargs)

    def _init_collection_data(self, additional_args):
        options = additional_args.get("options", None)
        if options is None:
            raise KeyError("No record of OpenFFWorkflow {} found and no initial options passed in.".format(name))

        ret = copy.deepcopy(options)
        ret["fragments"] = {}  # No known fragments
        ret["molecules"] = []

        ret["fragment_cache"] = {}  # Caches pulled fragment data

        return ret

    def _pre_save_prep(self, client):
        pass

    def get_options(self, key):
        if key not in self._option_sets:
            raise KeyError("Key `{}` not understood, key must be in {}.".format(self._option_sets))

        return copy.deepcopy(self._option_sets[key])

    def list_fragments(self):
        return copy.deepcopy(list(self.data["fragments"]))

    def list_initial_molecules(self):
        return copy.deepcopy(self.data["molecules"])

    def add_fragment(self, fragment_id, data, provenance={}):

        if fragment_id not in self.data["fragments"]:
            self.data["fragments"][fragment_id] = {}

        frag_data = self.data["fragments"][fragment_id]
        for name, packet in data.items():
            if name in frag_data:
                print("Already found label {} for fragment_ID {}, skipping.".format(name, fragment_id))
                continue

            torsion_meta = copy.deepcopy(
                {k: self.data[k]
                 for k in ("torsiondrive_meta", "optimization_meta", "qc_meta")})

            for k in ["grid_spacing", "dihedrals"]:
                torsion_meta["torsiondrive_meta"][k] = packet[k]

            ret = self.client.add_service("torsiondrive", [packet["initial_molecule"]], torsion_meta)

            hash_lists = []
            [hash_lists.extend(x) for x in ret.values()]

            if len(hash_lists) != 1:
                raise KeyError("Something went very wrong.")

            hash_index = hash_lists[0]
            frag_data[name] = hash_index

        self.data["fragments"][fragment_id] = frag_data

    def get_data(self):

        lookup = []
        for k, v in self.data["fragments"].items():
            lookup.extend(list(v.values()))

        data = self.client.get_procedures({"hash_index": lookup})
        data = {x._hash_index: x for x in data}

        ret = {}
        for frag, reqs in self.data["fragments"].items():
            ret[frag] = {}
            for label, hash_index in reqs.items():
                try:
                    ret[frag][label] = {json.dumps(k):v for k, v in data[hash_index].final_energies().items()}
                except KeyError:
                    ret[frag][label] = None


        return ret

