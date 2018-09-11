"""Mongo QCDB Fragment object and helpers
"""

import json
import copy

from .. import molecule
from .collection import Collection


class BioFragment(Collection):
    """
    This is a QCA Fragment class.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    """

    __required_fields = {"initial_molecule"}

    def __init__(self, name, initial_molecule=None, client=None, **kwargs):
        """
        Initializer for the Database object. If no Portal is supplied or the database name
        is not present on the server that the Portal is connected to a blank database will be
        created.

        Parameters
        ----------
        name : str
            The name of the Database
        client : client.FractalClient, optional
            A Portal client to connect to a server

        """
        super().__init__(name, client=client, initial_molecule=initial_molecule)

    def _init_collection_data(self, additional_args):
        mol = additional_args.get("initial_molecule", None)
        if mol is None:
            raise KeyError("No record of Fragment {} found and no initial molecule passed in.".format(name))

        if self.client is None:
            raise KeyError("BioFragment must be initialized with a FractalClient.")

        ret = self.client.add_molecules({"initial_molecule": mol})

        return {
            "initial_molecule": mol.to_json(),
            "initial_molecule_id": ret["initial_molecule"],
            "options": {"torsiondrive": {}},
            "torsiondrives": {}
        }

    def _pre_save_prep(self, client):
        pass

    def add_options_set(self, dtype, key, options):

        # Find the options category
        if dtype not in ["torsiondrive"]:
            raise KeyError("Options set of type {} not understood.".format(dtype))

        # Check for duplicates
        opts = self.data["options"][dtype]
        if key in opts:
            raise KeyError("Attempted to set options of type {} with key {}, duplicate key found.".format(dtype, key))

        # Validate options sets
        if dtype == "torsiondrive":
            req_keys = {"torsiondrive_meta", "optimization_meta", "qc_meta"}
            if len(options.keys() ^ req_keys):
                raise KeyError("'options' input for type {} must have keys {}.".format(dtype, req_keys))
        else:
            raise KeyError("Initial dtype check is wrong, please make an issue on GitHub.")

        # Set options
        opts[key] = options

    def submit_torsion_drives(self, options_set, torsions):

        # Grab the options key
        try:
            options = copy.deepcopy(self.data["options"]["torsiondrive"][options_set])
        except KeyError:
            raise KeyError("Options set of type {} for key {} not found.".format("torsiondrive", options_set))

        if len(torsions.keys() ^ {"internal", "terminal"}):
            raise KeyError("'torsions' input must have keys {}".format({"internal", "terminal"}))

        if options_set not in self.data["torsiondrives"]:
            self.data["torsiondrives"] = {}

        # Pull out different spacing
        spacing = {
            "internal": options["torsiondrive_meta"]["internal_grid_spacing"],
            "terminal": options["torsiondrive_meta"]["terminal_grid_spacing"]
        }
        options["torsiondrive_meta"] = {}
        options = json.dumps(options)

        # Loop over all spacing types and build jobs
        submissions = []
        for ttype in ["internal", "terminal"]:

            for t in torsions[ttype]:
                # Overwrite options
                tmp_options = json.loads(options)
                tmp_options["torsiondrive_meta"]["grid_spacing"] = spacing[ttype]
                tmp_options["torsiondrive_meta"]["dihedrals"] = t

                ret = self.client.add_service("torsiondrive", [self.data["initial_molecule_id"]], tmp_options)

