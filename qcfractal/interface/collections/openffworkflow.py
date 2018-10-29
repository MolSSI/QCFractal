"""Mongo QCDB Fragment object and helpers
"""

import copy

from . import collection_utils
from .collection import Collection

from typing import Dict


class OpenFFWorkflow(Collection):
    """
    This is a QCA OpenFFWorkflow class.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    """

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

        if client is None:
            raise KeyError("OpenFFWorkflow must have a client.")
        # Expand options
        if options is None:
            raise KeyError("No record of OpenFFWorkflow {} found and no initial options passed in.".format(name))
        super().__init__(name, client=client, **options, **kwargs)

        self._torsiondrive_cache = {}

        # First workflow is saved
        if self.data.id == self.data.fields['id'].default:
            ret = self.save()
            if len(ret) == 0:
                raise ValueError("Attempted to insert duplicate Workflow with name '{}'".format(name))
            self.data.id = ret[0][1]

    class DataModel(Collection.DataModel):
        """
        Internal Data structure base model typed by PyDantic

        This structure validates input, allows server-side validation and data security,
        and will create the information to pass back and forth between server and client
        """
        fragments: dict = {}
        enumerate_states: dict = {}
        enumerate_fragments: dict = {}
        torsiondrive_input: dict = {}
        torsiondrive_meta: dict = {}
        optimization_meta: Dict[str, str] = {
                "program": "geometric",
                "coordsys": "tric",
            }
        qc_meta: Dict[str, str] = {
                "driver": "gradient",
                "method": "UFF",
                "basis": "",
                "options": "none",
                "program": "rdkit",
            }

    # Valid options which can be fetched from the get_options method
    # Kept as separate list to be easier to read for devs
    __workflow_options = ("enumerate_states", "enumerate_fragments", "torsiondrive_input", "torsiondrive_meta",
                          "optimization_meta", "qc_meta")

    def _pre_save_prep(self, client):
        pass

    def get_options(self, key):
        """
        Obtains "base" workflow options that do not change.

        Parameters
        ----------
        key : str
            The original workflow options.

        Returns
        -------
        dict
            The requested options dictionary.
        """
        # Get the set of options unique to the Workflow data model
        if key not in self.__workflow_options:
            raise KeyError("Key `{}` not understood.".format(key))

        return copy.deepcopy(getattr(self.data, key))

    def list_fragments(self):
        """
        List all fragments associated with this workflow.

        Returns
        -------
        list of str
            A list of fragment id's.
        """
        return list(self.data.fragments)

    def add_fragment(self, fragment_id, data, provenance={}):
        """
        Adds a new fragment to the workflow along with the associated torsiondrives required.

        Parameters
        ----------
        fragment_id : str
            The tag associated with fragment. In general this should be the canonical isomeric
            explicit hydrogen mapped SMILES tag for this fragment.
        data : dict
            A dictionary of label : {initial_molecule, grid_spacing, dihedrals} keys.

        provenance : dict, optional
            The provenance of the fragments creation

        Example
        -------

        data = {
           "label1": {
                "initial_molecule": ptl.data.get_molecule("butane.json"),
                "grid_spacing": [60],
                "dihedrals": [[0, 2, 3, 1]],
            },
            ...
        }
        wf.add_fragment("CCCC", data=)
        """
        if fragment_id not in self.data.fragments:
            self.data.fragments[fragment_id] = {}

        frag_data = self.data.fragments[fragment_id]
        for name, packet in data.items():
            if name in frag_data:
                print("Already found label {} for fragment_ID {}, skipping.".format(name, fragment_id))
                continue

            # Build out a new service
            torsion_meta = copy.deepcopy(
                {k: getattr(self.data, k)
                 for k in ("torsiondrive_meta", "optimization_meta", "qc_meta")})

            for k in ["grid_spacing", "dihedrals"]:
                torsion_meta["torsiondrive_meta"][k] = packet[k]

            # Get hash of torsion
            ret = self.client.add_service("torsiondrive", [packet["initial_molecule"]], torsion_meta)

            hash_lists = []
            [hash_lists.extend(x) for x in ret.values()]

            if len(hash_lists) != 1:
                raise KeyError("Something went very wrong.")

            # add back to fragment data
            packet["hash_index"] = hash_lists[0]
            frag_data[name] = packet

        # Push collection data back to server
        self.save(overwrite=True)

    def get_fragment_data(self, fragments=None, refresh_cache=False):
        """Obtains fragment torsiondrives from server to local data.

        Parameters
        ----------
        fragments : None, optional
            A list of fragment ID's to query upon
        refresh_cache : bool, optional
            If True requery everything, otherwise use the cache to prevent extra lookups.
        """

        # If no fragments explicitly shown, grab all
        if fragments is None:
            fragments = self.data.fragments.keys()

        # Figure out the lookup
        lookup = []
        for frag in fragments:
            lookup.extend([v["hash_index"] for v in self.data.fragments[frag].values()])

        if refresh_cache is False:
            lookup = list(set(lookup) - self._torsiondrive_cache.keys())

        # Grab the data and update cache
        data = self.client.get_procedures({"hash_index": lookup})
        self._torsiondrive_cache.update({x._hash_index: x for x in data})

    def list_final_energies(self, fragments=None, refresh_cache=False):
        """
        Returns the final energies for the requested fragments.

        Parameters
        ----------
        fragments : None, optional
            A list of fragment ID's to query upon
        refresh_cache : bool, optional
            If True requery everything, otherwise use the cache to prevent extra lookups.

        Returns
        -------
        dict
            A dictionary structure with fragment and label fields available for access.
        """

        # If no fragments explicitly shown, grab all
        if fragments is None:
            fragments = self.data.fragments.keys()

        # Get the data if available
        self.get_fragment_data(fragments=fragments, refresh_cache=refresh_cache)

        ret = {}
        for frag in fragments:
            tmp = {}
            for k, v in self.data.fragments[frag].items():
                if v["hash_index"] in self._torsiondrive_cache:
                    tmp[k] = self._torsiondrive_cache[v["hash_index"]].final_energies()
                else:
                    tmp[k] = None

            ret[frag] = tmp

        return ret

    def list_final_molecules(self, fragments=None, refresh_cache=False):
        """
        Returns the final molecules for the requested fragments.

        Parameters
        ----------
        fragments : None, optional
            A list of fragment ID's to query upon
        refresh_cache : bool, optional
            If True requery everything, otherwise use the cache to prevent extra lookups.

        Returns
        -------
        dict
            A dictionary structure with fragment and label fields available for access.
        """

        # If no fragments explicitly shown, grab all
        if fragments is None:
            fragments = self.data.fragments.keys()

        # Get the data if available
        self.get_fragment_data(fragments=fragments, refresh_cache=refresh_cache)

        ret = {}
        for frag in fragments:
            tmp = {}
            for k, v in self.data.fragments[frag].items():
                if v["hash_index"] in self._torsiondrive_cache:
                    tmp[k] = self._torsiondrive_cache[v["hash_index"]].final_molecules()
                else:
                    tmp[k] = None

            ret[frag] = tmp

        return ret


collection_utils.register_collection(OpenFFWorkflow)
