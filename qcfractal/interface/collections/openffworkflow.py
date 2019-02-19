"""Mongo QCDB Fragment object and helpers
"""

import copy
from typing import Any, Dict

from ..models.torsiondrive import TorsionDrive, TorsionDriveInput
from ..orm import OptimizationORM
from .collection import Collection
from .collection_utils import register_collection


class OpenFFWorkflow(Collection):
    """
    This is a QCA OpenFFWorkflow class.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    """

    def __init__(self, name, client=None, **kwargs):
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
        super().__init__(name, client=client, **kwargs)

        self._torsiondrive_cache = {}

        # First workflow is saved
        if self.data.id == self.data.fields['id'].default:
            ret = self.save()
            if len(ret) == 0:
                raise ValueError("Attempted to insert duplicate Workflow with name '{}'".format(name))
            self.data.id = ret[0]

    class DataModel(Collection.DataModel):
        """
        Internal Data structure base model typed by PyDantic

        This structure validates input, allows server-side validation and data security,
        and will create the information to pass back and forth between server and client
        """
        fragments: Dict[str, Any] = {}
        enumerate_states: Dict[str, Any] = {
            "version": "",
            "options": {
                "protonation": True,
                "tautomers": False,
                "stereoisomers": True,
                "max_states": 200,
                "level": 0,
                "reasonable": True,
                "carbon_hybridization": True,
                "suppress_hydrogen": True
            }
        }
        enumerate_fragments: Dict[str, Any] = {"version": "", "options": {}}
        torsiondrive_input: Dict[str, Any] = {
            "restricted": True,
            "torsiondrive_options": {
                "max_conf": 1,
                "terminal_torsion_resolution": 30,
                "internal_torsion_resolution": 30,
                "scan_internal_terminal_combination": 0,
                "scan_dimension": 1
            },
            "restricted_optimization_options": {
                "maximum_rotation": 30,
                "interval": 5
            }
        }
        torsiondrive_static_options: Dict[str, Any] = {
            "keywords": {},
            "optimization_spec": {
                "program": "geometric",
                "keywords": {
                    "coordsys": "tric",
                }
            },
            "qc_spec": {
                "driver": "gradient",
                "method": "UFF",
                "basis": "",
                "keywords": None,
                "program": "rdkit",
            }
        }
        optimization_static_options: Dict[str, Any] = {
            "optimization_spec": {
                "program": "geometric",
                "keywords": {
                    "coordsys": "tric",
                }
            },
            "qc_spec": {
                "driver": "gradient",
                "method": "UFF",
                "basis": "",
                "keywords": None,
                "program": "rdkit",
            },
        }

    # Valid options which can be fetched from the get_options method
    # Kept as separate list to be easier to read for devs
    __workflow_options = ("enumerate_states", "enumerate_fragments", "torsiondrive_input",
                          "torsiondrive_static_options", "optimization_static_options")

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

    def add_fragment(self, fragment_id, data, provenance=None):
        """
        Adds a new fragment to the workflow along with the associated input required.

        Parameters
        ----------
        fragment_id : str
            The tag associated with fragment. In general this should be the canonical isomeric
            explicit hydrogen mapped SMILES tag for this fragment.
        data : dict
            A dictionary of label : {type, intial_molecule, grid_spacing, dihedrals} for torsiondrive type and
            label : {type, initial_molecule, contraints} for an optimization type

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
        if provenance is None:
            provenance = {}

        if fragment_id not in self.data.fragments:
            self.data.fragments[fragment_id] = {}

        frag_data = self.data.fragments[fragment_id]
        for name, packet in data.items():
            if name in frag_data:
                print("Already found label {} for fragment_ID {}, skipping.".format(name, fragment_id))
                continue
            if packet['type'] == 'torsiondrive_input':
                ret = self._add_torsiondrive(packet)
            elif packet['type'] == 'optimization_input':
                ret = self._add_optimize(packet)
            else:
                raise KeyError("{} is not an openffworklow type job".format(packet['type']))

            # add back to fragment data
            packet["id"] = ret
            # packet["provenance"] = provenance
            frag_data[name] = packet

        # Push collection data back to server
        self.save(overwrite=True)

    def _add_torsiondrive(self, packet):
        # Build out a new service
        torsion_meta = copy.deepcopy({
            k: self.data.torsiondrive_static_options[k]
            for k in ("keywords", "optimization_spec", "qc_spec")
        })

        for k in ["grid_spacing", "dihedrals"]:
            torsion_meta["keywords"][k] = packet[k]

        # Get hash of torsion
        inp = TorsionDriveInput(**torsion_meta, initial_molecule=packet["initial_molecule"])
        ret = self.client.add_service([inp])

        return ret.ids[0]

    def _add_optimize(self, packet):
        meta = copy.deepcopy({k: self.data.optimization_static_options[k] for k in ("keywords", "qc_spec", "program")})

        meta["keywords"] = {"values": meta.pop("keywords"), "program": meta["program"]}
        for k in ["constraints"]:
            meta["keywords"]["values"][k] = packet[k]

        # Get hash of optimization
        ret = self.client.add_procedure("optimization", meta["program"], meta, [packet["initial_molecule"]])

        return ret.ids[0]

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
            lookup.extend([v["id"] for v in self.data.fragments[frag].values()])

        if refresh_cache is False:
            lookup = list(set(lookup) - self._torsiondrive_cache.keys())

        # Grab the data and update cache
        data = self.client.get_procedures({"id": lookup})
        self._torsiondrive_cache.update({x.id: x for x in data})

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
                if v["id"] in self._torsiondrive_cache:
                    # TODO figure out a better solution here
                    obj = self._torsiondrive_cache[v["id"]]
                    if isinstance(obj, TorsionDrive):
                        tmp[k] = obj.final_energies()
                    elif isinstance(obj, OptimizationORM):
                        tmp[k] = obj.final_energy()
                    else:
                        raise TypeError("Internal type error encoured, buy a dev a coffee.")
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
                if v["id"] in self._torsiondrive_cache:
                    obj = self._torsiondrive_cache[v["id"]]
                    if isinstance(obj, TorsionDrive):
                        tmp[k] = obj.final_molecules()
                    elif isinstance(obj, OptimizationORM):
                        tmp[k] = obj.final_molecule()
                    else:
                        raise TypeError("Internal type error encoured, buy a dev a coffee.")
                else:
                    tmp[k] = None

            ret[frag] = tmp

        return ret


register_collection(OpenFFWorkflow)
