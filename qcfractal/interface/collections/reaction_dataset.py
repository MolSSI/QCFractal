"""
QCPortal Database ODM
"""
import itertools as it
from enum import Enum
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel

from ..constants import get_scale
from ..dict_utils import replace_dict_keys
from ..models.common_models import Molecule
from .collection_utils import nCr, register_collection
from .dataset import Dataset


class _ReactionTypeEnum(str, Enum):
    """Helper class for locking the reaction type into one or the other"""
    rxn = 'rxn'
    ie = 'ie'


class ReactionRecord(BaseModel):
    """Data model for the `reactions` list in Dataset"""
    attributes: Dict[str, Union[int, float, str]]  # Might be overloaded key types
    reaction_results: Dict[str, dict]
    name: str
    stoichiometry: Dict[str, Dict[str, float]]


class ReactionDataset(Dataset):
    """
    The ReactionDataset class for homogeneous computations on many reactions.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    data : dict
        JSON representation of the database backbone
    df : pd.DataFrame
        The underlying dataframe for the Dataset object
    rxn_index : pd.Index
        The unrolled reaction index for all reactions in the Dataset
    """

    def __init__(self, name, client=None, ds_type="rxn", **kwargs):
        """
        Initializer for the Dataset object. If no Portal is supplied or the database name
        is not present on the server that the Portal is connected to a blank database will be
        created.

        Parameters
        ----------
        name : str
            The name of the Dataset
        client : client.FractalClient, optional
            A Portal client to connected to a server
        ds_type : str, optional
            The type of Dataset involved

        """
        ds_type = ds_type.lower()
        super().__init__(name, client=client, ds_type=ds_type, **kwargs)

        # Internal data
        self.rxn_index = pd.DataFrame()
        self.df = pd.DataFrame()

        # Initialize internal data frames
        self.df = pd.DataFrame(index=self.get_index())

        self.rxn_index = None
        self._form_index()

        # If we making a new database we may need new hashes and json objects
        self._new_molecules = {}
        self._new_keywords = {}
        self._new_records = []

    class DataModel(Dataset.DataModel):

        ds_type: _ReactionTypeEnum = _ReactionTypeEnum.rxn
        records: List[ReactionRecord] = []

    def _check_state(self):
        if self._new_molecules or self._new_keywords:
            raise ValueError("New molecules or keywords detected, run save before submitting new tasks.")

    def _form_index(self):
        # Unroll the index
        tmp_index = []
        for rxn in self.data.records:
            name = rxn.name
            for stoich_name in list(rxn.stoichiometry):
                for mol_hash, coef in rxn.stoichiometry[stoich_name].items():
                    tmp_index.append([name, stoich_name, mol_hash, coef])

        self.rxn_index = pd.DataFrame(tmp_index, columns=["name", "stoichiometry", "molecule", "coefficient"])

    def _pre_save_prep(self, client):
        self._canonical_pre_save(client)

        mol_ret = self._add_molecules_by_dict(client, self._new_molecules)

        # Update internal molecule UUID's to servers UUID's
        for record in self._new_records:
            stoichiometry = replace_dict_keys(record.stoichiometry, mol_ret)
            new_record = record.copy(update={"stoichiometry": stoichiometry})
            self.data.records.append(new_record)

        self._new_records = []
        self._new_molecules = {}

        self._form_index()

    def _unroll_query(self, keys, stoich, field="return_result"):
        """Unrolls a complex query into a "flat" query for the server object

        Parameters
        ----------
        keys : dict
            Server query fields
        stoich : str
            The stoichiometry to access for the query (default/cp/cp3/etc)
        field : str, optional
            The results field to query on

        Returns
        -------
        ret : pd.DataFrame
            A DataFrame representation of the unrolled query
        """
        self._check_state()

        tmp_idx = self.rxn_index[self.rxn_index["stoichiometry"] == stoich].copy()
        tmp_idx = tmp_idx.reset_index(drop=True)

        # There could be duplicates so take the unique and save the map
        umols, uidx = np.unique(tmp_idx["molecule"], return_index=True)

        # Evaluate the overall dataframe
        query_keys = {k: v for k, v in keys.items()}
        query_keys["molecule"] = list(umols)
        query_keys["projection"] = {field: True, "molecule": True}
        values = pd.DataFrame(self.client.get_results(**query_keys), columns=["molecule", field])

        # Join on molecule hash
        tmp_idx = tmp_idx.merge(values, how="left", on="molecule")

        # Apply stoich values
        tmp_idx[field] *= tmp_idx["coefficient"]
        tmp_idx = tmp_idx.drop(['stoichiometry', 'molecule', 'coefficient'], axis=1)

        # If *any* value is null in the stoich sum, the whole thing should be Null. Pandas is being too clever
        null_mask = tmp_idx.copy()
        null_mask[field] = null_mask[field].isnull()
        null_mask = null_mask.groupby(["name"]).sum() != False

        tmp_idx = tmp_idx.groupby(["name"]).sum()
        tmp_idx[null_mask] = np.nan

        return tmp_idx

    def query(self,
              method,
              basis,
              driver=None,
              keywords=None,
              program=None,
              stoich="default",
              prefix="",
              postfix="",
              reaction_results=False,
              scale="kcal",
              field="return_result",
              ignore_ds_type=False):
        """
        Queries the local Portal for the requested keys and stoichiometry.

        Parameters
        ----------
        method : str
            The computational method to query on (B3LYP)
        basis : str
            The computational basis query on (6-31G)
        driver : str, optional
            Search within energy, gradient, etc computations
        keywords : str, optional
            The option token desired
        program : str, optional
            The program to query on
        stoich : str
            The given stoichiometry to compute.
        prefix : str
            A prefix given to the resulting column names.
        postfix : str
            A postfix given to the resulting column names.
        reaction_results : bool
            Toggles a search between the Mongo Pages and the Databases's reaction_results field.
        scale : str, double
            All units are based in Hartree, the default scaling is to kcal/mol.
        field : str, optional
            The result field to query on
        ignore_ds_type : bool
            Override of "ie" for "rxn" db types.


        Returns
        -------
        success : bool
            Returns True if the requested query was successful or not.

        Notes
        -----


        Examples
        --------

        ds.query("B3LYP", "aug-cc-pVDZ", stoich="cp", prefix="cp-")

        """

        driver, keywords, program = self._default_parameters(driver, keywords, program)

        if not reaction_results and (self.client is None):
            raise AttributeError("DataBase: FractalClient was not set.")

        query_keys = {
            "method": method.lower(),
            "basis": basis.lower(),
            "driver": driver.lower(),
            "keywords": keywords,
            "program": program.lower(),
        }
        # # If reaction results
        if reaction_results:
            tmp_idx = pd.Series(index=self.df.index)
            for rxn in self.data.records:
                try:
                    tmp_idx.loc[rxn.name] = rxn.reaction_results[stoich][method]
                except KeyError:
                    pass

            # Convert to numeric
            tmp_idx = pd.to_numeric(tmp_idx, errors='ignore')
            tmp_idx *= get_scale(scale)

            self.df[prefix + method + postfix] = tmp_idx
            return True

        # if self.data.ds_type.lower() == "ie":
        #     _ie_helper(..)

        if (not ignore_ds_type) and (self.data.ds_type.lower() == "ie"):
            monomer_stoich = ''.join([x for x in stoich if not x.isdigit()]) + '1'
            tmp_idx_complex = self._unroll_query(query_keys, stoich, field=field)
            tmp_idx_monomers = self._unroll_query(query_keys, monomer_stoich, field=field)

            # Combine
            tmp_idx = tmp_idx_complex - tmp_idx_monomers

        else:
            tmp_idx = self._unroll_query(query_keys, stoich, field=field)
        tmp_idx.columns = [prefix + method + '/' + basis + postfix for _ in tmp_idx.columns]

        # scale
        tmp_idx = tmp_idx.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        tmp_idx[tmp_idx.select_dtypes(include=['number']).columns] *= get_scale(scale)

        # Apply to df
        self.df[tmp_idx.columns] = tmp_idx

        return True

    def compute(self, method, basis, driver=None, keywords=None, program=None, stoich="default", ignore_ds_type=False):
        """Executes a computational method for all reactions in the Dataset.
        Previously completed computations are not repeated.

        Parameters
        ----------
        method : str
            The computational method to compute (B3LYP)
        basis : str
            The computational basis to compute (6-31G)
        driver : str, optional
            The type of computation to run (energy, gradient, etc)
        stoich : str, optional
            The stoichiometry of the requested compute (cp/nocp/etc)
        keywords : str, optional
            The keyword alias for the requested compute
        program : str, optional
            The underlying QC program
        ignore_ds_type : bool, optional
            Optionally only compute the "default" geometry

        Returns
        -------
        ret : dict
            A dictionary of the keys for all requested computations
        """
        self._check_state()

        if self.client is None:
            raise AttributeError("DataBase: Compute: Client was not set.")

        driver, keywords, program = self._default_parameters(driver, keywords, program)

        # Figure out molecules that we need
        if (not ignore_ds_type) and (self.data.ds_type.lower() == "ie"):
            monomer_stoich = ''.join([x for x in stoich if not x.isdigit()]) + '1'
            tmp_monomer = self.rxn_index[self.rxn_index["stoichiometry"] == monomer_stoich].copy()
            tmp_complex = self.rxn_index[self.rxn_index["stoichiometry"] == stoich].copy()
            tmp_idx = pd.concat((tmp_monomer, tmp_complex), axis=0)
        else:
            tmp_idx = self.rxn_index[self.rxn_index["stoichiometry"] == stoich].copy()

        tmp_idx = tmp_idx.reset_index(drop=True)

        # There could be duplicates so take the unique and save the map
        umols, uidx = np.unique(tmp_idx["molecule"], return_index=True)

        complete_values = self.client.get_results(
            molecule=list(umols),
            driver=driver,
            keywords=keywords,
            program=program,
            method=method,
            basis=basis,
            projection={"molecule": True})

        complete_mols = np.array([x["molecule"] for x in complete_values])
        umols = np.setdiff1d(umols, complete_mols)
        compute_list = list(umols)

        ret = self.client.add_compute(program, method.lower(), basis.lower(), driver, keywords, compute_list)

        return ret

    def get_rxn(self, name):
        """
        Returns the JSON object of a specific reaction.

        Parameters
        ----------
        name : str
            The name of the reaction to query

        Returns
        -------
        ret : dict
            The JSON representation of the reaction

        """

        found = []
        for num, x in enumerate(self.data.records):
            if x.name == name:
                found.append(num)

        if len(found) == 0:
            raise KeyError("Dataset:get_rxn: Reaction name '{}' not found.".format(name))

        if len(found) > 1:
            raise KeyError("Dataset:get_rxn: Multiple reactions of name '{}' found. Dataset failure.".format(name))

        return self.data.records[found[0]]

    # Visualization
    def ternary(self, cvals=None):
        """Plots a ternary diagram of the DataBase if available

        Parameters
        ----------
        cvals : None, optional
            Description

        """
        raise Exception("MPL not avail")

#        return visualization.Ternary2D(self.df, cvals=cvals)

# Adders

    def parse_stoichiometry(self, stoichiometry):
        """
        Parses a stiochiometry list.

        Parameters
        ----------
        stoichiometry : list
            A list of tuples describing the stoichiometry.

        Returns
        -------
        stoich : list
            A list of formatted tuples describing the stoichiometry for use in a MongoDB.

        Notes
        -----
        This function attempts to convert the molecule into its correspond hash. The following will happen depending on the form of the Molecule.
            - Molecule hash - Used directly in the stoichiometry.
            - Molecule class - Hash is obtained and the molecule will be added to the database upon saving.
            - Molecule string - Molecule will be converted to a Molecule class and the same process as the above will occur.


        Examples
        --------


        """

        ret = {}

        mol_hashes = []
        mol_values = []

        for line in stoichiometry:
            if len(line) != 2:
                raise KeyError("Dataset: Parse stoichiometry: passed in as a list must of key : value type")

            # Get the values
            try:
                mol_values.append(float(line[1]))
            except:
                raise TypeError("Dataset: Parse stoichiometry: must be able to cast second value must be as float.")

            # What kind of molecule is it?
            mol = line[0]

            # This is a molecule hash, should be in the database
            if isinstance(mol, str) and (len(mol) == 40):
                molecule_hash = mol

            elif isinstance(mol, str):
                qcf_mol = Molecule.from_data(mol)

                molecule_hash = qcf_mol.get_hash()

                if molecule_hash not in list(self._new_molecules):
                    self._new_molecules[molecule_hash] = qcf_mol.json_dict()

            elif isinstance(mol, Molecule):
                molecule_hash = mol.get_hash()

                if molecule_hash not in list(self._new_molecules):
                    self._new_molecules[molecule_hash] = mol.json_dict()

            else:
                raise TypeError("Dataset: Parse stoichiometry: first value must either be a molecule hash, "
                                "a molecule str, or a Molecule class.")

            mol_hashes.append(molecule_hash)

        # Sum together the coefficients of duplicates
        ret = {}
        for mol, coef in zip(mol_hashes, mol_values):
            if mol in list(ret):
                ret[mol] += coef
            else:
                ret[mol] = coef

        return ret

    def add_rxn(self, name, stoichiometry, reaction_results=None, attributes=None, other_fields=None):
        """
        Adds a reaction to a database object.

        Parameters
        ----------
        name : str
            Name of the reaction.
        stoichiometry : list or dict
            Either a list or dictionary of lists
        reaction_results :  dict or None, Optional, Default: None
            A dictionary of the computed total interaction energy results
        attributes :  dict or None, Optional, Default: None
            A dictionary of attributes to assign to the reaction
        other_fields : dict or None, Optional, Default: None
            A dictionary of additional user defined fields to add to the reaction entry

        Notes
        -----

        Examples
        --------

        Returns
        -------
        ret : dict
            A complete JSON specification of the reaction


        """
        if reaction_results is None:
            reaction_results = {}
        if attributes is None:
            attributes = {}
        if other_fields is None:
            other_fields = {}
        rxn_dict = {"name": name}

        # Set name
        if name in self.get_index():
            raise KeyError("Dataset: Name '{}' already exists. "
                           "Please either delete this entry or call the update function.".format(name))

        # Set stoich
        if isinstance(stoichiometry, dict):
            rxn_dict["stoichiometry"] = {}

            if "default" not in list(stoichiometry):
                raise KeyError("Dataset:add_rxn: Stoichiometry dict must have a 'default' key.")

            for k, v in stoichiometry.items():
                rxn_dict["stoichiometry"][k] = self.parse_stoichiometry(v)

        elif isinstance(stoichiometry, (tuple, list)):
            rxn_dict["stoichiometry"] = {}
            rxn_dict["stoichiometry"]["default"] = self.parse_stoichiometry(stoichiometry)
        else:
            raise TypeError("Dataset:add_rxn: Type of stoichiometry input was not recognized:", type(stoichiometry))

        # Set attributes
        if not isinstance(attributes, dict):
            raise TypeError("Dataset:add_rxn: attributes must be a dictionary, not '{}'".format(type(attributes)))

        rxn_dict["attributes"] = attributes

        if not isinstance(other_fields, dict):
            raise TypeError("Dataset:add_rxn: other_fields must be a dictionary, not '{}'".format(type(attributes)))

        for k, v in other_fields.items():
            rxn_dict[k] = v

        if "default" in list(reaction_results):
            rxn_dict["reaction_results"] = reaction_results
        elif isinstance(reaction_results, dict):
            rxn_dict["reaction_results"] = {}
            rxn_dict["reaction_results"]["default"] = reaction_results
        else:
            raise TypeError("Passed in reaction_results not understood.")

        rxn = ReactionRecord(**rxn_dict)

        self._new_records.append(rxn)

        return rxn

    def add_ie_rxn(self, name, mol, **kwargs):
        """Add a interaction energy reaction entry to the database. Automatically
        builds CP and no-CP reactions for the fragmented molecule.

        Parameters
        ----------
        name : str
            The name of the reaction
        mol : Molecule
            A molecule with multiple fragments
        **kwargs
            Additional kwargs to pass into `build_id_fragments`.

        Returns
        -------
        ret : dict
            A JSON representation of the new reaction.
        """
        reaction_results = kwargs.pop("reaction_results", {})
        attributes = kwargs.pop("attributes", {})
        other_fields = kwargs.pop("other_fields", {})

        stoichiometry = self.build_ie_fragments(mol, name=name, **kwargs)
        return self.add_rxn(
            name, stoichiometry, reaction_results=reaction_results, attributes=attributes, other_fields=other_fields)

    @staticmethod
    def build_ie_fragments(mol, **kwargs):
        """
        Build the stoichiometry for an Interaction Energy.

        Parameters
        ----------
        mol : Molecule class or str
            Molecule to fragment.
        do_default : bool
            Create the default (noCP) stoichiometry.
        do_cp : bool
            Create the counterpoise (CP) corrected stoichiometry.
        do_vmfc : bool
            Create the Valiron-Mayer Function Counterpoise (VMFC) corrected stoichiometry.
        max_nbody : int
            What is the maximum fragment level built, if zero defaults to the maximum number of fragments.

        Notes
        -----

        Examples
        --------

        Returns
        -------
        ret : dict
            A JSON representation of the fragmented molecule.

        """

        do_default = kwargs.pop("do_default", True)
        do_cp = kwargs.pop("do_cp", True)
        do_vmfc = kwargs.pop("do_vmfc", False)
        max_nbody = kwargs.pop("max_nbody", 0)

        if not isinstance(mol, Molecule):

            mol = Molecule.from_data(mol, **kwargs)

        ret = {}

        max_frag = len(mol.fragments)
        if max_nbody == 0:
            max_nbody = max_frag

        if max_nbody < 2:
            raise AttributeError("Dataset:build_ie_fragments: Molecule must have at least two fragments.")

        # Build some info
        fragment_range = list(range(max_frag))

        # Loop over the bodis
        for nbody in range(1, max_nbody):
            nocp_tmp = []
            cp_tmp = []
            for k in range(1, nbody + 1):
                take_nk = nCr(max_frag - k - 1, nbody - k)
                sign = ((-1)**(nbody - k))
                coef = take_nk * sign
                for frag in it.combinations(fragment_range, k):
                    if do_default:
                        nocp_tmp.append((mol.get_fragment(frag, orient=True), coef))
                    if do_cp:
                        ghost = list(set(fragment_range) - set(frag))
                        cp_tmp.append((mol.get_fragment(frag, ghost, orient=True), coef))

            if do_default:
                ret["default" + str(nbody)] = nocp_tmp

            if do_cp:
                ret["cp" + str(nbody)] = cp_tmp

        # VMFC is a special beast
        if do_vmfc:
            raise KeyError("VMFC isnt quite ready for primetime!")

            # ret.update({"vmfc" + str(nbody): [] for nbody in range(1, max_nbody)})
            # nbody_range = list(range(1, max_nbody))
            # for nbody in nbody_range:
            #     for cp_combos in it.combinations(fragment_range, nbody):
            #         basis_tuple = tuple(cp_combos)
            #         for interior_nbody in nbody_range:
            #             for x in it.combinations(cp_combos, interior_nbody):
            #                 ghost = list(set(basis_tuple) - set(x))
            #                 ret["vmfc" + str(interior_nbody)].append((mol.get_fragment(x, ghost), 1.0))

        # Add in the maximal position
        if do_default:
            ret["default"] = [(mol, 1.0)]

        if do_cp:
            ret["cp"] = [(mol, 1.0)]

        # if do_vmfc:
        #     ret["vmfc"] = [(mol, 1.0)]

        return ret

register_collection(ReactionDataset)
