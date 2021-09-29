"""
QCPortal Database ODM
"""
import itertools as it
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
from qcelemental import constants

from ..models import Molecule, ProtoModel
from ..util import replace_dict_keys
from .collection_utils import nCr, register_collection
from .dataset import Dataset

if TYPE_CHECKING:  # pragma: no cover
    from .. import FractalClient
    from ..models import ComputeResponse


class _ReactionTypeEnum(str, Enum):
    """Helper class for locking the reaction type into one or the other"""

    rxn = "rxn"
    ie = "ie"


class ReactionEntry(ProtoModel):
    """Data model for the `reactions` list in Dataset"""

    attributes: Dict[str, Union[int, float, str]]  # Might be overloaded key types
    reaction_results: Dict[str, dict]
    name: str
    stoichiometry: Dict[str, Dict[str, float]]
    extras: Dict[str, Any] = {}


class ReactionDataset(Dataset):
    """
    The ReactionDataset class for homogeneous computations on many reactions.

    Attributes
    ----------
    client : client.FractalClient
        A FractalClient connected to a server
    data : ReactionDataset.DataModel
        A Model representation of the database backbone
    df : pd.DataFrame
        The underlying dataframe for the Dataset object
    rxn_index : pd.Index
        The unrolled reaction index for all reactions in the Dataset
    """

    def __init__(self, name: str, client: Optional["FractalClient"] = None, ds_type: str = "rxn", **kwargs) -> None:
        """
        Initializer for the Dataset object. If no Portal is supplied or the database name
        is not present on the server that the Portal is connected to a blank database will be
        created.

        Parameters
        ----------
        name : str
            The name of the Dataset
        client : client.FractalClient, optional
            A FractalClient connected to a server
        ds_type : str, optional
            The type of Dataset involved

        """
        ds_type = ds_type.lower()
        super().__init__(name, client=client, ds_type=ds_type, **kwargs)

    class DataModel(Dataset.DataModel):

        ds_type: _ReactionTypeEnum = _ReactionTypeEnum.rxn
        records: Optional[List[ReactionEntry]] = None

        history: Set[Tuple[str, str, str, Optional[str], Optional[str], str]] = set()
        history_keys: Tuple[str, str, str, str, str, str] = (
            "driver",
            "program",
            "method",
            "basis",
            "keywords",
            "stoichiometry",
        )

    def _entry_index(self, subset: Optional[List[str]] = None) -> None:
        if self.data.records is None:
            self._get_data_records_from_db()
        # Unroll the index
        tmp_index = []
        for rxn in self.data.records:
            name = rxn.name
            for stoich_name in list(rxn.stoichiometry):
                for mol_hash, coef in rxn.stoichiometry[stoich_name].items():
                    tmp_index.append([name, stoich_name, mol_hash, coef])
        ret = pd.DataFrame(tmp_index, columns=["name", "stoichiometry", "molecule", "coefficient"])
        if subset is None:
            return ret
        else:
            return ret.reset_index().set_index("name").loc[subset].reset_index().set_index("index")

    def _molecule_indexer(
        self,
        stoich: Union[str, List[str]],
        subset: Optional[Union[str, Set[str]]] = None,
        coefficients: bool = False,
        force: bool = False,
    ) -> Tuple[Dict[Tuple[str, ...], "ObjectId"], Tuple[str]]:
        """Provides a {index: molecule_id} mapping for a given subset.

        Parameters
        ----------
        stoich : Union[str, List[str]]
            The stoichiometries, or list of stoichiometries to return
        subset : Optional[Union[str, Set[str]]], optional
            The indices of the desired subset. Return all indices if subset is None.
        coefficients : bool, optional
            Returns the coefficients if as part of the index if True

        No Longer Returned
        ------------------
        Dict[str, 'ObjectId']
            Molecule index to molecule ObjectId map

        Returns
        -------
        Tuple[Dict[Tuple[str, ...], 'ObjectId'], Tuple[str]]
            Molecule index to molecule ObjectId map, and index names
        """
        if isinstance(stoich, str):
            stoich = [stoich]

        index = self.get_entries(subset=subset, force=force)
        matched_rows = index[np.in1d(index["stoichiometry"], stoich)]

        if subset:
            matched_rows = matched_rows[np.in1d(matched_rows["name"], subset)]

        names = ("name", "stoichiometry", "idx")
        if coefficients:
            names = names + ("coefficient",)

        ret = {}
        for gb_idx, group in matched_rows.groupby(["name", "stoichiometry"]):
            for cnt, (idx, row) in enumerate(group.iterrows()):
                if coefficients:
                    ret[gb_idx + (cnt, row["coefficient"])] = row["molecule"]
                else:
                    ret[gb_idx + (cnt,)] = row["molecule"]

        return ret, names

    def valid_stoich(self, subset=None, force: bool = False) -> Set[str]:
        entries = self.get_entries(subset=subset, force=force)
        return set(entries["stoichiometry"].unique())

    def _validate_stoich(self, stoich: Union[List[str], str], subset=None, force: bool = False) -> None:
        if isinstance(stoich, str):
            stoich = [stoich]
        if isinstance(subset, str):
            subset = [subset]
        valid_stoich = self.valid_stoich(subset=subset, force=force)
        for s in stoich:
            if s.lower() not in valid_stoich:
                raise KeyError("Stoichiometry not understood, valid keys are {}.".format(valid_stoich))

    def _pre_save_prep(self, client: "FractalClient") -> None:
        self._canonical_pre_save(client)

        mol_ret = self._add_molecules_by_dict(client, self._new_molecules)

        # Update internal molecule UUID's to servers UUID's
        for record in self._new_records:
            stoichiometry = replace_dict_keys(record.stoichiometry, mol_ret)
            new_record = record.copy(update={"stoichiometry": stoichiometry})
            self.data.records.append(new_record)

        self._new_records: List[ReactionEntry] = []
        self._new_molecules = {}

        self._entry_index()

    def get_values(
        self,
        method: Optional[Union[str, List[str]]] = None,
        basis: Optional[Union[str, List[str]]] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        driver: Optional[str] = None,
        stoich: str = "default",
        name: Optional[Union[str, List[str]]] = None,
        native: Optional[bool] = None,
        subset: Optional[Union[str, List[str]]] = None,
        force: bool = False,
    ) -> pd.DataFrame:
        """
        Obtains values from the known history from the search paramaters provided for the expected `return_result` values.
        Defaults to the standard programs and keywords if not provided.

        Note that unlike `get_records`, `get_values` will automatically expand searches and return multiple method
        and basis combinations simultaneously.

        `None` is a wildcard selector. To search for `None`, use `"None"`.

        Parameters
        ----------
        method : Optional[Union[str, List[str]]], optional
            The computational method (B3LYP)
        basis : Optional[Union[str, List[str]]], optional
            The computational basis (6-31G)
        keywords : Optional[str], optional
            The keyword alias
        program : Optional[str], optional
            The underlying QC program
        driver : Optional[str], optional
            The type of calculation (e.g. energy, gradient, hessian, dipole...)
        stoich : str, optional
            Stoichiometry of the reaction.
        name : Optional[Union[str, List[str]]], optional
            Canonical name of the record. Overrides the above selectors.
        native: Optional[bool], optional
            True: only include data computed with QCFractal
            False: only include data contributed from outside sources
            None: include both
        subset: Optional[List[str]], optional
            The indices of the desired subset. Return all indices if subset is None.
        force : bool, optional
            Data is typically cached, forces a new query if True

        Returns
        ------
        DataFrame
           A DataFrame of values with columns corresponding to methods and rows corresponding to reaction entries.
           Contributed (native=False) columns are marked with "(contributed)" and may include units in square brackets
           if their units differ in dimensionality from the ReactionDataset's default units.
        """
        return self._get_values(
            method=method,
            basis=basis,
            keywords=keywords,
            program=program,
            driver=driver,
            stoich=stoich,
            name=name,
            native=native,
            subset=subset,
            force=force,
        )

    def _get_native_values(
        self,
        subset: Set[str],
        method: Optional[str] = None,
        basis: Optional[str] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        stoich: Optional[str] = None,
        name: Optional[str] = None,
        force: bool = False,
    ) -> pd.DataFrame:
        self._validate_stoich(stoich, subset=subset, force=force)

        # So that datasets with no records do not require a default program and default keywords
        if len(self.list_records()) == 0:
            return pd.DataFrame(index=self.get_index(subset))

        queries = self._form_queries(
            method=method, basis=basis, keywords=keywords, program=program, stoich=stoich, name=name
        )

        if len(queries) == 0:
            return pd.DataFrame(index=self.get_index(subset))

        stoich_complex = queries.pop("stoichiometry").values[0]
        stoich_monomer = "".join([x for x in stoich_complex if not x.isdigit()]) + "1"

        def _query_apply_coeffients(stoich, query):

            # Build the starting table
            indexer, names = self._molecule_indexer(stoich=stoich, coefficients=True, force=force)
            df = self._get_records(indexer, query, include=["return_result"], merge=True)
            df.index = pd.MultiIndex.from_tuples(df.index, names=names)
            df.reset_index(inplace=True)

            # Block out null values `groupby.sum()` will return 0 rather than NaN in all cases
            null_mask = df[["name", "return_result"]].copy()
            null_mask["return_result"] = null_mask["return_result"].isnull()
            null_mask = null_mask.groupby(["name"])["return_result"].sum() != False

            # Multiply by coefficients and sum
            df["return_result"] *= df["coefficient"]
            df = df.groupby(["name"])["return_result"].sum()
            df[null_mask] = np.nan
            return df

        names = []
        new_queries = []
        new_data = pd.DataFrame(index=subset)

        for _, query in queries.iterrows():

            query = query.replace({np.nan: None}).to_dict()
            qname = query["name"]
            names.append(qname)

            if force or not self._subset_in_cache(qname, subset):
                self._column_metadata[qname] = query
                new_queries.append(query)

        if not self._use_view(force):
            units: Dict[str, str] = {}
            for query in new_queries:
                qname = query.pop("name")
                if self.data.ds_type == _ReactionTypeEnum.ie:
                    # This implements 1-body counterpoise correction
                    # TODO: this will need to contain the logic for VMFC or other method-of-increments strategies
                    data_complex = _query_apply_coeffients(stoich_complex, query)
                    data_monomer = _query_apply_coeffients(stoich_monomer, query)
                    data = data_complex - data_monomer
                elif self.data.ds_type == _ReactionTypeEnum.rxn:
                    data = _query_apply_coeffients(stoich_complex, query)
                else:
                    raise ValueError(
                        f"ReactionDataset ds_type is not a member of _ReactionTypeEnum. (Got {self.data.ds_type}.)"
                    )

                new_data[qname] = data * constants.conversion_factor("hartree", self.units)
                query["name"] = qname
                units[qname] = self.units
        else:
            for query in new_queries:
                query["native"] = True
            new_data, units = self._view.get_values(new_queries)
            for query in new_queries:
                qname = query["name"]
                new_data[qname] = new_data[qname] * constants.conversion_factor(units[qname], self.units)

        for query in new_queries:
            qname = query["name"]
            self._column_metadata[qname].update({"native": True, "units": units[qname]})

        self._update_cache(new_data)
        return self.df.loc[subset, names]

    def visualize(
        self,
        method: Optional[str] = None,
        basis: Optional[str] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        stoich: str = "default",
        groupby: Optional[str] = None,
        metric: str = "UE",
        bench: Optional[str] = None,
        kind: str = "bar",
        return_figure: Optional[bool] = None,
        show_incomplete: bool = False,
    ) -> "plotly.Figure":
        """
        Parameters
        ----------
        method : Optional[str], optional
            Methods to query
        basis : Optional[str], optional
            Bases to query
        keywords : Optional[str], optional
            Keyword aliases to query
        program : Optional[str], optional
            Programs aliases to query
        stoich : str, optional
            Stoichiometry to query
        groupby : Optional[str], optional
            Groups the plot by this index.
        metric : str, optional
            The metric to use either UE (unsigned error) or URE (unsigned relative error)
        bench : Optional[str], optional
            The benchmark level of theory to use
        kind : str, optional
            The kind of chart to produce, either 'bar' or 'violin'
        return_figure : Optional[bool], optional
            If True, return the raw plotly figure. If False, returns a hosted iPlot. If None, return a iPlot display in Jupyter notebook and a raw plotly figure in all other circumstances.
        show_incomplete: bool, optional
            Display statistics method/basis set combinations where results are incomplete

        Returns
        -------
        plotly.Figure
            The requested figure.
        """

        query = {"method": method, "basis": basis, "keywords": keywords, "program": program, "stoichiometry": stoich}
        query = {k: v for k, v in query.items() if v is not None}

        return self._visualize(
            metric,
            bench,
            query=query,
            groupby=groupby,
            return_figure=return_figure,
            kind=kind,
            show_incomplete=show_incomplete,
        )

    def get_molecules(
        self,
        subset: Optional[Union[str, Set[str]]] = None,
        stoich: Union[str, List[str]] = "default",
        force: bool = False,
    ) -> pd.DataFrame:
        """Queries full Molecules from the database.

        Parameters
        ----------
        subset : Optional[Union[str, Set[str]]], optional
            The index subset to query on
        stoich : Union[str, List[str]], optional
            The stoichiometries to pull from, either a single or multiple stoichiometries
        force : bool, optional
            Force pull of molecules from server

        Return
        ------
        pd.DataFrame
            Indexed Molecules which match the stoich and subset string.
        """

        self._check_client()
        self._check_state()
        if isinstance(subset, str):
            subset = [subset]

        self._validate_stoich(stoich, subset=subset, force=force)

        indexer, names = self._molecule_indexer(stoich=stoich, subset=subset, force=force)
        df = self._get_molecules(indexer, force=force)
        df.index = pd.MultiIndex.from_tuples(df.index, names=names)

        return df

    def get_records(
        self,
        method: str,
        basis: Optional[str] = None,
        *,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        stoich: Union[str, List[str]] = "default",
        include: Optional[List[str]] = None,
        subset: Optional[Union[str, Set[str]]] = None,
    ) -> Union[pd.DataFrame, "ResultRecord"]:
        """
        Queries the local Portal for the requested keys and stoichiometry.

        Parameters
        ----------
        method : str
            The computational method to query on (B3LYP)
        basis : Optional[str], optional
            The computational basis to query on (6-31G)
        keywords : Optional[str], optional
            The option token desired
        program : Optional[str], optional
            The program to query on
        stoich : Union[str, List[str]], optional
            The given stoichiometry to compute.
        include : Optional[Dict[str, bool]], optional
            The attribute project to perform on the query, otherwise returns ResultRecord objects.
        subset : Optional[Union[str, Set[str]]], optional
            The index subset to query on

        Returns
        -------
        Union[pd.DataFrame, 'ResultRecord']
            The name of the queried column

        """

        self._check_client()
        self._check_state()

        method = method.upper()
        if isinstance(stoich, str):
            stoich = [stoich]

        ret = []
        for s in stoich:
            name, _, history = self._default_parameters(program, method, basis, keywords, stoich=s)
            history.pop("stoichiometry")
            indexer, names = self._molecule_indexer(stoich=s, subset=subset, force=True)
            df = self._get_records(
                indexer,
                history,
                include=include,
                merge=False,
                raise_on_plan="`get_records` can only be used for non-composite quantities. You likely queried a DFT+D method or similar that requires a combination of DFT and -D. Please query each piece separately.",
            )
            df = df[0]
            df.index = pd.MultiIndex.from_tuples(df.index, names=names)
            ret.append(df)

        ret = pd.concat(ret)
        ret.sort_index(inplace=True)

        return ret

    def compute(
        self,
        method: str,
        basis: Optional[str] = None,
        *,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        stoich: str = "default",
        ignore_ds_type: bool = False,
        tag: Optional[str] = None,
        priority: Optional[str] = None,
    ) -> "ComputeResponse":
        """Executes a computational method for all reactions in the Dataset.
        Previously completed computations are not repeated.

        Parameters
        ----------
        method : str
            The computational method to compute (B3LYP)
        basis : Optional[str], optional
            The computational basis to compute (6-31G)
        keywords : Optional[str], optional
            The keyword alias for the requested compute
        program : Optional[str], optional
            The underlying QC program
        stoich : str, optional
            The stoichiometry of the requested compute (cp/nocp/etc)
        ignore_ds_type : bool, optional
            Optionally only compute the "default" geometry
        tag : Optional[str], optional
            The queue tag to use when submitting compute requests.
        priority : Optional[str], optional
            The priority of the jobs low, medium, or high.

        Returns
        -------
        ComputeResponse
            An object that contains the submitted ObjectIds of the new compute. This object has the following fields:
              - ids: The ObjectId's of the task in the order of input molecules
              - submitted: A list of ObjectId's that were submitted to the compute queue
              - existing: A list of ObjectId's of tasks already in the database

        """
        self._check_client()
        self._check_state()

        entry_index = self.get_entries(force=True)

        self._validate_stoich(stoich, subset=None, force=True)
        compute_keys = {"program": program, "method": method, "basis": basis, "keywords": keywords, "stoich": stoich}

        # Figure out molecules that we need
        if (not ignore_ds_type) and (self.data.ds_type.lower() == "ie"):

            monomer_stoich = "".join([x for x in stoich if not x.isdigit()]) + "1"
            tmp_monomer = entry_index[entry_index["stoichiometry"] == monomer_stoich].copy()

            ret1 = self._compute(compute_keys, tmp_monomer["molecule"], tag, priority)

            tmp_complex = entry_index[entry_index["stoichiometry"] == stoich].copy()
            ret2 = self._compute(compute_keys, tmp_complex["molecule"], tag, priority)

            ret = ret1.merge(ret2)
        else:
            tmp_complex = entry_index[entry_index["stoichiometry"] == stoich].copy()
            ret = self._compute(compute_keys, tmp_complex["molecule"], tag, priority)

        # Update the record that this was computed
        self.save()

        return ret

    def get_rxn(self, name: str) -> ReactionEntry:
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

    def parse_stoichiometry(self, stoichiometry: List[Tuple[Union[Molecule, str], float]]) -> Dict[str, float]:
        """
        Parses a stiochiometry list.

        Parameters
        ----------
        stoichiometry : list
            A list of tuples describing the stoichiometry.

        Returns
        -------
        Dict[str, float]
            A dictionary describing the stoichiometry for use in the database.
            Keys are molecule hashes. Values are stoichiometric coefficients.

        Notes
        -----
        This function attempts to convert the molecule into its corresponding hash. The following will happen depending on the form of the Molecule.
            - Molecule hash - Used directly in the stoichiometry.
            - Molecule class - Hash is obtained and the molecule will be added to the database upon saving.
            - Molecule string - Molecule will be converted to a Molecule class and the same process as the above will occur.


        """

        mol_hashes = []
        mol_values = []

        for line in stoichiometry:
            if len(line) != 2:
                raise KeyError("Dataset: Parse stoichiometry: passed in as a list, must be of key : value type")

            # Get the values
            try:
                mol_values.append(float(line[1]))
            except:
                raise TypeError("Dataset: Parse stoichiometry: must be able to cast second value to a float.")

            # What kind of molecule is it?
            mol = line[0]

            if isinstance(mol, str) and (len(mol) == 40):
                molecule_hash = mol

            elif isinstance(mol, str):
                qcf_mol = Molecule.from_data(mol)

                molecule_hash = qcf_mol.get_hash()

                if molecule_hash not in list(self._new_molecules):
                    self._new_molecules[molecule_hash] = qcf_mol

            elif isinstance(mol, Molecule):
                molecule_hash = mol.get_hash()

                if molecule_hash not in list(self._new_molecules):
                    self._new_molecules[molecule_hash] = mol

            else:
                raise TypeError(
                    "Dataset: Parse stoichiometry: first value must either be a molecule hash, "
                    "a molecule str, or a Molecule class."
                )

            mol_hashes.append(molecule_hash)

        # Sum together the coefficients of duplicates
        ret: Dict[str, float] = {}
        for mol, coef in zip(mol_hashes, mol_values):
            if mol in list(ret):
                ret[mol] += coef
            else:
                ret[mol] = coef

        return ret

    def add_rxn(
        self,
        name: str,
        stoichiometry: Dict[str, List[Tuple[Molecule, float]]],
        reaction_results: Optional[Dict[str, str]] = None,
        attributes: Optional[Dict[str, Union[int, float, str]]] = None,
        other_fields: Optional[Dict[str, Any]] = None,
    ) -> ReactionEntry:
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

        Returns
        -------
        ReactionEntry
            A complete specification of the reaction
        """
        if reaction_results is None:
            reaction_results = {}
        if attributes is None:
            attributes = {}
        if other_fields is None:
            other_fields = {}
        rxn_dict: Dict[str, Any] = {"name": name}

        # Set name
        if name in self.get_index():
            raise KeyError(
                "Dataset: Name '{}' already exists. "
                "Please either delete this entry or call the update function.".format(name)
            )

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

        rxn_dict["extras"] = other_fields

        if "default" in list(reaction_results):
            rxn_dict["reaction_results"] = reaction_results
        elif isinstance(reaction_results, dict):
            rxn_dict["reaction_results"] = {}
            rxn_dict["reaction_results"]["default"] = reaction_results
        else:
            raise TypeError("Passed in reaction_results not understood.")

        rxn = ReactionEntry(**rxn_dict)
        self._new_records.append(rxn)

        return rxn

    def add_ie_rxn(self, name: str, mol: Molecule, **kwargs) -> ReactionEntry:
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
        ReactionEntry
            A representation of the new reaction.
        """
        reaction_results = kwargs.pop("reaction_results", {})
        attributes = kwargs.pop("attributes", {})
        other_fields = kwargs.pop("other_fields", {})

        stoichiometry = self.build_ie_fragments(mol, name=name, **kwargs)
        return self.add_rxn(
            name, stoichiometry, reaction_results=reaction_results, attributes=attributes, other_fields=other_fields
        )

    @staticmethod
    def build_ie_fragments(mol: Molecule, **kwargs) -> Dict[str, List[Tuple[Molecule, float]]]:
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
            The maximum fragment level built, if zero defaults to the maximum number of fragments.

        Notes
        -----

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
        if max_frag < 2:
            raise AttributeError("Dataset:build_ie_fragments: Molecule must have at least two fragments.")

        # Build some info
        fragment_range = list(range(max_frag))

        # Loop over the bodis
        for nbody in range(1, max_nbody):
            nocp_tmp = []
            cp_tmp = []
            for k in range(1, nbody + 1):
                take_nk = nCr(max_frag - k - 1, nbody - k)
                sign = (-1) ** (nbody - k)
                coef = take_nk * sign
                for frag in it.combinations(fragment_range, k):
                    if do_default:
                        nocp_tmp.append((mol.get_fragment(frag, orient=True, group_fragments=True), coef))
                    if do_cp:
                        ghost = list(set(fragment_range) - set(frag))
                        cp_tmp.append((mol.get_fragment(frag, ghost, orient=True, group_fragments=True), coef))

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
