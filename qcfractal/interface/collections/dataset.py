"""
QCPortal Database ODM
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel
from qcelemental import constants

from ..statistics import wrap_statistics
from .collection import Collection
from .collection_utils import register_collection


class MoleculeRecord(BaseModel):
    name: str
    molecule_id: str
    comment: Optional[str] = None
    local_results: Dict[str, Any] = {}


class ContributedValues(BaseModel):
    name: str
    doi: Optional[str] = None
    theory_level: Union[str, Dict[str, str]]
    theory_level_details: Union[str, Dict[str, str]] = None
    comments: Optional[str] = None
    values: Dict[str, Any]
    units: str


class Dataset(Collection):
    """
    The Dataset class for homogeneous computations on many molecules.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    data : dict
        JSON representation of the database backbone
    df : pd.DataFrame
        The underlying dataframe for the Dataset object
    """

    def __init__(self, name, client=None, **kwargs):
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
        """
        super().__init__(name, client=client, **kwargs)

        # Initialize internal data frames
        self.df = pd.DataFrame(index=self.get_index())

        # If we making a new database we may need new hashes and json objects
        self._new_molecules = {}
        self._new_keywords = {}
        self._new_records = []
        self._updated_state = False

    class DataModel(Collection.DataModel):

        # Defaults
        default_program: Optional[str] = None
        default_keywords: Dict[str, str] = {}
        default_driver: str = "energy"
        alias_keywords: Dict[str, Dict[str, str]] = {}

        # Data
        records: List[MoleculeRecord] = []
        contributed_values: Dict[str, ContributedValues] = {}

        # History, driver, program, method (basis, options)
        history: Set[Tuple[str, str, str, Optional[str], Optional[str]]] = set()
        history_keys: Tuple[str, str, str, str, str] = ("driver", "program", "method", "basis", "keywords")

    def _check_state(self):
        if self._new_molecules or self._new_keywords or self._new_records or self._updated_state:
            raise ValueError("New molecules, keywords, or records detected, run save before submitting new tasks.")

    def _canonical_pre_save(self, client):

        for k in list(self._new_keywords.keys()):
            ret = client.add_keywords([self._new_keywords[k]])
            assert len(ret) == 1, "KeywordSet added incorrectly"
            self.data.alias_keywords[k[0]][k[1]] = ret[0]
            del self._new_keywords[k]
        self._updated_state = False

    def _add_molecules_by_dict(self, client, molecules):

        flat_map_keys = []
        flat_map_mols = []
        for k, v in molecules.items():
            flat_map_keys.append(k)
            flat_map_mols.append(v)

        mol_ret = client.add_molecules(flat_map_mols)

        return {k: v for k, v in zip(flat_map_keys, mol_ret)}

    def _pre_save_prep(self, client):
        self._canonical_pre_save(client)

        # Preps any new molecules introduced to the Dataset before storing data.
        mol_ret = self._add_molecules_by_dict(client, self._new_molecules)

        # Update internal molecule UUID's to servers UUID's
        for record in self._new_records:
            new_record = record.copy(update={"molecule_id": mol_ret[record.molecule_id]})
            self.data.records.append(new_record)

        self._new_records = []
        self._new_molecules = {}

    def _add_history(self, **history: Dict[str, Optional[str]]) -> None:
        """
        Adds compute history to the dataset
        """
        if history.keys() != set(self.data.history_keys):
            raise KeyError("Internal error: Incorrect history keys passed in.")

        new_history = []
        for key in self.data.history_keys:
            value = history[key]
            if value is not None:
                value = value.lower()
            new_history.append(value)

        self.data.history.add(tuple(new_history))

    def list_history(self, **search: Dict[str, Optional[str]]) -> 'DataFrame':
        """
        Lists the history of computations completed.

        Parameters
        ----------
        **search : Dict[str, Optional[str]]
            Allows searching to narrow down return.

        Returns
        -------
        DataFrame
            The computed keys.

        """

        if not (search.keys() <= set(self.data.history_keys)):
            raise KeyError("Not all query keys were understood.")

        df = pd.DataFrame(list(self.data.history), columns=self.data.history_keys)

        for key, value in search.items():
            if value is not None:
                value = value.lower()
                df = df[df[key] == value]
            else:
                df = df[df[key].isnull()]

        df.set_index(list(self.data.history_keys[:-1]), inplace=True)
        df.sort_index(inplace=True)
        return df

    def _default_parameters(self, driver, keywords, program):
        """
        Takes raw input parsed parameters and applies defaults to them.
        """

        if program is None:
            if self.data.default_program is None:
                raise KeyError("No default program was set and none was provided.")
            program = self.data.default_program
        else:
            program = program.lower()

        if driver is None:
            driver = self.data.default_driver

        keywords_alias = keywords
        if keywords is None:
            if program in self.data.default_keywords:
                keywords_alias = self.data.default_keywords[program]
                keywords = self.data.alias_keywords[program][keywords_alias]
        else:
            if (program not in self.data.alias_keywords) or (keywords not in self.data.alias_keywords[program]):
                raise KeyError("KeywordSet alias '{}' not found for program '{}'.".format(keywords, program))

            keywords_alias = keywords
            keywords = self.data.alias_keywords[program][keywords]

        return driver, keywords, keywords_alias, program

    def _query(self, indexer, query, field="return_result", scale=None):
        """
        Runs a query based on an indexer which is index : molecule_id
        """
        self._check_state()

        field = field.lower()

        query["molecule"] = set(indexer.values())

        query["projection"] = {"molecule": True, field: True}
        records = pd.DataFrame(self.client.get_results(**query), columns=["molecule", field])

        ret = pd.DataFrame.from_dict(indexer, orient="index", columns=["molecule"])
        ret.reset_index(inplace=True)
        ret = ret.merge(records, how="left", on="molecule")
        ret.rename(columns={field: "result"}, inplace=True)
        ret.set_index("index", inplace=True)
        ret.drop("molecule", axis=1, inplace=True)

        if scale:
            ret[ret.select_dtypes(include=['number']).columns] *= constants.conversion_factor('hartree', scale)

        return ret

    def set_default_program(self, program: str) -> bool:
        """
        Sets the default program.
        """

        self.data.default_program = program.lower()

    def add_keywords(self, alias: str, program: str, keyword: 'KeywordSet', default: bool=False) -> bool:
        """
        Adds an option alias to the dataset. Not that keywords are not present
        until a save call has been completed.

        Parameters
        ----------
        alias : str
            The alias of the option
        keyword : KeywordSet
            The Keywords object to use.
        default : bool
            Sets this option as the default for the program
        """

        alias = alias.lower()
        program = program.lower()
        if program not in self.data.alias_keywords:
            self.data.alias_keywords[program] = {}

        if alias in self.data.alias_keywords[program]:
            raise KeyError("Alias '{}' already set for program {}.".format(alias, keyword.program))

        self._new_keywords[(program, alias)] = keyword

        if default:
            self.data.default_keywords[program] = alias
        return True

    def get_keywords(self, alias: str, program: str) -> 'KeywordSet':

        if self.client is None:
            raise AttributeError("Dataset: Client was not set.")

        alias = alias.lower()
        program = program.lower()
        if (program not in self.data.alias_keywords) or (alias not in self.data.alias_keywords[program]):
            raise KeyError("Keywords {}: {} not found.".format(program, alias))

        kwid = self.data.alias_keywords[program][alias]
        return self.client.get_keywords([kwid])[0]

    def add_contributed_values(self, contrib: ContributedValues, overwrite=False) -> None:
        """Adds a ContributedValues to the database.

        Parameters
        ----------
        contrib : ContributedValues
            The ContributedValues to add.
        overwrite : bool, optional
            Forces

        """

        # Convert and validate
        if isinstance(contrib, ContributedValues):
            contrib = contrib.copy()
        else:
            contrib = ContributedValues(**contrib)

        # Check the key
        key = contrib.name.lower()
        if (key in self.data.contributed_values) and (overwrite is False):
            raise KeyError(
                "Key '{}' already found in contributed values. Use `overwrite=True` to force an update.".format(key))

        self.data.contributed_values[key] = contrib
        self._updated_state = True

    def list_contributed_values(self) -> List[str]:
        """
        Lists the known keys for all contributed values.
        """

        return list(self.data.contributed_values)

    def get_contributed_values(self, key: str) -> ContributedValues:
        """Returns a copy of the requested ContributedValues object.

        Parameters
        ----------
        key : str
            The ContributedValues object key.

        Returns
        -------
        ContributedValues
            The requested ContributedValues object.
        """
        return self.data.contributed_values[key.lower()].copy()

    def get_contributed_values_column(self, key: str, scale='hartree') -> 'Series':
        """Returns a Pandas column with the requested contributed values

        Parameters
        ----------
        key : str
            The ContributedValues object key.
        scale : None, optional
            All units are based in Hartree, the default scaling is to kcal/mol.

        Returns
        -------
        Series
            A pandas Series containing the request values.
        """
        data = self.get_contributed_values(key)

        # Annoying work around to prevent some pands magic
        if isinstance(next(iter(data.values.values())), (int, float)):
            values = data.values
        else:
            values = {k: [v] for k, v in data.values.items()}

        tmp_idx = pd.DataFrame.from_dict(values, orient="index", columns=[key])

        # Convert to numeric
        tmp_idx[tmp_idx.select_dtypes(include=['number']).columns] *= constants.conversion_factor(data.units, scale)

        return tmp_idx

    def add_entry(self, name, molecule, **kwargs):

        mhash = molecule.get_hash()
        self._new_molecules[mhash] = molecule
        self._new_records.append(MoleculeRecord(name=name, molecule_id=mhash, **kwargs))

    def query(self,
              method,
              basis=None,
              *,
              driver=None,
              keywords=None,
              program=None,
              contrib=False,
              scale="kcal / mol",
              field="return_result",
              as_array=False):
        """
        Queries the local Portal for the requested keys.

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
        contrib : bool
            Toggles a search between the Mongo Pages and the Databases's ContributedValues field.
        scale : str, double
            All units are based in Hartree, the default scaling is to kcal/mol.
        field : str, optional
            The result field to query on


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

        driver, keywords, keywords_alias, program = self._default_parameters(driver, keywords, program)

        if not contrib and (self.client is None):
            raise AttributeError("DataBase: FractalClient was not set.")

        query_keys = {
            "method": method,
            "basis": basis,
            "driver": driver,
            "keywords": keywords,
            "program": program,
        }
        # # If reaction results
        if contrib:
            tmp_idx = self.get_contributed_values_column(method, scale=scale)

        else:
            indexer = {e.name: e.molecule_id for e in self.data.records}

            tmp_idx = self._query(indexer, query_keys, field=field, scale=scale)
            tmp_idx.rename(columns={"result": method + '/' + basis}, inplace=True)

        if as_array:
            tmp_idx[tmp_idx.columns[0]] = tmp_idx[tmp_idx.columns[0]].apply(lambda x: np.array(x))

        # Apply to df
        self.df[tmp_idx.columns] = tmp_idx

        return True

    def compute(self, method, basis, driver=None, keywords=None, program=None, ignore_ds_type=False):
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
            raise AttributeError("Dataset: Compute: Client was not set.")

        driver, keywords, keywords_alias, program = self._default_parameters(driver, keywords, program)

        molecule_idx = [e.molecule_id for e in self.data.records]
        umols, uidx = np.unique(molecule_idx, return_index=True)

        ret = self.client.add_compute(program, method, basis, driver, keywords, list(umols))

        # Update the record that this was computed
        self._add_history(driver=driver, program=program, method=method, basis=basis, keywords=keywords_alias)
        self.save()

        return ret

    def get_index(self):
        """
        Returns the current index of the database.

        Returns
        -------
        ret : list of str
            The names of all reactions in the database
        """
        return [x.name for x in self.data.records]

    # Statistical quantities
    def statistics(self, stype, value, bench="Benchmark"):
        """Summary

        Parameters
        ----------
        stype : str
            The type of statistic in question
        value : str
            The method string to compare
        bench : str, optional
            The benchmark method for the comparison

        Returns
        -------
        ret : pd.DataFrame, pd.Series, float
            Returns a DataFrame, Series, or float with the requested statistics depending on input.
        """
        return wrap_statistics(stype, self.df, value, bench)

    # Getters
    def __getitem__(self, args):
        """A wrapped to the underlying pd.DataFrame to access columnar data

        Parameters
        ----------
        args : str
            The column to access

        Returns
        -------
        ret : pd.Series, pd.DataFrame
            A view of the underlying dataframe data
        """
        return self.df[args]


register_collection(Dataset)
