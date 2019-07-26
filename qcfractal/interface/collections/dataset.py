"""
QCPortal Database ODM
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel
from qcelemental import constants

from .collection import Collection
from .collection_utils import composition_planner, register_collection
from ..models import ComputeResponse, Molecule, ObjectId
from ..statistics import wrap_statistics
from ..visualization import bar_plot, violin_plot


class MoleculeRecord(BaseModel):
    name: str
    molecule_id: ObjectId
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
        A FractalClient connected to a server
    data : dict
        JSON representation of the database backbone
    df : pd.DataFrame
        The underlying dataframe for the Dataset object
    """

    def __init__(self, name: str, client: Optional['FractalClient']=None, **kwargs: Dict[str, Any]):
        """
        Initializer for the Dataset object. If no Portal is supplied or the database name
        is not present on the server that the Portal is connected to a blank database will be
        created.

        Parameters
        ----------
        name : str
            The name of the Dataset
        client : Optional['FractalClient'], optional
            A Portal client to connected to a server
        **kwargs : Dict[str, Any]
            Additional kwargs to pass to the collection
        """
        super().__init__(name, client=client, **kwargs)

        self._units = self.data.default_units

        # If we making a new database we may need new hashes and json objects
        self._new_molecules = {}
        self._new_keywords = {}
        self._new_records = []
        self._updated_state = False

        # Initialize internal data frames and load in contrib
        self.df = pd.DataFrame(index=self.get_index())

        # Inherited classes need to call this themselves
        for cv in self.data.contributed_values.values():
            tmp_idx = self.get_contributed_values_column(cv.name)
            self.df[tmp_idx.columns[0]] = tmp_idx

    class DataModel(Collection.DataModel):

        # Defaults
        default_program: Optional[str] = None
        default_keywords: Dict[str, str] = {}
        default_driver: str = "energy"
        default_units: str = "kcal / mol"
        default_benchmark: Optional[str] = None

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

    def _pre_save_prep(self, client):
        self._canonical_pre_save(client)

        # Preps any new molecules introduced to the Dataset before storing data.
        mol_ret = self._add_molecules_by_dict(client, self._new_molecules)

        # Update internal molecule UUID's to servers UUID's
        for record in self._new_records:
            molecule_hash = record.pop("molecule_hash")
            new_record = MoleculeRecord(molecule_id=mol_ret[molecule_hash], **record)
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

    def list_history(self, dftd3: bool=False, get_base: bool=False, pretty: bool=True, **search: Dict[str, Optional[str]]) -> 'DataFrame':
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

        show_dftd3 = dftd3

        if not (search.keys() <= set(self.data.history_keys)):
            raise KeyError("Not all query keys were understood.")

        history = pd.DataFrame(list(self.data.history), columns=self.data.history_keys)

        # Build out -D3 combos
        dftd3 = history[history["program"] == "dftd3"].copy()
        dftd3["base"] = [x.split("-d3")[0] for x in dftd3["method"]]

        nondftd3 = history[history["program"] != "dftd3"]
        dftd3combo = nondftd3.merge(dftd3[["method", "base"]], left_on="method", right_on="base")
        dftd3combo["method"] = dftd3combo["method_y"]
        dftd3combo.drop(["method_x", "method_y", "base"], axis=1, inplace=True)

        history = pd.concat([history, dftd3combo], sort=False)
        history = history.reset_index()
        history.drop("index", axis=1, inplace=True)

        # Find the returned subset
        ret = history.copy()
        for key, value in search.items():
            if value is None:
                ret = ret[ret[key].isnull()]
            elif isinstance(value, str):
                value = value.lower()
                ret = ret[ret[key] == value]
            elif isinstance(value, (list, tuple)):
                query = [x.lower() for x in value]
                ret = ret[ret[key].isin(query)]
            else:
                raise TypeError(f"Search type {type(value)} not understood.")


        if show_dftd3 is False:
            ret = ret[ret["program"] != "dftd3"]

        if pretty:
            ret.fillna("None", inplace=True)

        ret.set_index(list(self.data.history_keys[:-1]), inplace=True)
        ret.sort_index(inplace=True)
        return ret

    def _get_history(self, force: bool = False, **search: Dict[str, Optional[str]]) -> 'DataFrame':
        """Queries for all history that matches the search

        Parameters
        ----------
        force : bool, optional
            Force a query even if the data is already present
        **search : Dict[str, Optional[str]]
            History query paramters

        Returns
        -------
        DataFrame
            A DataFrame of the queried parameters
        """

        queries = self.list_history(**search, dftd3=True, pretty=False).reset_index()
        if queries.shape[0] > 10:
            raise TypeError("More than 10 queries formed, please narrow the search.")

        # queries["name"] = None
        for name, query in queries.iterrows():

            query = query.replace({np.nan: None}).to_dict()
            query.pop("driver")
            if "stoichiometry" in query:
                query["stoich"] = query.pop("stoichiometry")
            queries.loc[name, "name"] = self.query(query.pop("method").upper(), force=force, **query)

        return queries

    def get_history(self,
                    method: Optional[str]=None,
                    basis: Optional[str]=None,
                    keywords: Optional[str]=None,
                    program: Optional[str]=None,
                    force: bool=False) -> 'DataFrame':
        """ Queries known history from the search paramaters provided. Defaults to the standard
        programs and keywords if not provided.

        Parameters
        ----------
        method : Optional[str]
            The computational method to compute (B3LYP)
        basis : Optional[str], optional
            The computational basis to compute (6-31G)
        keywords : Optional[str], optional
            The keyword alias for the requested compute
        program : Optional[str], optional
            The underlying QC program

        Returns
        -------
        DataFrame
            A DataFrame of the queried parameters
        """

        # Get default program/keywords
        name, dbkeys, history = self._default_parameters(program, "nan", "nan", keywords)

        for k, v in [("method", method), ("basis", basis)]:

            if v is not None:
                history[k] = v
            else:
                history.pop(k, None)

        return self._get_history(**history, force=force)

    def _visualize(self,
                   metric,
                   bench,
                   query: Dict[str, Optional[str]],
                   groupby: Optional[str]=None,
                   return_figure=None,
                   digits=3,
                   kind="bar") -> 'plotly.Figure':

        # Validate query dimensions
        list_queries = [k for k, v in query.items() if isinstance(v, (list, tuple))]
        if len(list_queries) > 2:
            raise TypeError("A maximum of two lists are allowed.")

        # Check kind
        kind = kind.lower()
        if kind not in ["bar", "violin"]:
            raise KeyError(f"Visualiztion kind must either be 'bar' or 'violin', found {kind}")

        # Check metric
        metric = metric.upper()
        if metric == "UE":
            ylabel = f"UE [{self.units}]"
        elif metric == "URE":
            ylabel = "URE [%]"
        else:
            raise KeyError('Metric {} not understood, available metrics: "UE", "URE"'.format(metric))

        if kind == "bar":
            ylabel = "M" + ylabel
            metric = "M" + metric

        # Are we a groupby?
        _valid_groupby = {"method", "basis", "keywords", "program", "stoic", "d3"}
        if groupby is not None:
            groupby = groupby.lower()
            if groupby not in _valid_groupby:
                raise KeyError(f"Groupby option {groupby} not understood.")
            if (groupby != "d3") and (groupby not in query):
                raise KeyError(
                    f"Groupby option {groupby} not found in query, must provide a search on this parameter.")

            if (groupby != "d3") and (not isinstance(query[groupby], (tuple, list))):
                raise KeyError(f"Groupby option {groupby} must be a list.")


            if (groupby == "d3"):
                full_history = self.get_history(**query)
                full_history["base"] = [x.split("-d3")[0] for x in full_history["method"]]
                full_history["d3"] = [
                    method.replace(base, "").replace("-d", "d")
                    for method, base in zip(full_history["method"], full_history["base"])
                ]


                query_names = []
                queries = []
                for name, gb in full_history.groupby("d3"):
                    gb = gb.copy()

                    queries.append(gb)
                    if name == "":
                        query_names.append("No -D3")
                    else:
                        query_names.append(name.upper())

            else:

                query_names = []
                queries = []
                for gb in query[groupby]:
                    gb_query = query.copy()
                    gb_query[groupby] = gb

                    queries.append(self.get_history(**gb_query))
                    query_names.append(self._canonical_name(**{groupby: gb}))

            if (kind == "violin") and (len(queries) != 2):
                raise KeyError(f"Groupby option for violin plots must have two entries.")

        else:
            queries = [self.get_history(**query)]
            query_names = ["Stats"]

        title = f"{self.data.name} Dataset Statistics"

        series = []
        for q, name in zip(queries, query_names):

            if len(q) == 0:
                raise KeyError("No query matches, nothing to visualize!")
            stat = self.statistics(metric, list(q["name"]), bench=bench)
            stat = stat.round(digits)
            stat.sort_index(inplace=True)
            stat.name = name

            col_names = {}
            for record in q.to_dict(orient="records"):
                if (groupby == "d3"):
                    record["method"] = record["base"]

                elif groupby:
                    record[groupby] = None

                index_name = self._canonical_name(
                    record["program"],
                    record["method"],
                    record["basis"],
                    record["keywords"],
                    stoich=record.get("stoich"))

                col_names[record["name"]] = index_name

            if kind == "bar":
                stat.index = [col_names[x] for x in stat.index]
            else:
                stat.columns = [col_names[x] for x in stat.columns]

            series.append(stat)

        if kind == "bar":
            return bar_plot(series, title=title, ylabel=ylabel, return_figure=return_figure)
        else:
            negative = None
            if groupby:
                negative = series[1]

            return violin_plot(series[0], negative=negative, title=title, ylabel=ylabel, return_figure=return_figure)

    def visualize(self,
                  method: Optional[str]=None,
                  basis: Optional[str]=None,
                  keywords: Optional[str]=None,
                  program: Optional[str]=None,
                  groupby: Optional[str]=None,
                  metric: str="UE",
                  bench: Optional[str]=None,
                  kind: str="bar",
                  return_figure: Optional[bool]=None) -> 'plotly.Figure':
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

        Returns
        -------
        plotly.Figure
            The requested figure.
        """

        query = {"method": method, "basis": basis, "keywords": keywords, "program": program}
        query = {k: v for k, v in query.items() if v is not None}

        return self._visualize(metric, bench, query=query, groupby=groupby, return_figure=return_figure, kind=kind)

    def _canonical_name(self,
                        program: Optional[str]=None,
                        method: Optional[str]=None,
                        basis: Optional[str]=None,
                        keywords: Optional[str]=None,
                        stoich: Optional[str]=None) -> str:
        """
        Attempts to build a canonical name for a DataFrame column
        """

        name = ""
        if method:
            name = method.upper()

        if basis and name:
            name = f"{name}/{basis.lower()}"
        elif basis:
            name = f"{basis.lower()}"

        if keywords and (keywords != self.data.default_keywords.get(program, None)):
            name = f"{name}-{keywords}"

        if program and (program.lower() != self.data.default_program):
            name = f"{name}-{program.title()}"

        if stoich:
            if name == "":
                name = stoich.lower()
            elif (stoich.lower() != "default"):
                name = f"{stoich.lower()}-{name}"

        return name

    def _default_parameters(self,
                            program: str,
                            method: str,
                            basis: Optional[str],
                            keywords: Optional[str],
                            stoich: Optional[str]=None) -> Tuple[str, str, str, str]:
        """
        Takes raw input parsed parameters and applies defaults to them.
        """

        # Handle default program
        if program is None:
            if self.data.default_program is None:
                raise KeyError("No default program was set and none was provided.")
            program = self.data.default_program
        else:
            program = program.lower()

        driver = self.data.default_driver

        # Handle keywords
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

        # Form database and history keys
        dbkeys = {"driver": driver, "program": program, "method": method, "basis": basis, "keywords": keywords}
        history = {**dbkeys, **{"keywords": keywords_alias}}
        if stoich is not None:
            history["stoichiometry"] = stoich

        name = self._canonical_name(program, method, basis, keywords_alias, stoich)

        return name, dbkeys, history

    def _query(self, indexer: str, query: Dict[str, Any], field: str="return_result") -> 'Series':
        """
        Runs a query based on an indexer which is index : molecule_id

        Parameters
        ----------
        indexer : str
            The primary index of the query
        query : Dict[str, Any]
            A results query
        field : str, optional
            The field to pull from the ResultRecords
        scale : str, optional
            The scale of the computation

        Returns
        -------
        Series
            A Series of the data results
        """
        self._check_state()

        field = field.lower()

        ret = []
        for query_set in composition_planner(**query):

            # Set the index to remove duplicates
            molecules = list(set(indexer.values()))
            query_set["projection"] = {"molecule": True, field: True}

            # Chunk up the queries
            records = []
            for i in range(0, len(molecules), self.client.query_limit):
                query_set["molecule"] = molecules[i:i + self.client.query_limit]
                records.extend(self.client.query_results(**query_set))

            records = pd.DataFrame(records, columns=["molecule", field])

            df = pd.DataFrame.from_dict(indexer, orient="index", columns=["molecule"])
            df.reset_index(inplace=True)

            # Outer join on left to merge duplicate molecules
            df = df.merge(records, how="left", on="molecule")
            df.set_index("index", inplace=True)
            df.drop("molecule", axis=1, inplace=True)
            ret.append(df)

        if len(ret) == 1:
            retdf = ret[0]
        else:
            retdf = ret[0]
            for df in ret[1:]:
                retdf += df

        retdf[retdf.select_dtypes(include=['number']).columns] *= constants.conversion_factor('hartree', self.units)

        return retdf

    def _compute(self, compute_keys, molecules, tag, priority):
        """
        Internal compute function
        """

        name, dbkeys, history = self._default_parameters(
            compute_keys["program"],
            compute_keys["method"],
            compute_keys["basis"],
            compute_keys["keywords"],
            stoich=compute_keys.get("stoich", None))

        self._check_state()

        if self.client is None:
            raise AttributeError("Dataset: Compute: Client was not set.")

        umols = list(set(molecules))

        ids = []
        submitted = []
        existing = []
        for compute_set in composition_planner(**dbkeys):

            for i in range(0, len(umols), self.client.query_limit):
                chunk_mols = umols[i:i + self.client.query_limit]
                ret = self.client.add_compute(
                    compute_set["program"],
                    compute_set["method"],
                    compute_set["basis"],
                    compute_set["driver"],
                    compute_set["keywords"],
                    chunk_mols,
                    tag=tag,
                    priority=priority)

            ids.extend(ret.ids)
            submitted.extend(ret.submitted)
            existing.extend(ret.existing)

            qhistory = history.copy()
            qhistory["program"] = compute_set["program"]
            qhistory["method"] = compute_set["method"]
            qhistory["basis"] = compute_set["basis"]
            self._add_history(**qhistory)

        return ComputeResponse(ids=ids, submitted=submitted, existing=existing)

    @property
    def units(self):
        return self._units

    @units.setter
    def units(self, value):

        self.df *= constants.conversion_factor(self._units, value)
        self._units = value

    def set_default_program(self, program: str) -> bool:
        """
        Sets the default program.

        Parameters
        ----------
        program : str
            The program to default to.
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
        program : str
            The compute program the alias is for
        keyword : KeywordSet
            The Keywords object to use.
        default : bool, optional
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
        """Pulls the keywords alias from the server for inspection.

        Parameters
        ----------
        alias : str
            The keywords alias.
        program : str
            The program the keywords correspond to.

        Returns
        -------
        KeywordSet
            The requested KeywordSet

        """
        if self.client is None:
            raise AttributeError("Dataset: Client was not set.")

        alias = alias.lower()
        program = program.lower()
        if (program not in self.data.alias_keywords) or (alias not in self.data.alias_keywords[program]):
            raise KeyError("Keywords {}: {} not found.".format(program, alias))

        kwid = self.data.alias_keywords[program][alias]
        return self.client.query_keywords([kwid])[0]

    def add_contributed_values(self, contrib: ContributedValues, overwrite=False) -> None:
        """Adds a ContributedValues to the database.

        Parameters
        ----------
        contrib : ContributedValues
            The ContributedValues to add.
        overwrite : bool, optional
            Overwrites pre-existing values
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

        Returns
        -------
        List[str]
            A list of all known contributed values.
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

    def get_contributed_values_column(self, key: str) -> 'Series':
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

        tmp_idx = pd.DataFrame.from_dict(values, orient="index", columns=[data.name])

        # Convert to numeric
        tmp_idx[tmp_idx.select_dtypes(include=['number']).columns] *= constants.conversion_factor(
            data.units, self.units)

        return tmp_idx

    def add_entry(self, name: str, molecule: Molecule, **kwargs: Dict[str, Any]):
        """Adds a new entry to the Datset

        Parameters
        ----------
        name : str
            The name of the record
        molecule : Molecule
            The Molecule associated with this record
        **kwargs : Dict[str, Any]
            Additional arguements to pass to the record
        """
        mhash = molecule.get_hash()
        self._new_molecules[mhash] = molecule
        self._new_records.append({"name": name, "molecule_hash": mhash, **kwargs})

    def query(self,
              method: str,
              basis: Optional[str]=None,
              *,
              keywords: Optional[str]=None,
              program: Optional[str]=None,
              field: str=None,
              as_array: bool=False,
              force: bool=False) -> str:
        """
        Queries the local Portal for the requested keys.

        Parameters
        ----------
        method : str
            The computational method to query on (B3LYP)
        basis : Optional[str], optional
            The computational basis query on (6-31G)
        keywords : Optional[str], optional
            The option token desired
        program : Optional[str], optional
            The program to query on
        field : str, optional
            The result field to query on
        as_array : bool, optional
            Converts the returned values to NumPy arrays
        force : bool, optional
            Forces a requery if data is already present

        Returns
        -------
        success : str
            The name of the queried column

        Examples
        --------

        >>> ds.query("B3LYP", "aug-cc-pVDZ", stoich="cp", prefix="cp-")

        """

        name, dbkeys, history = self._default_parameters(program, method, basis, keywords)

        if field is None:
            field = "return_result"
        else:
            name = "{}-{}".format(name, field)

        if (name in self.df) and (force is False):
            return name

        if self.client is None:
            raise AttributeError("DataBase: FractalClient was not set.")

        # # If reaction results
        indexer = {e.name: e.molecule_id for e in self.data.records}

        tmp_idx = self._query(indexer, dbkeys, field=field)
        tmp_idx.rename(columns={field: name}, inplace=True)

        if as_array:
            tmp_idx[tmp_idx.columns[0]] = tmp_idx[tmp_idx.columns[0]].apply(lambda x: np.array(x))

        # Apply to df
        self.df[tmp_idx.columns] = tmp_idx

        return tmp_idx.columns[0]

    def compute(self,
                method: str,
                basis: Optional[str]=None,
                *,
                keywords: Optional[str]=None,
                program: Optional[str]=None,
                tag: Optional[str]=None,
                priority: Optional[str]=None) -> ComputeResponse:
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

        compute_keys = {"program": program, "method": method, "basis": basis, "keywords": keywords}

        molecule_idx = [e.molecule_id for e in self.data.records]

        ret = self._compute(compute_keys, molecule_idx, tag, priority)
        self.save()

        return ret

    def get_index(self) -> List[str]:
        """
        Returns the current index of the database.

        Returns
        -------
        ret : List[str]
            The names of all reactions in the database
        """
        return [x.name for x in self.data.records]

    # Statistical quantities
    def statistics(self, stype: str, value: str, bench: Optional[str]=None, **kwargs: Dict[str, Any]):
        """Provides statistics for various columns in the underlying dataframe.

        Parameters
        ----------
        stype : str
            The type of statistic in question
        value : str
            The method string to compare
        bench : str, optional
            The benchmark method for the comparison, defaults to `default_benchmark`.
        kwargs: Dict[str, Any]
            Additional kwargs to pass to the statistics functions


        Returns
        -------
        ret : pd.DataFrame, pd.Series, float
            Returns a DataFrame, Series, or float with the requested statistics depending on input.
        """

        if (bench is None):
            bench = self.data.default_benchmark

        if (bench is None):
            raise KeyError("No benchmark provided and default_benchmark is None!")

        return wrap_statistics(stype.upper(), self.df, value, bench, **kwargs)

    # Getters
    def __getitem__(self, args: str) -> 'Series':
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
