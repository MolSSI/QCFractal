"""
QCPortal Database ODM
"""
import hashlib
import json
import tempfile
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import requests

import pydantic
from qcelemental import constants

from ..models import ComputeResponse, ObjectId, ProtoModel
from ..statistics import wrap_statistics
from ..visualization import bar_plot, violin_plot
from .collection import Collection
from .collection_utils import composition_planner, register_collection

if TYPE_CHECKING:  # pragma: no cover
    from .. import FractalClient  # lgtm[py/unused-import] (https://github.com/Semmle/ql/issues/2014)
    from ..models import KeywordSet, Molecule, ResultRecord
    from . import DatasetView


class MoleculeEntry(ProtoModel):
    name: str
    molecule_id: ObjectId
    comment: Optional[str] = None
    local_results: Dict[str, Any] = {}


class ContributedValues(ProtoModel):
    name: str
    doi: Optional[str] = None
    theory_level: Union[str, Dict[str, str]]
    theory_level_details: Optional[Union[str, Dict[str, Optional[str]]]] = None
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
    def __init__(self, name: str, client: Optional['FractalClient'] = None, **kwargs: Any) -> None:
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
        self._new_molecules: Dict[str, Molecule] = {}
        self._new_keywords: Dict[Tuple[str, str], KeywordSet] = {}
        self._new_records: List[Dict[str, Any]] = []
        self._updated_state = False

        self._view: Optional[DatasetView] = None
        if self.data.view_available:
            from . import RemoteView
            self._view = RemoteView(client, self.data.id)
        self._disable_view: bool = False  # for debugging and testing
        self._disable_query_limit: bool = False  # for debugging and testing

        # Initialize internal data frames and load in contrib
        self.df = pd.DataFrame(index=self.get_index())
        self._column_metadata: Dict[str, Any] = {}
        self._form_index()

    class DataModel(Collection.DataModel):

        # Defaults
        default_program: Optional[str] = None
        default_keywords: Dict[str, str] = {}
        default_driver: str = "energy"
        default_units: str = "kcal / mol"
        default_benchmark: Optional[str] = None

        alias_keywords: Dict[str, Dict[str, str]] = {}

        # Data
        records: List[MoleculeEntry] = []
        contributed_values: Dict[str, ContributedValues] = {}

        # History: driver, program, method (basis, keywords)
        history: Set[Tuple[str, str, str, Optional[str], Optional[str]]] = set()
        history_keys: Tuple[str, str, str, str, str] = ("driver", "program", "method", "basis", "keywords")

    def set_view(self, path: Union[str, Path]) -> None:
        """
        Set a dataset to use a local view.

        Parameters
        ----------
        path: Union[str, Path]
            path to an hdf5 file representing a view for this dataset
        """
        from . import HDF5View
        self._view = HDF5View(path)

    def download(self, local_path: Optional[Union[str, Path]] = None, verify: bool = True) -> None:
        """
        Download a remote view if available. The dataset will use this view to avoid server queries for calls to:
        - get_entries
        - get_molecules
        - get_values
        - list_values

        Parameters
        ----------
        local_path: Optional[Union[str, Path]], optional
            Local path the store downloaded view. If None, the view will be stored in a temporary file and deleted on exit.
        verify: bool, optional
            Verify download checksum. Default: True.
        """
        if self.data.view_url is None:
            raise ValueError("A view for this dataset is not available on the server")

        if local_path is not None:
            local_path = Path(local_path)
        else:
            self._view_tempfile = tempfile.NamedTemporaryFile()  # keep temp file alive until self is destroyed
            local_path = self._view_tempfile.name

        r = requests.get(self.data.view_url, stream=True)
        with open(local_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=8192):
                fd.write(chunk)

        if verify:
            remote_checksum = self.data.view_metadata['blake2b_checksum']
            from . import HDF5View
            local_checksum = HDF5View(local_path).hash()
            if remote_checksum != local_checksum:
                raise ValueError(f"Checksum verification failed. Expected: {remote_checksum}, Got: {local_checksum}")

        self.set_view(local_path)

    def to_file(self, path: Union[str, Path], encoding: str) -> None:
        """
        Writes a view of the dataset to a file

        Parameters
        ----------
        path: Union[str, Path]
            Where to write the file
        encoding: str
            Options: plaintext, hdf5
        """
        if encoding.lower() == "plaintext":
            from . import PlainTextView
            PlainTextView(path).write(self)
        elif encoding.lower() in ["hdf5", "h5"]:
            from . import HDF5View
            HDF5View(path).write(self)
        else:
            raise NotImplementedError(f"Unsupported encoding: {encoding}")

    def _form_index(self) -> None:
        self._entry_index = pd.DataFrame([[entry.name, entry.molecule_id] for entry in self.data.records],
                                         columns=["name", "molecule_id"])

    def _check_state(self) -> None:
        if self._new_molecules or self._new_keywords or self._new_records or self._updated_state:
            raise ValueError("New molecules, keywords, or records detected, run save before submitting new tasks.")

    def _canonical_pre_save(self, client: 'FractalClient') -> None:

        for k in list(self._new_keywords.keys()):
            ret = client.add_keywords([self._new_keywords[k]])
            assert len(ret) == 1, "KeywordSet added incorrectly"
            self.data.alias_keywords[k[0]][k[1]] = ret[0]
            del self._new_keywords[k]
        self._updated_state = False

    def _pre_save_prep(self, client: 'FractalClient') -> None:
        self._canonical_pre_save(client)

        # Preps any new molecules introduced to the Dataset before storing data.
        mol_ret = self._add_molecules_by_dict(client, self._new_molecules)

        # Update internal molecule UUID's to servers UUID's
        for record in self._new_records:
            molecule_hash = record.pop("molecule_hash")
            new_record = MoleculeEntry(molecule_id=mol_ret[molecule_hash], **record)
            self.data.records.append(new_record)

        self._new_records = []
        self._new_molecules = {}
        self._form_index()

    def get_entries(self, force: bool = False) -> pd.DataFrame:
        """
        Provides a list of entries for the dataset

        Parameters
        ----------
        force: bool, optional
            skip cache

        Returns
        -------
        pd.DataFrame
            A dataframe containing entry names and specifciations.
            For Dataset, specifications are molecule ids.
            For ReactionDataset, specifications describe reaction stoichiometry.
        """
        if self._use_view(force):
            ret = self._view.get_entries()
        else:
            ret = self._entry_index
        return ret.copy()

    def _molecule_indexer(self, subset: Optional[Union[str, Set[str]]] = None,
                          force: bool = False) -> Dict[str, ObjectId]:
        """Provides a {index: molecule_id} mapping for a given subset.

        Parameters
        ----------
        subset : Optional[Union[str, Set[str]]], optional
            The indices of the desired subset. Return all indices if subset is None.

        Returns
        -------
        Dict[str, 'ObjectId']
            Molecule index to molecule ObjectId map
        """
        index = self.get_entries(force)
        if subset:
            if isinstance(subset, str):
                subset = {subset}

            index = index[index.name.isin(subset)]

        return {row['name']: row['molecule_id'] for row in index.to_dict('records')}

    def _add_history(self, **history: Optional[str]) -> None:
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

    def list_values(self,
                    method: Optional[Union[str, List[str]]] = None,
                    basis: Optional[Union[str, List[str]]] = None,
                    keywords: Optional[str] = None,
                    program: Optional[str] = None,
                    driver: Optional[str] = None,
                    name: Optional[Union[str, List[str]]] = None,
                    native: Optional[bool] = None,
                    force: bool = False) -> pd.DataFrame:
        """
        Lists available data that may be queried with get_values.
        Results may be narrowed by providing search keys.
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
        name : Optional[Union[str, List[str]]], optional
            The canonical name of the data column
        native: Optional[bool], optional
            True: only include data computed with QCFractal
            False: only include data contributed from outside sources
            None: include both
        force : bool, optional
            Data is typically cached, forces a new query if True

        Returns
        -------
        DataFrame
            A DataFrame of the matching data specifications
        """
        spec: Dict[str, Optional[Union[str, bool, List[str]]]] = {
            "method": method,
            "basis": basis,
            "keywords": keywords,
            "program": program,
            "name": name,
            "driver": driver
        }

        if self._use_view(force):
            ret = self._view.list_values()
            spec["native"] = native
        else:
            ret = []
            if native in {True, None}:
                df = self._list_records(dftd3=False)
                df['native'] = True
                ret.append(df)

            if native in {False, None}:
                df = self._list_contributed_values()
                df['native'] = False
                ret.append(df)

            ret = pd.concat(ret)

        # Filter
        ret.fillna("None", inplace=True)
        ret = self._filter_records(ret, **spec)

        # Sort
        sort_index = ["native"] + list(self.data.history_keys[:-1])
        if "stoichiometry" in ret.columns:
            sort_index += ["stoichiometry"]
        ret.set_index(sort_index, inplace=True)
        ret.sort_index(inplace=True)
        ret.reset_index(inplace=True)
        ret.set_index(["native"] + list(self.data.history_keys[:-1]), inplace=True)

        return ret

    @staticmethod
    def _filter_records(df: pd.DataFrame,
                        **spec: Optional[Union[str, bool, List[Union[str, bool]], Tuple]]) -> pd.DataFrame:
        """
        Helper for filtering records on a spec. Note that `None` is a wildcard while `"None"` matches `None` and NaN.
        """
        ret = df.copy()
        for key, value in spec.items():
            if value is None:
                continue
            if isinstance(value, bool):
                ret = ret[ret[key] == value]
            elif isinstance(value, str):
                value = value.lower()
                ret = ret[ret[key].fillna("None").str.lower() == value]
            elif isinstance(value, (list, tuple)):
                query = [x.lower() for x in value]
                ret = ret[ret[key].fillna("None").str.lower().isin(query)]
            else:
                raise TypeError(f"Search type {type(value)} not understood.")
        return ret

    def list_records(self, dftd3: bool = False, pretty: bool = True,
                     **search: Optional[Union[str, List[str]]]) -> pd.DataFrame:
        """
        Lists specifications of available records, i.e. method, program, basis set, keyword set, driver combinations
        `None` is a wildcard selector. To search for `None`, use `"None"`.

        Parameters
        ----------
        pretty: bool
            Replace NaN with "None" in returned DataFrame
        **search : Dict[str, Optional[str]]
            Allows searching to narrow down return.

        Returns
        -------
        DataFrame
            Record specifications matching **search.

        """
        ret = self._list_records(dftd3=dftd3)
        ret = self._filter_records(ret, **search)
        if pretty:
            ret.fillna("None", inplace=True)
        return ret

    def _list_records(self, dftd3: bool = False) -> pd.DataFrame:
        """
        Lists specifications of available records, i.e. method, program, basis set, keyword set, driver combinations
        `None` is a wildcard selector. To search for `None`, use `"None"`.

        Parameters
        ----------
        dftd3: bool, optional
            Include dftd3 program record specifications in addition to composite DFT-D3 record specifications

        Returns
        -------
        DataFrame
            Record specifications matching **search.

        """
        show_dftd3 = dftd3

        history = pd.DataFrame(list(self.data.history), columns=self.data.history_keys)

        # Short circuit because merge and apply below require data
        if history.shape[0] == 0:
            ret = history.copy()
            ret['name'] = None
            return ret

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
        # Add name column
        ret['name'] = ret.apply(lambda row: self._canonical_name(program=row["program"],
                                                                 method=row["method"],
                                                                 basis=row["basis"],
                                                                 keywords=row["keywords"],
                                                                 stoich=row.get("stoichiometry", None),
                                                                 driver=row["driver"]),
                                axis=1)
        if show_dftd3 is False:
            ret = ret[ret["program"] != "dftd3"]

        return ret

    def get_values(self,
                   method: Optional[Union[str, List[str]]] = None,
                   basis: Optional[Union[str, List[str]]] = None,
                   keywords: Optional[str] = None,
                   program: Optional[str] = None,
                   driver: Optional[str] = None,
                   name: Optional[Union[str, List[str]]] = None,
                   native: Optional[bool] = None,
                   force: bool = False) -> pd.DataFrame:
        """
        Obtains values matching the search parameters provided for the expected `return_result` values.
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
        name : Optional[Union[str, List[str]]], optional
            Canonical name of the record. Overrides the above selectors.
        native: Optional[bool], optional
            True: only include data computed with QCFractal
            False: only include data contributed from outside sources
            None: include both
        force : bool, optional
            Data is typically cached, forces a new query if True

        Returns
        -------
        DataFrame
            A DataFrame of values with columns corresponding to methods and rows corresponding to molecule entries.
        """
        return self._get_values(method=method,
                                basis=basis,
                                keywords=keywords,
                                program=program,
                                driver=driver,
                                name=name,
                                native=native,
                                force=force)

    def _get_values(self, native: Optional[bool] = None, force: bool = False, **spec) -> pd.DataFrame:
        ret = []

        if native in {True, None}:
            spec_nodriver = spec.copy()
            driver = spec_nodriver.pop("driver")
            if driver is not None and driver != self.data.default_driver:
                raise KeyError(
                    f"For native values, driver ({driver}) must be the same as the dataset's default driver "
                    f"({self.data.default_driver}). Consider using get_records instead.")
            df = self._get_native_values(force=force, **spec_nodriver)
            ret.append(df)

        if native in {False, None}:
            df = self._get_contributed_values(**spec)
            ret.append(df)
        ret_df = pd.concat(ret, axis=1)
        ret_df.sort_index(inplace=True)

        return ret_df

    def _get_native_values(self,
                           method: Optional[Union[str, List[str]]] = None,
                           basis: Optional[Union[str, List[str]]] = None,
                           keywords: Optional[str] = None,
                           program: Optional[str] = None,
                           name: Optional[Union[str, List[str]]] = None,
                           force: bool = False) -> pd.DataFrame:
        """
        Obtains records matching the provided search criteria.
        Defaults to the standard programs and keywords if not provided.

        Parameters
        ----------
        method : Optional[Union[str, List[str]]], optional
            The computational method to compute (B3LYP)
        basis : Optional[Union[str, List[str]]], optional
            The computational basis to compute (6-31G)
        keywords : Optional[str], optional
            The keyword alias for the requested compute
        program : Optional[str], optional
            The underlying QC program
        name : Optional[Union[str, List[str]]], optional
            Canonical name of the record. Overrides the above selectors.
        force : bool, optional
            Data is typically cached, forces a new query if True.

        Returns
        -------
        DataFrame
            A DataFrame of the queried parameters
        """
        au_units = {'energy': 'hartree', 'gradient': 'hartree/bohr', 'hessian': 'hartree/bohr**2'}

        # So that datasets with no records do not require a default program and default keywords
        if len(self.list_records()) == 0:
            return pd.DataFrame(columns=['index']).set_index('index')

        queries = self._form_queries(method=method, basis=basis, keywords=keywords, program=program, name=name)
        names = []
        new_queries = []
        for _, query in queries.iterrows():

            query = query.replace({np.nan: None}).to_dict()
            if "stoichiometry" in query:
                query["stoich"] = query.pop("stoichiometry")

            qname = query["name"]
            names.append(qname)
            if force or (qname not in self.df.columns):
                self._column_metadata[qname] = query
                new_queries.append(query)

        if not self._use_view(force):
            units: Dict[str, str] = {}
            for query in new_queries:
                driver = query.pop("driver")
                qname = query.pop("name")
                data = self.get_records(query.pop("method").upper(),
                                        projection={"return_result": True},
                                        merge=True,
                                        **query)
                self.df[qname] = data["return_result"]
                units[qname] = au_units[driver]
                query["name"] = qname
        else:
            for query in new_queries:
                query["native"] = True
            data, units = self._view.get_values(new_queries)
            self.df = pd.concat([self.df, data], axis=1)

        for query in new_queries:
            qname = query["name"]
            self.df[qname] *= constants.conversion_factor(units[qname], self.units)
            self._column_metadata[qname].update({"native": True, "units": self.units})

        return self.df[names]

    def _form_queries(self,
                      method: Optional[Union[str, List[str]]] = None,
                      basis: Optional[Union[str, List[str]]] = None,
                      keywords: Optional[str] = None,
                      program: Optional[str] = None,
                      stoich: Optional[str] = None,
                      name: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        if name is None:
            _, _, history = self._default_parameters(program, "nan", "nan", keywords, stoich=stoich)
            for k, v in [("method", method), ("basis", basis)]:

                if v is not None:
                    history[k] = v
                else:
                    history.pop(k, None)
            queries = self.list_records(**history, dftd3=True, pretty=False)
        else:
            if any((field is not None for field in {program, method, basis, keywords})):
                warnings.warn("Name and additional field were provided. Only name will be used as a selector.",
                              RuntimeWarning)
            queries = self.list_records(name=name, dftd3=True, pretty=False)

        if queries.shape[0] > 10 and self._disable_query_limit is False:
            raise TypeError("More than 10 queries formed, please narrow the search.")
        return queries

    def _visualize(self,
                   metric,
                   bench,
                   query: Dict[str, Union[Optional[str], List[str]]],
                   groupby: Optional[str] = None,
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
        _valid_groupby = {"method", "basis", "keywords", "program", "stoich", "d3"}
        if groupby is not None:
            groupby = groupby.lower()
            if groupby not in _valid_groupby:
                raise KeyError(f"Groupby option {groupby} not understood.")
            if (groupby != "d3") and (groupby not in query):
                raise KeyError(
                    f"Groupby option {groupby} not found in query, must provide a search on this parameter.")

            if (groupby != "d3") and (not isinstance(query[groupby], (tuple, list))):
                raise KeyError(f"Groupby option {groupby} must be a list.")

            query_names = []
            queries = []
            if groupby == "d3":
                base = [method.upper().split("-D3")[0] for method in query["method"]]
                d3types = [
                    method.upper().replace(b, "").replace("-D", "D") for method, b in zip(query["method"], base)
                ]

                # Preserve order of first unique appearance
                seen: Set[str] = set()
                unique_d3types = [x for x in d3types if not (x in seen or seen.add(x))]

                for d3type in unique_d3types:
                    gb_query = query.copy()
                    gb_query["method"] = []
                    for i in range(len(base)):
                        method = query["method"][i]
                        if method.upper().replace(base[i], "").replace("-D", "D") == d3type:
                            gb_query["method"].append(method)
                    queries.append(gb_query)
                    if d3type == "":
                        query_names.append("No -D3")
                    else:
                        query_names.append(d3type.upper())
            else:
                for gb in query[groupby]:
                    gb_query = query.copy()
                    gb_query[groupby] = gb

                    queries.append(gb_query)
                    query_names.append(self._canonical_name(**{groupby: gb}))

            if (kind == "violin") and (len(queries) != 2):
                raise KeyError(f"Groupby option for violin plots must have two entries.")

        else:
            queries = [query]
            query_names = ["Stats"]

        title = f"{self.data.name} Dataset Statistics"

        series = []
        for q, name in zip(queries, query_names):

            if len(q) == 0:
                raise KeyError("No query matches, nothing to visualize!")

            # Pull the values
            if "stoichiometry" in q:
                q["stoich"] = q.pop("stoichiometry")
            values = self.get_values(**q)

            # Create the statistics
            stat = self.statistics(metric, values, bench=bench)
            stat = stat.round(digits)
            stat.sort_index(inplace=True)
            stat.name = name

            # Munge the column names based on the groupby parameter
            col_names = {}
            for k, v in stat.iteritems():
                record = self._column_metadata[k]
                if (groupby == "d3"):
                    record["method"] = record["method"].upper().split("-D3")[0]

                elif groupby:
                    record[groupby] = None

                index_name = self._canonical_name(record["program"],
                                                  record["method"],
                                                  record["basis"],
                                                  record["keywords"],
                                                  stoich=record.get("stoich"))

                col_names[k] = index_name

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
                  method: Optional[str] = None,
                  basis: Optional[str] = None,
                  keywords: Optional[str] = None,
                  program: Optional[str] = None,
                  groupby: Optional[str] = None,
                  metric: str = "UE",
                  bench: Optional[str] = None,
                  kind: str = "bar",
                  return_figure: Optional[bool] = None) -> 'plotly.Figure':
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
            If True, return the raw plotly figure. If False, returns a hosted iPlot.
            If None, return a iPlot display in Jupyter notebook and a raw plotly figure in all other circumstances.

        Returns
        -------
        plotly.Figure
            The requested figure.
        """

        query = {"method": method, "basis": basis, "keywords": keywords, "program": program}
        query = {k: v for k, v in query.items() if v is not None}

        return self._visualize(metric, bench, query=query, groupby=groupby, return_figure=return_figure, kind=kind)

    def _canonical_name(self,
                        program: Optional[str] = None,
                        method: Optional[str] = None,
                        basis: Optional[str] = None,
                        keywords: Optional[str] = None,
                        stoich: Optional[str] = None,
                        driver: Optional[str] = None) -> str:
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
                            program: Optional[str],
                            method: str,
                            basis: Optional[str],
                            keywords: Optional[str],
                            stoich: Optional[str] = None
                            ) -> Tuple[str, Dict[str, Union[str, 'KeywordSet']], Dict[str, str]]:
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

    def _get_molecules(self, indexer: Dict[Any, ObjectId], force: bool = False) -> pd.DataFrame:
        """Queries a list of molecules using a molecule indexer

        Parameters
        ----------
        indexer : Dict[str, 'ObjectId']
            A key/value index of molecules to query
        force : bool, optional
            Force pull of molecules from server

        Returns
        -------
        pd.DataFrame
            A table of Molecules, indexed by Entry names

        Raises
        ------
        KeyError
            If no records match the query
        """

        molecule_ids = list(set(indexer.values()))
        if not self._use_view(force):
            molecules: List['Molecule'] = []
            for i in range(0, len(molecule_ids), self.client.query_limit):
                molecules.extend(self.client.query_molecules(id=molecule_ids[i:i + self.client.query_limit]))
            # XXX: molecules = pd.DataFrame({"molecule_id": molecule_ids, "molecule": molecules}) fails
            #      test_gradient_dataset_get_molecules and I don't know why
            molecules = pd.DataFrame({"molecule_id": molecule.id, "molecule": molecule} for molecule in molecules)
        else:
            molecules = self._view.get_molecules(molecule_ids)
            molecules = pd.DataFrame({"molecule_id": molecule_ids, "molecule": molecules})

        if len(molecules) == 0:
            raise KeyError("Query matched 0 records.")

        df = pd.DataFrame.from_dict(indexer, orient="index", columns=["molecule_id"])

        df.reset_index(inplace=True)

        # Outer join on left to merge duplicate molecules
        df = df.merge(molecules, how="left", on="molecule_id")
        df.set_index("index", inplace=True)
        df.drop("molecule_id", axis=1, inplace=True)

        return df

    def _get_records(self,
                     indexer: Dict[Any, ObjectId],
                     query: Dict[str, Any],
                     projection: Optional[Dict[str, bool]] = None,
                     merge: bool = False,
                     raise_on_plan: Union[str, bool] = False) -> 'pd.Series':
        """
        Runs a query based on an indexer which is index : molecule_id

        Parameters
        ----------
        indexer : Dict[str, ObjectId]
            A key/value index of molecules to query
        query : Dict[str, Any]
            A results query
        projection : Optional[Dict[str, bool]], optional
            Description
        merge : bool, optional
            Sum compound queries together, useful for mixing results
        raise_on_plan : Union[str, bool], optional
            Raises a KeyError is True or string if a multi-stage plan is detected.

        Returns
        -------
        pd.Series
            A Series of the data results

        """
        self._check_client()
        self._check_state()

        ret = []
        plan = composition_planner(**query)
        if raise_on_plan and (len(plan) > 1):
            if raise_on_plan is True:
                raise KeyError("Recieved a multi-stage plan when this function does not support multi-staged plans.")
            else:
                raise KeyError(raise_on_plan)

        for query_set in plan:

            query_set["keywords"] = self.get_keywords(query_set["keywords"], query_set["program"], return_id=True)
            # Set the index to remove duplicates
            molecules = list(set(indexer.values()))
            if projection:
                proj = {k.lower(): v for k, v in projection.items()}
                proj["molecule"] = True
                query_set["projection"] = proj

            # Chunk up the queries
            records: List[ResultRecord] = []
            for i in range(0, len(molecules), self.client.query_limit):
                query_set["molecule"] = molecules[i:i + self.client.query_limit]
                records.extend(self.client.query_results(**query_set))

            if projection is None:
                records = [{"molecule": x.molecule, "record": x} for x in records]

            records = pd.DataFrame.from_dict(records)

            df = pd.DataFrame.from_dict(indexer, orient="index", columns=["molecule"])
            df.reset_index(inplace=True)

            if records.shape[0] > 0:
                # Outer join on left to merge duplicate molecules
                df = df.merge(records, how="left", on="molecule")
            else:
                # No results, fill NaN values
                if projection is None:
                    df["record"] = None
                else:
                    for k, v in projection.items():
                        if v:
                            df[k] = np.nan

            df.set_index("index", inplace=True)
            df.drop("molecule", axis=1, inplace=True)

            ret.append(df)

        if len(molecules) == 0:
            raise KeyError("Query matched 0 records.")

        if merge:
            retdf = ret[0]
            for df in ret[1:]:
                retdf += df
            return retdf
        else:
            return ret

    def _compute(self,
                 compute_keys: Dict[str, Union[str, None]],
                 molecules: Union[List[str], pd.Series],
                 tag: Optional[str] = None,
                 priority: Optional[str] = None) -> ComputeResponse:
        """
        Internal compute function
        """

        name, dbkeys, history = self._default_parameters(compute_keys["program"],
                                                         compute_keys["method"],
                                                         compute_keys["basis"],
                                                         compute_keys["keywords"],
                                                         stoich=compute_keys.get("stoich", None))

        self._check_client()
        self._check_state()

        umols = list(set(molecules))

        ids: List[Optional[ObjectId]] = []
        submitted: List[ObjectId] = []
        existing: List[ObjectId] = []
        for compute_set in composition_planner(**dbkeys):

            for i in range(0, len(umols), self.client.query_limit):
                chunk_mols = umols[i:i + self.client.query_limit]
                ret = self.client.add_compute(**compute_set, molecule=chunk_mols, tag=tag, priority=priority)

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

        self.data.__dict__["default_program"] = program.lower()
        return True

    def set_default_benchmark(self, benchmark: str) -> bool:
        """
        Sets the default benchmark value.

        Parameters
        ----------
        benchmark : str
            The benchmark to default to.
        """

        self.data.__dict__["default_benchmark"] = benchmark
        return True

    def add_keywords(self, alias: str, program: str, keyword: 'KeywordSet', default: bool = False) -> bool:
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

    def get_keywords(self, alias: str, program: str, return_id: bool = False) -> Union['KeywordSet', str]:
        """Pulls the keywords alias from the server for inspection.

        Parameters
        ----------
        alias : str
            The keywords alias.
        program : str
            The program the keywords correspond to.
        return_id : bool, optional
            If True, returns the ``id`` rather than the ``KeywordSet`` object.
            Description

        Returns
        -------
        Union['KeywordSet', str]
            The requested ``KeywordSet`` or ``KeywordSet`` ``id``.

        """
        self._check_client()
        if alias is None:
            if return_id:
                return None
            else:
                return {}

        alias = alias.lower()
        program = program.lower()
        if (program not in self.data.alias_keywords) or (alias not in self.data.alias_keywords[program]):
            raise KeyError("Keywords {}: {} not found.".format(program, alias))

        kwid = self.data.alias_keywords[program][alias]
        if return_id:
            return kwid
        else:
            return self.client.query_keywords([kwid])[0]

    def add_contributed_values(self, contrib: ContributedValues, overwrite: bool = False) -> None:
        """
        Adds a ContributedValues to the database. Be sure to call save() to commit changes to the server.

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
        if set(contrib.values.keys()) != set(self.get_index()):
            raise ValueError("Contributed values indices do not match the entries in the dataset.")
        # Check the key
        key = contrib.name.lower()
        if (key in self.data.contributed_values) and (overwrite is False):
            raise KeyError(
                "Key '{}' already found in contributed values. Use `overwrite=True` to force an update.".format(key))

        self.data.contributed_values[key] = contrib
        self._updated_state = True

    def _list_contributed_values(self) -> pd.DataFrame:
        """
        Lists all specifications of contributed data, i.e. method, program, basis set, keyword set, driver combinations

        Returns
        -------
        DataFrame
            Contributed value specifications.
        """
        ret = pd.DataFrame(columns=self.data.history_keys + tuple(["name"]))

        cvs = ((cv_data.name, cv_data.theory_level_details)
               for (cv_name, cv_data) in self.data.contributed_values.items())

        for cv_name, theory_level_details in cvs:
            spec = {"name": cv_name}
            for k in self.data.history_keys:
                spec[k] = "Unknown"
            # ReactionDataset uses "default" as a default value for stoich,
            # but many contributed datasets lack a stoich field
            if "stoichiometry" in self.data.history_keys:
                spec["stoichiometry"] = "default"
            if isinstance(theory_level_details, dict):
                spec.update(**theory_level_details)
            ret = ret.append(spec, ignore_index=True)

        return ret

    def _get_contributed_values(self, force: bool = False, **spec) -> pd.DataFrame:
        queries = self._filter_records(self._list_contributed_values().rename(columns={'stoichiometry': 'stoich'}),
                                       **spec)
        column_names: List[str] = []
        new_queries = []
        for query in queries.to_dict("records"):
            column_name = self.data.contributed_values[query["name"].lower()].name
            column_names.append(column_name)
            if force or (column_name not in self.df.columns):
                self._column_metadata[column_name] = query
                new_queries.append(query)

        if not self._use_view(force):
            units: Dict[str, str] = {}

            for query in new_queries:
                data = self.data.contributed_values[query["name"].lower()].copy()
                column_name = data.name
                # Annoying work around to prevent some pandas magic
                if isinstance(next(iter(data.values.values())), (int, float)):
                    values = data.values
                else:
                    # TODO temporary patch until msgpack collections
                    if isinstance(data.theory_level_details, dict) and "driver" in data.theory_level_details:
                        cv_driver = data.theory_level_details["driver"]
                    else:
                        cv_driver = self.data.default_driver
                    if cv_driver == "gradient":
                        values = {k: np.array(v).reshape(-1, 3) for k, v in data.values.items()}
                    else:
                        values = {k: np.array(v) for k, v in data.values.items()}
                self.df[column_name] = pd.Series(list(values.values()), index=list(values.keys()))
                units[column_name] = data.units
        else:
            for query in new_queries:
                query["native"] = False
            ret, units = self._view.get_values(new_queries)
            self.df = pd.concat([self.df, ret], axis=1)

        # convert units
        for query in new_queries:
            column_name = self.data.contributed_values[query["name"].lower()].name
            metadata = {"native": False}
            try:
                self.df[column_name] *= constants.conversion_factor(units[column_name], self.units)
                metadata["units"] = self.units
            except ValueError as e:
                # This is meant to catch pint.errors.DimensionalityError without importing pint, which is too slow
                if e.__class__.__name__ == "DimensionalityError":
                    metadata["units"] = units[column_name]
                else:
                    raise
            self._column_metadata[column_name].update(metadata)

        return self.df[column_names]

    def get_molecules(self, subset: Optional[Union[str, Set[str]]] = None,
                      force: bool = False) -> Union[pd.DataFrame, 'Molecule']:
        """Queries full Molecules from the database.

        Parameters
        ----------
        subset : Optional[Union[str, Set[str]]], optional
            The index subset to query on
        force : bool, optional
            Force pull of molecules from server

        Returns
        -------
        Union[pd.DataFrame, 'Molecule']
            Either a DataFrame of indexed Molecules or a single Molecule if a single subset string was provided.
        """
        indexer = self._molecule_indexer(subset=subset, force=force)
        df = self._get_molecules(indexer, force)

        if isinstance(subset, str):
            return df.iloc[0, 0]
        else:
            return df

    def get_records(self,
                    method: str,
                    basis: Optional[str] = None,
                    *,
                    keywords: Optional[str] = None,
                    program: Optional[str] = None,
                    projection: Optional[Dict[str, bool]] = None,
                    subset: Optional[Union[str, Set[str]]] = None,
                    merge: bool = False) -> Union[pd.DataFrame, 'ResultRecord']:
        """
        Queries full ResultRecord objects from the database.

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
        projection : Optional[Dict[str, bool]], optional
            The attribute project to perform on the query, otherwise returns ResultRecord objects.
        subset : Optional[Union[str, Set[str]]], optional
            The index subset to query on
        merge : bool
            Merge multiple results into one (as in the case of DFT-D3).
            This only works when projection={'return_results'=True}, as in get_values.

        Returns
        -------
        Union[pd.DataFrame, 'ResultRecord']
            Either a DataFrame of indexed ResultRecords or a single ResultRecord if a single subset string was provided.
        """
        name, _, history = self._default_parameters(program, method, basis, keywords)
        if name not in set(self.list_records().reset_index()["name"].unique()):
            raise KeyError(f"Requested query ({name}) did not match a known record.")

        indexer = self._molecule_indexer(subset=subset, force=True)
        df = self._get_records(indexer, history, projection=projection, merge=merge)

        if not merge and len(df) == 1:
            df = df[0]

        if len(df) == 0:
            raise KeyError("Query matched no records!")

        if isinstance(subset, str):
            return df.iloc[0, 0]
        else:
            return df

    def add_entry(self, name: str, molecule: 'Molecule', **kwargs: Dict[str, Any]) -> None:
        """Adds a new entry to the Dataset

        Parameters
        ----------
        name : str
            The name of the record
        molecule : Molecule
            The Molecule associated with this record
        **kwargs : Dict[str, Any]
            Additional arguments to pass to the record
        """
        mhash = molecule.get_hash()
        self._new_molecules[mhash] = molecule
        self._new_records.append({"name": name, "molecule_hash": mhash, **kwargs})

    def compute(self,
                method: str,
                basis: Optional[str] = None,
                *,
                keywords: Optional[str] = None,
                program: Optional[str] = None,
                tag: Optional[str] = None,
                priority: Optional[str] = None) -> ComputeResponse:
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
    def statistics(self, stype: str, value: str, bench: Optional[str] = None,
                   **kwargs: Dict[str, Any]) -> Union[np.ndarray, pd.Series, np.float64]:
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
        np.ndarray, pd.Series, float
            Returns an ndarray, Series, or float with the requested statistics depending on input.
        """

        if bench is None:
            bench = self.data.default_benchmark

        if bench is None:
            raise KeyError("No benchmark provided and default_benchmark is None!")

        return wrap_statistics(stype.upper(), self, value, bench, **kwargs)

    def _use_view(self, force: bool = False) -> bool:
        """Helper function to decide whether to use a locally available HDF5 view"""
        return (force is False) and (self._view is not None) and (self._disable_view is False)

    def _clear_cache(self) -> None:
        self.df = pd.DataFrame(index=self.get_index())

    # Getters
    def __getitem__(self, args: str) -> pd.Series:
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
