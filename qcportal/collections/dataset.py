"""
QCPortal Database ODM
"""
import gzip
import tempfile
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import requests
from pydantic import Field, validator
from qcelemental import constants
from qcelemental.models.types import Array
from tqdm import tqdm

from .collection import Collection
from .collection_utils import register_collection
from ...interface.models import Citation, ComputeResponse, ObjectId, ProtoModel
from ...interface.statistics import wrap_statistics
from ...interface.visualization import bar_plot, violin_plot

if TYPE_CHECKING:  # pragma: no cover
    from .. import PortalClient
    from ...interface.models import Molecule, SinglepointRecord
    from ..keywords import KeywordSet


class MoleculeEntry(ProtoModel):
    name: str = Field(..., description="The name of entry.")
    molecule_id: ObjectId = Field(..., description="The id of the Molecule the entry references.")
    comment: Optional[str] = Field(None, description="A comment for the entry")
    local_results: Dict[str, Any] = Field({}, description="Additional local values.")


class MoleculeEntrySpecification(ProtoModel):
    name: str
    driver: str
    program: str
    method: str
    basis: str
    keywords: Dict[str, Any]


# TODO: do we still want a concept of contributed values?
class ContributedValues(ProtoModel):
    name: str = Field(..., description="The name of the contributed values.")
    values: Any = Field(..., description="The values in the contributed values.")
    index: Array[str] = Field(
        ..., description="The entry index for the contributed values, matches the order of the `values` array."
    )
    values_structure: Dict[str, Any] = Field(
        {}, description="A machine readable description of the values structure. Typically not needed."
    )

    theory_level: Union[str, Dict[str, str]] = Field(..., description="A string representation of the theory level.")
    units: str = Field(..., description="The units of the values, can be any valid QCElemental unit.")
    theory_level_details: Optional[Union[str, Dict[str, Optional[str]]]] = Field(
        None, description="A detailed reprsentation of the theory level."
    )

    citations: Optional[List[Citation]] = Field(None, description="Citations associated with the contributed values.")
    external_url: Optional[str] = Field(None, description="An external URL to the raw contributed values data.")
    doi: Optional[str] = Field(None, description="A DOI for the contributed values data.")

    comments: Optional[str] = Field(None, description="Additional comments about the contributed values")

    @validator("values")
    def _make_array(cls, v):
        if isinstance(v, (list, tuple)) and isinstance(v[0], (float, int, str, bool)):
            v = np.array(v)

        return v


class Dataset(Collection):
    """
    The Dataset class for homogeneous point computations on many molecules.

    Attributes
    ----------
    client : PortalClient
        A PortalClient connected to a server.

    """

    class _DataModel(Collection._DataModel):

        # Defaults
        default_program: Optional[str] = None
        default_keywords: Dict[str, str] = {}
        default_driver: str = "energy"
        default_units: str = "kcal / mol"
        default_benchmark: Optional[str] = None

        alias_keywords: Dict[str, Dict[str, str]] = {}
        # records: List = []

        # Data
        # records: Optional[List[MoleculeEntry]] = []
        contributed_values: Dict[str, ContributedValues] = {}

        # History: driver, program, method (basis, keywords)
        history: Set[Tuple[str, str, str, Optional[str], Optional[str]]] = set()
        history_keys: Tuple[str, str, str, str, str] = ("driver", "program", "method", "basis", "keywords")

    def _apply_remappings(self, datadict):
        """This is a collection of temprorary shims applied
        to the datamodel after it is initialized.

        Eventually, the datamodel should be upgraded and database should be migrated to
        make these changes part of the datamodel itself.

        """
        datadict["records"] = {record["name"]: MoleculeEntry(**record) for record in (datadict["records"])}

        datadict["specs"] = dict()
        for spec_content in datadict["history"]:
            spec = {key: value for key, value in zip(datadict["history_keys"], spec_content)}

            keywords_name = spec["keywords"]
            spec["name"] = f"{spec['program']}/{keywords_name}"
            spec["keywords"] = self._client.get_keywords(datadict["alias_keywords"][spec["program"]][keywords_name])

            datadict["specs"][spec["name"]] = MoleculeEntrySpecification(**spec)

        return datadict

    def __init__(self, name: str, client: Optional["PortalClient"] = None, **kwargs: Any) -> None:
        """Initialize a Dataset Collection.

        Parameters
        ----------
        name : str
            The name of the Collection object; used to reference the collection on the server.
        client : PortalClient, optional
            A PortalClient connected to a server.
        **kwargs : Dict[str, Any]
            Additional keywords passed to the Collection and the initial data constructor.
            It is up to Collection subclasses to make use of that data.

        """
        super().__init__(name, client=client, **kwargs)

        self._units = self._data.default_units

        # If making a new dataset may need new hashes and json objects
        # self._new_molecules: Dict[str, Molecule] = {}
        # self._new_keywords: Dict[Tuple[str, str], KeywordSet] = {}
        # self._new_records: List[Dict[str, Any]] = []
        # self._updated_state = False

        # Load contributed columns
        # self._column_metadata: Dict[str, Any] = {}

    def _get_procedure_ids(self, specification: str, sieve: Optional[List[str]] = None) -> Dict[str, "ObjectId"]:
        """Get a mapping of record names to its object ID in the database.

        Parameters
        ----------
        spec : str
            The specification name to get mapping for.
        sieve : Optional[List[str]], optional
            List of record names to restrict the mapping to.

        Returns
        -------
        Dict[str, ObjectId]
            A dictionary of identifier to id mappings.

        """
        # Try to get the specification, will exception if not found.
        spec = self.get_spec(specification)

        mapper = {}
        for rec in self._data.records.values():
            if sieve and rec.name not in sieve:
                continue

            try:
                td_id = rec.object_map[spec.name]
                mapper[rec.name] = td_id
            except KeyError:
                pass

        return mapper

    def _query(
        self,
        specification: str,
        series: bool = False,
        pad: int = 0,
        include: Optional["QueryListStr"] = None,
    ) -> Union[Dict, pd.Series]:
        """Queries a given specification from the server.

        Parameters
        ----------
        specification : str
            The specification name to query.
        series : bool
            If True, return a `pandas.Series`.
        pad : int
            Spaces to pad spec names in progress output
        include : QueryListStr, optional
            Filters the returned fields, will return a dictionary rather than an object.

        Returns
        -------
        Union[Dict, pd.Series]
            Records collected from the server.

        """
        # Try to get the specification, will exception if not found.
        spec = self.get_spec(specification)

        mapper = self._get_procedure_ids(spec.name)
        query_ids = list(mapper.values())

        # Chunk up the queries
        procedures: List[Dict[str, Any]] = []
        for i in tqdm(
            range(0, len(query_ids), self._client.query_limit),
            desc="{} || {} ".format(specification.rjust(pad), self._client.address),
        ):
            chunk_ids = query_ids[i : i + self._client.query_limit]
            procedures.extend(self._client.get_records(id=chunk_ids, include=include))

        if include is not None:
            proc_lookup = {x["id"]: x for x in procedures}
        else:
            proc_lookup = {x.id: x for x in procedures}

        data = {}
        for name, oid in mapper.items():
            try:
                data[name] = proc_lookup[oid]
            except KeyError:
                data[name] = None

        if series:
            return pd.Series(data)
        else:
            return data

    def _pre_sync_prep(self, client: "PortalClient") -> None:
        pass

    def add_entry(
        self,
        name: str,
        molecule: "Molecule",
        comment: Optional[Dict[str, Any]] = None,
        local_results: Dict[str, Any] = None,
    ) -> None:

        if name in self._data.records:
            raise KeyError(f"Entry {name} already in the dataset.")

        # Build new objects
        molecule_id = self.client.add_molecules([molecule])[0]
        entry = MoleculeEntry(name=name, molecule=molecule_id, comment=comment, local_results=local_results)
        self._data.records[name] = entry

    # TODO: do this next; really need to sort out how we do spec names when Dataset
    # hasn't had this concept really before
    def add_spec(self, name, driver, program, method, basis, keywords):
        pass


# OLD STUFF BELOW; GRAB ONLY AS NEEDED


class _Dataset(Collection):
    def _get_data_records_from_db(self):
        self._check_client()
        # This is hacky. What we want to do is get records and contributed values correctly unpacked into pydantic
        # objects. So what we do is call get_collection with include. But we have to also include collection and
        # name in the query because they are required in the collection DataModel. But we can use these to check that
        # we got back the right data, so that's nice.
        response = self._client.get_collection(
            self.__class__.__name__.lower(),
            self.name,
            full_return=False,
            include=["records", "contributed_values", "collection", "name", "id"],
        )
        if not (response.data.id == self._data.id and response.data.name == self.name):
            raise ValueError("Got the wrong records and contributed values from the server.")
        # This works because get_collection builds a validated Dataset object
        self._data.__dict__["records"] = response.data.records
        self._data.__dict__["contributed_values"] = response.data.contributed_values

    def _entry_index(self, subset: Optional[List[str]] = None) -> pd.DataFrame:
        # TODO: make this fast for subsets
        if self._data.records is None:
            self._get_data_records_from_db()

        ret = pd.DataFrame(
            [[entry.name, entry.molecule_id] for entry in self._data.records], columns=["name", "molecule_id"]
        )
        if subset is None:
            return ret
        else:
            return ret.reset_index().set_index("name").loc[subset].reset_index().set_index("index")

    def _check_state(self) -> None:
        if self._new_molecules or self._new_keywords or self._new_records or self._updated_state:
            raise ValueError("New molecules, keywords, or records detected, run save before submitting new tasks.")

    def _canonical_pre_sync(self, client: "PortalClient") -> None:
        self._ensure_contributed_values()
        if self._data.records is None:
            self._get_data_records_from_db()
        for k in list(self._new_keywords.keys()):
            ret = client.add_keywords([self._new_keywords[k]])
            assert len(ret) == 1, "KeywordSet added incorrectly"
            self._data.alias_keywords[k[0]][k[1]] = ret[0]
            del self._new_keywords[k]
        self._updated_state = False

    def _pre_sync_prep(self, client: "PortalClient") -> None:
        self._canonical_pre_sync(client)

        # Preps any new molecules introduced to the Dataset before storing data.
        mol_ret = self._add_molecules_by_dict(client, self._new_molecules)

        # Update internal molecule UUID's to servers UUID's
        for record in self._new_records:
            molecule_hash = record.pop("molecule_hash")
            new_record = MoleculeEntry(molecule_id=mol_ret[molecule_hash], **record)
            self._data.records.append(new_record)

        self._new_records = []
        self._new_molecules = {}

    def get_entries(self, subset: Optional[List[str]] = None, force: bool = False) -> pd.DataFrame:
        """
        Provides a list of entries for the dataset

        Parameters
        ----------
        subset: Optional[List[str]], optional
            The indices of the desired subset. Return all indices if subset is None.
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
            ret = self._view.get_entries(subset)
        else:
            ret = self._entry_index(subset)
        return ret.copy()

    def _molecule_indexer(
        self, subset: Optional[Union[str, Set[str]]] = None, force: bool = False
    ) -> Dict[str, ObjectId]:
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
        if subset:
            if isinstance(subset, str):
                subset = {subset}
        index = self.get_entries(force=force, subset=subset)
        # index = index[index.name.isin(subset)]

        return {row["name"]: row["molecule_id"] for row in index.to_dict("records")}

    def _add_history(self, **history: Optional[str]) -> None:
        """
        Adds compute history to the dataset
        """
        if history.keys() != set(self._data.history_keys):
            raise KeyError("Internal error: Incorrect history keys passed in.")

        new_history = []
        for key in self._data.history_keys:

            value = history[key]
            if value is not None:
                value = value.lower()

            new_history.append(value)

        self._data.history.add(tuple(new_history))

    def list_values(
        self,
        method: Optional[Union[str, List[str]]] = None,
        basis: Optional[Union[str, List[str]]] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        driver: Optional[str] = None,
        name: Optional[Union[str, List[str]]] = None,
        native: Optional[bool] = None,
        force: bool = False,
    ) -> pd.DataFrame:
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
            "driver": driver,
        }

        if self._use_view(force):
            ret = self._view.list_values()
            spec["native"] = native
        else:
            ret = []
            if native in {True, None}:
                df = self._list_records(dftd3=False)
                df["native"] = True
                ret.append(df)

            if native in {False, None}:
                df = self._list_contributed_values()
                df["native"] = False
                ret.append(df)

            ret = pd.concat(ret)

        # Filter
        ret.fillna("None", inplace=True)
        ret = self._filter_records(ret, **spec)

        # Sort
        sort_index = ["native"] + list(self._data.history_keys[:-1])
        if "stoichiometry" in ret.columns:
            sort_index += ["stoichiometry", "name"]
        ret.set_index(sort_index, inplace=True)
        ret.sort_index(inplace=True)
        ret.reset_index(inplace=True)
        ret.set_index(["native"] + list(self._data.history_keys[:-1]), inplace=True)

        return ret

    @staticmethod
    def _filter_records(
        df: pd.DataFrame, **spec: Optional[Union[str, bool, List[Union[str, bool]], Tuple]]
    ) -> pd.DataFrame:
        """
        Helper for filtering records on a spec. Note that `None` is a wildcard while `"None"` matches `None` and NaN.
        """
        ret = df.copy()

        if len(ret) == 0:  # workaround pandas empty dataframe sharp edges
            return ret

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

    def list_records(
        self, dftd3: bool = False, pretty: bool = True, **search: Optional[Union[str, List[str]]]
    ) -> pd.DataFrame:
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

        history = pd.DataFrame(list(self._data.history), columns=self._data.history_keys)

        # Short circuit because merge and apply below require data
        if history.shape[0] == 0:
            ret = history.copy()
            ret["name"] = None
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

        # Drop duplicates due to stoich in some instances, this could be handled with multiple merges
        # Simpler to do it this way.
        history.drop_duplicates(inplace=True)

        # Find the returned subset
        ret = history.copy()

        # Add name column
        ret["name"] = ret.apply(
            lambda row: self._canonical_name(
                program=row["program"],
                method=row["method"],
                basis=row["basis"],
                keywords=row["keywords"],
                stoich=row.get("stoichiometry", None),
                driver=row["driver"],
            ),
            axis=1,
        )
        if show_dftd3 is False:
            ret = ret[ret["program"] != "dftd3"]

        return ret

    def get_values(
        self,
        method: Optional[Union[str, List[str]]] = None,
        basis: Optional[Union[str, List[str]]] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        driver: Optional[str] = None,
        name: Optional[Union[str, List[str]]] = None,
        native: Optional[bool] = None,
        subset: Optional[Union[str, List[str]]] = None,
        force: bool = False,
    ) -> pd.DataFrame:
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
        subset: Optional[List[str]], optional
            The indices of the desired subset. Return all indices if subset is None.
        force : bool, optional
            Data is typically cached, forces a new query if True

        Returns
        -------
        DataFrame
            A DataFrame of values with columns corresponding to methods and rows corresponding to molecule entries.
        """
        return self._get_values(
            method=method,
            basis=basis,
            keywords=keywords,
            program=program,
            driver=driver,
            name=name,
            native=native,
            subset=subset,
            force=force,
        )

    def _get_values(
        self,
        native: Optional[bool] = None,
        force: bool = False,
        subset: Optional[Union[str, List[str]]] = None,
        **spec: Union[List[str], str, None],
    ) -> pd.DataFrame:
        ret = []

        if subset is None:
            subset_set = set(self.get_index(force=force))
        elif isinstance(subset, str):
            subset_set = {subset}
        elif isinstance(subset, list):
            subset_set = set(subset)
        else:
            raise ValueError(f"Subset must be str, List[str], or None. Got {type(subset)}")

        if native in {True, None}:
            spec_nodriver = spec.copy()
            driver = spec_nodriver.pop("driver")
            if driver is not None and driver != self._data.default_driver:
                raise KeyError(
                    f"For native values, driver ({driver}) must be the same as the dataset's default driver "
                    f"({self._data.default_driver}). Consider using get_records instead."
                )
            df = self._get_native_values(subset=subset_set, force=force, **spec_nodriver)
            ret.append(df)

        if native in {False, None}:
            df = self._get_contributed_values(subset=subset_set, force=force, **spec)
            ret.append(df)
        ret_df = pd.concat(ret, axis=1)
        ret_df = ret_df.loc[subset if subset is not None else self.get_index()]

        return ret_df

    def _get_native_values(
        self,
        subset: Set[str],
        method: Optional[Union[str, List[str]]] = None,
        basis: Optional[Union[str, List[str]]] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        name: Optional[Union[str, List[str]]] = None,
        force: bool = False,
    ) -> pd.DataFrame:
        """
        Obtains records matching the provided search criteria.
        Defaults to the standard programs and keywords if not provided.

        Parameters
        ----------
        subset: Set[str]
            The indices of the desired subset.
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
        au_units = {"energy": "hartree", "gradient": "hartree/bohr", "hessian": "hartree/bohr**2"}

        # So that datasets with no records do not require a default program and default keywords
        if len(self.list_records()) == 0:
            return pd.DataFrame(index=self.get_index(subset))

        queries = self._form_queries(method=method, basis=basis, keywords=keywords, program=program, name=name)
        names = []
        new_queries = []
        for _, query in queries.iterrows():

            query = query.replace({np.nan: None}).to_dict()
            if "stoichiometry" in query:
                query["stoich"] = query.pop("stoichiometry")

            qname = query["name"]
            names.append(qname)
            if force:
                self._column_metadata[qname] = query
                new_queries.append(query)

        new_data = pd.DataFrame(index=subset)

        if not self._use_view(force):
            units: Dict[str, str] = {}
            for query in new_queries:
                driver = query.pop("driver")
                qname = query.pop("name")
                data = self.get_records(
                    query.pop("method").upper(), include=["return_result"], merge=True, subset=subset, **query
                )
                new_data[qname] = data["return_result"]
                units[qname] = au_units[driver]
                query["name"] = qname
        else:
            for query in new_queries:
                query["native"] = True
            new_data, units = self._view.get_values(new_queries, subset)

        for query in new_queries:
            qname = query["name"]
            new_data[qname] *= constants.conversion_factor(units[qname], self.units)
            self._column_metadata[qname].update({"native": True, "units": self.units})

        return self._df.loc[subset, names]

    def _form_queries(
        self,
        method: Optional[Union[str, List[str]]] = None,
        basis: Optional[Union[str, List[str]]] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        stoich: Optional[str] = None,
        name: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
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
                warnings.warn(
                    "Name and additional field were provided. Only name will be used as a selector.", RuntimeWarning
                )
            queries = self.list_records(name=name, dftd3=True, pretty=False)

        if queries.shape[0] > 10 and self._disable_query_limit is False:
            raise TypeError("More than 10 queries formed, please narrow the search.")
        return queries

    def _visualize(
        self,
        metric,
        bench,
        query: Dict[str, Union[Optional[str], List[str]]],
        groupby: Optional[str] = None,
        return_figure=None,
        digits=3,
        kind="bar",
        show_incomplete: bool = False,
    ) -> "plotly.Figure":

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
                raise KeyError(f"Groupby option {groupby} not found in query, must provide a search on this parameter.")

            if (groupby != "d3") and (not isinstance(query[groupby], (tuple, list))):
                raise KeyError(f"Groupby option {groupby} must be a list.")

            query_names = []
            queries = []
            if groupby == "d3":
                base = [method.upper().split("-D3")[0] for method in query["method"]]
                d3types = [method.upper().replace(b, "").replace("-D", "D") for method, b in zip(query["method"], base)]

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

        title = f"{self._data.name} Dataset Statistics"

        series = []
        for q, name in zip(queries, query_names):

            if len(q) == 0:
                raise KeyError("No query matches, nothing to visualize!")

            # Pull the values
            if "stoichiometry" in q:
                q["stoich"] = q.pop("stoichiometry")
            values = self.get_values(**q)

            if not show_incomplete:
                values = values.dropna(axis=1, how="any")

            # Create the statistics
            stat = self.statistics(metric, values, bench=bench)
            stat = stat.round(digits)
            stat.sort_index(inplace=True)
            stat.name = name

            # Munge the column names based on the groupby parameter
            col_names = {}
            for k, v in stat.iteritems():
                record = self._column_metadata[k].copy()
                if groupby == "d3":
                    record["method"] = record["method"].upper().split("-D3")[0]

                elif groupby:
                    record[groupby] = None

                index_name = self._canonical_name(
                    record["program"],
                    record["method"],
                    record["basis"],
                    record["keywords"],
                    stoich=record.get("stoich"),
                )

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

    def visualize(
        self,
        method: Optional[str] = None,
        basis: Optional[str] = None,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
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
        show_incomplete: bool, optional
            Display statistics method/basis set combinations where results are incomplete

        Returns
        -------
        plotly.Figure
            The requested figure.
        """

        query = {"method": method, "basis": basis, "keywords": keywords, "program": program}
        query = {k: v for k, v in query.items() if v is not None}

        return self._visualize(metric, bench, query=query, groupby=groupby, return_figure=return_figure, kind=kind)

    def _canonical_name(
        self,
        program: Optional[str] = None,
        method: Optional[str] = None,
        basis: Optional[str] = None,
        keywords: Optional[str] = None,
        stoich: Optional[str] = None,
        driver: Optional[str] = None,
    ) -> str:
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

        if keywords and (keywords != self._data.default_keywords.get(program, None)):
            name = f"{name}-{keywords}"

        if program and (program.lower() != self._data.default_program):
            name = f"{name}-{program.title()}"

        if stoich:
            if name == "":
                name = stoich.lower()
            elif stoich.lower() != "default":
                name = f"{stoich.lower()}-{name}"

        return name

    def _default_parameters(
        self,
        program: Optional[str],
        method: str,
        basis: Optional[str],
        keywords: Optional[str],
        stoich: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Union[str, "KeywordSet"]], Dict[str, str]]:
        """
        Takes raw input parsed parameters and applies defaults to them.
        """

        # Handle default program
        if program is None:
            if self._data.default_program is None:
                raise KeyError("No default program was set and none was provided.")
            program = self._data.default_program
        else:
            program = program.lower()

        driver = self._data.default_driver

        # Handle keywords
        keywords_alias = keywords
        if keywords is None:
            if program in self._data.default_keywords:
                keywords_alias = self._data.default_keywords[program]
                keywords = self._data.alias_keywords[program][keywords_alias]
        else:
            if (program not in self._data.alias_keywords) or (keywords not in self._data.alias_keywords[program]):
                raise KeyError("KeywordSet alias '{}' not found for program '{}'.".format(keywords, program))

            keywords_alias = keywords
            keywords = self._data.alias_keywords[program][keywords]

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
            molecules: List["Molecule"] = []
            for i in range(0, len(molecule_ids), self._client.query_limit):
                molecules.extend(self._client.query_molecules(id=molecule_ids[i : i + self._client.query_limit]))
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

    def _get_records(
        self,
        indexer: Dict[Any, ObjectId],
        query: Dict[str, Any],
        include: Optional[List[str]] = None,
        merge: bool = False,
        raise_on_plan: Union[str, bool] = False,
    ) -> "pd.Series":
        """
        Runs a query based on an indexer which is index : molecule_id

        Parameters
        ----------
        indexer : Dict[str, ObjectId]
            A key/value index of molecules to query
        query : Dict[str, Any]
            A results query
        include : Optional[List[str]], optional
            The attributes to return. Otherwise returns SinglepointRecord objects.
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
        plan = self._composition_planner(**query)
        if raise_on_plan and (len(plan) > 1):
            if raise_on_plan is True:
                raise KeyError("Recieved a multi-stage plan when this function does not support multi-staged plans.")
            else:
                raise KeyError(raise_on_plan)

        for query_set in plan:

            query_set["keywords"] = self.get_keywords(query_set["keywords"], query_set["program"], return_id=True)
            # Set the index to remove duplicates
            molecules = list(set(indexer.values()))
            if include:
                proj = [k.lower() for k in include]
                if "molecule" not in proj:
                    proj.append("molecule")
                query_set["include"] = proj

            # Chunk up the queries
            records: List[SinglepointRecord] = []
            for i in range(0, len(molecules), self._client.query_limit):
                query_set["molecule"] = molecules[i : i + self._client.query_limit]
                records.extend(self._client.query_results(**query_set))

            if include is None:
                records = [{"molecule": x.molecule, "record": x} for x in records]

            records = pd.DataFrame.from_dict(records)

            df = pd.DataFrame.from_dict(indexer, orient="index", columns=["molecule"])
            df.reset_index(inplace=True)

            if records.shape[0] > 0:
                # Outer join on left to merge duplicate molecules
                df = df.merge(records, how="left", on="molecule")
            else:
                # No results, fill NaN values
                if include is None:
                    df["record"] = None
                else:
                    for k in include:
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

    def _compute(
        self,
        compute_keys: Dict[str, Union[str, None]],
        molecules: Union[List[str], pd.Series],
        tag: Optional[str] = None,
        priority: Optional[str] = None,
        protocols: Optional[Dict[str, Any]] = None,
    ) -> ComputeResponse:
        """
        Internal compute function
        """

        name, dbkeys, history = self._default_parameters(
            compute_keys["program"],
            compute_keys["method"],
            compute_keys["basis"],
            compute_keys["keywords"],
            stoich=compute_keys.get("stoich", None),
        )

        self._check_client()
        self._check_state()

        umols = list(set(molecules))

        ids: List[Optional[ObjectId]] = []
        submitted: List[ObjectId] = []
        existing: List[ObjectId] = []
        for compute_set in self._composition_planner(**dbkeys):

            for i in range(0, len(umols), self._client.query_limit):
                chunk_mols = umols[i : i + self._client.query_limit]
                ret = self._client.add_compute(
                    **compute_set, molecule=chunk_mols, tag=tag, priority=priority, protocols=protocols
                )

                ids.extend(ret.ids)
                submitted.extend(ret.claimed)
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
        for column in self._df.columns:
            try:
                self._df[column] *= constants.conversion_factor(self._column_metadata[column]["units"], value)

                # Cast units to quantities so that `kcal / mol` == `kilocalorie / mole`
                metadata_quantity = constants.Quantity(self._column_metadata[column]["units"])
                self_quantity = constants.Quantity(self._units)
                if metadata_quantity != self_quantity:
                    warnings.warn(
                        f"Data column '{column}' did not have the same units as the dataset. "
                        f"This has been corrected."
                    )
                self._column_metadata[column]["units"] = value
            except (ValueError, TypeError) as e:
                # This is meant to catch pint.errors.DimensionalityError without importing pint, which is too slow.
                # In pint <=0.9, DimensionalityError is a ValueError.
                # In pint >=0.10, DimensionalityError is TypeError.
                if e.__class__.__name__ == "DimensionalityError":
                    pass
                else:
                    raise
        self._units = value

    def set_default_program(self, program: str) -> bool:
        """
        Sets the default program.

        Parameters
        ----------
        program : str
            The program to default to.
        """

        self._data.__dict__["default_program"] = program.lower()
        return True

    def set_default_benchmark(self, benchmark: str) -> bool:
        """
        Sets the default benchmark value.

        Parameters
        ----------
        benchmark : str
            The benchmark to default to.
        """

        self._data.__dict__["default_benchmark"] = benchmark
        return True

    def add_keywords(self, alias: str, program: str, keyword: "KeywordSet", default: bool = False) -> bool:
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
        if program not in self._data.alias_keywords:
            self._data.alias_keywords[program] = {}

        if alias in self._data.alias_keywords[program]:
            raise KeyError("Alias '{}' already set for program {}.".format(alias, program))

        self._new_keywords[(program, alias)] = keyword

        if default:
            self._data.default_keywords[program] = alias
        return True

    def list_keywords(self) -> pd.DataFrame:
        """Lists keyword aliases for each program in the dataset.

        Returns
        -------
        pd.DataFrame
            A dataframe containing programs, keyword aliases, KeywordSet ids, and whether those keywords are the
            default for a program. Indexed on program.
        """
        data = []
        for program, kwaliases in self._data.alias_keywords.items():
            prog_default_kw = self._data.default_keywords.get(program, None)
            for kwalias, kwid in kwaliases.items():
                data.append(
                    {
                        "program": program,
                        "keywords": kwalias,
                        "id": kwid,
                        "default": prog_default_kw == kwalias,
                    }
                )
        return pd.DataFrame(data).set_index("program")

    def get_keywords(self, alias: str, program: str, return_id: bool = False) -> Union["KeywordSet", str]:
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
        if (program not in self._data.alias_keywords) or (alias not in self._data.alias_keywords[program]):
            raise KeyError("Keywords {}: {} not found.".format(program, alias))

        kwid = self._data.alias_keywords[program][alias]
        if return_id:
            return kwid
        else:
            return self._client.query_keywords([kwid])[0]

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
        self.get_entries(force=True)
        self._ensure_contributed_values()

        # Convert and validate
        if isinstance(contrib, ContributedValues):
            contrib = contrib.copy()
        else:
            contrib = ContributedValues(**contrib)

        if set(contrib.index) != set(self.get_index()):
            raise ValueError("Contributed values indices do not match the entries in the dataset.")

        # Check the key
        key = contrib.name.lower()
        if (key in self._data.contributed_values) and (overwrite is False):
            raise KeyError(
                "Key '{}' already found in contributed values. Use `overwrite=True` to force an update.".format(key)
            )

        self._data.contributed_values[key] = contrib
        self._updated_state = True

    def _ensure_contributed_values(self) -> None:
        if self._data.contributed_values is None:
            self._get_data_records_from_db()

    def _list_contributed_values(self) -> pd.DataFrame:
        """
        Lists all specifications of contributed data, i.e. method, program, basis set, keyword set, driver combinations

        Returns
        -------
        DataFrame
            Contributed value specifications.
        """
        self._ensure_contributed_values()
        ret = pd.DataFrame(columns=self._data.history_keys + tuple(["name"]))

        cvs = (
            (cv_data.name, cv_data.theory_level_details) for (cv_name, cv_data) in self._data.contributed_values.items()
        )

        for cv_name, theory_level_details in cvs:
            spec = {"name": cv_name}
            for k in self._data.history_keys:
                spec[k] = "Unknown"
            # ReactionDataset uses "default" as a default value for stoich,
            # but many contributed datasets lack a stoich field
            if "stoichiometry" in self._data.history_keys:
                spec["stoichiometry"] = "default"
            if isinstance(theory_level_details, dict):
                spec.update(**theory_level_details)
            ret = ret.append(spec, ignore_index=True)

        return ret

    def _update_cache(self, new_data: pd.DataFrame) -> None:
        new_df = pd.DataFrame(
            index=set(self._df.index) | set(new_data.index), columns=set(self._df.columns) | set(new_data.columns)
        )
        new_df.update(new_data)
        new_df.update(self._df)
        self._df = new_df

    def _get_contributed_values(self, subset: Set[str], force: bool = False, **spec) -> pd.DataFrame:

        cv_list = self.list_values(native=False, force=force).reset_index()
        queries = self._filter_records(cv_list.rename(columns={"stoichiometry": "stoich"}), **spec)
        column_names: List[str] = []
        new_queries = []

        for query in queries.to_dict("records"):
            column_name = query["name"]
            column_names.append(column_name)
            if force:
                self._column_metadata[column_name] = query
                new_queries.append(query)

        new_data = pd.DataFrame(index=subset)
        if not self._use_view(force):
            self._ensure_contributed_values()
            units: Dict[str, str] = {}

            for query in new_queries:
                data = self._data.contributed_values[query["name"].lower()].copy()
                column_name = data.name

                # Annoying work around to prevent some pandas magic
                if isinstance(data.values[0], (int, float, bool, np.number)):
                    values = data.values
                else:
                    # TODO temporary patch until msgpack collections
                    if isinstance(data.theory_level_details, dict) and "driver" in data.theory_level_details:
                        cv_driver = data.theory_level_details["driver"]
                    else:
                        cv_driver = self._data.default_driver

                    if cv_driver == "gradient":
                        values = [np.array(v).reshape(-1, 3) for v in data.values]
                    else:
                        values = [np.array(v) for v in data.values]

                new_data[column_name] = pd.Series(values, index=data.index)[subset]
                units[column_name] = data.units
        else:
            for query in new_queries:
                query["native"] = False
            new_data, units = self._view.get_values(new_queries, subset)

        # convert units
        for query in new_queries:
            column_name = query["name"]
            metadata = {"native": False}
            try:
                new_data[column_name] *= constants.conversion_factor(units[column_name], self.units)
                metadata["units"] = self.units
            except (ValueError, TypeError) as e:
                # This is meant to catch pint.errors.DimensionalityError without importing pint, which is too slow.
                # In pint <=0.9, DimensionalityError is a ValueError.
                # In pint >=0.10, DimensionalityError is TypeError.
                if e.__class__.__name__ == "DimensionalityError":
                    metadata["units"] = units[column_name]
                else:
                    raise
            self._column_metadata[column_name].update(metadata)

        return self._df.loc[subset, column_names]

    def get_molecules(
        self, subset: Optional[Union[str, Set[str]]] = None, force: bool = False
    ) -> Union[pd.DataFrame, "Molecule"]:
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

    def get_records(
        self,
        method: str,
        basis: Optional[str] = None,
        *,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        include: Optional[List[str]] = None,
        subset: Optional[Union[str, Set[str]]] = None,
        merge: bool = False,
    ) -> Union[pd.DataFrame, "SinglepointRecord"]:
        """
        Queries full SinglepointRecord objects from the database.

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
        include : Optional[List[str]], optional
            The attributes to return. Otherwise returns SinglepointRecord objects.
        subset : Optional[Union[str, Set[str]]], optional
            The index subset to query on
        merge : bool
            Merge multiple results into one (as in the case of DFT-D3).
            This only works when include=['return_results'], as in get_values.

        Returns
        -------
        Union[pd.DataFrame, 'SinglepointRecord']
            Either a DataFrame of indexed SinglepointRecords or a single SinglepointRecord if a single subset string was provided.
        """
        name, _, history = self._default_parameters(program, method, basis, keywords)
        if len(self.list_records(**history)) == 0:
            raise KeyError(f"Requested query ({name}) did not match a known record.")

        indexer = self._molecule_indexer(subset=subset, force=True)
        df = self._get_records(indexer, history, include=include, merge=merge)

        if not merge and len(df) == 1:
            df = df[0]

        if len(df) == 0:
            raise KeyError("Query matched no records!")

        if isinstance(subset, str):
            return df.iloc[0, 0]
        else:
            return df

    def add_entry(self, name: str, molecule: "Molecule", **kwargs: Dict[str, Any]) -> None:
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

    def compute(
        self,
        method: str,
        basis: Optional[str] = None,
        *,
        keywords: Optional[str] = None,
        program: Optional[str] = None,
        tag: Optional[str] = None,
        priority: Optional[str] = None,
        protocols: Optional[Dict[str, Any]] = None,
    ) -> ComputeResponse:
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
        protocols: Optional[Dict[str, Any]], optional
            Protocols for store more or less data per field. Current valid
            protocols: {'wavefunction'}

        Returns
        -------
        ComputeResponse
            An object that contains the submitted ObjectIds of the new compute. This object has the following fields:
              - ids: The ObjectId's of the task in the order of input molecules
              - submitted: A list of ObjectId's that were submitted to the compute queue
              - existing: A list of ObjectId's of tasks already in the database
        """
        self.get_entries(force=True)
        compute_keys = {"program": program, "method": method, "basis": basis, "keywords": keywords}

        molecule_idx = [e.molecule_id for e in self._data.records]

        ret = self._compute(compute_keys, molecule_idx, tag, priority, protocols)
        self.save()

        return ret

    def get_index(self, subset: Optional[List[str]] = None, force: bool = False) -> List[str]:
        """
        Returns the current index of the database.

        Returns
        -------
        ret : List[str]
            The names of all reactions in the database
        """
        return list(self.get_entries(subset=subset, force=force)["name"].unique())

    ## Statistical quantities
    def statistics(
        self, stype: str, value: str, bench: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> Union[np.ndarray, pd.Series, np.float64]:
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
            bench = self._data.default_benchmark

        if bench is None:
            raise KeyError("No benchmark provided and default_benchmark is None!")

        return wrap_statistics(stype.upper(), self, value, bench, **kwargs)

    def _use_view(self, force: bool = False) -> bool:
        """Helper function to decide whether to use a locally available HDF5 view"""
        return (force is False) and (self._view is not None) and (self._disable_view is False)

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
        return self._df[args]

    # NOTE: needed for older datasets that featured `-d3` in method call, using `dftd3`
    @staticmethod
    def _composition_planner(program=None, method=None, basis=None, driver=None, keywords=None):
        """
        Plans out a given query into multiple pieces
        """

        base = {"program": program, "method": method, "basis": basis, "driver": driver, "keywords": keywords}

        if ("-d3" in method.lower()) and ("dftd3" != program.lower()) and ("hessian" != driver.lower()):
            dftd3keys = {"program": "dftd3", "method": method, "basis": None, "driver": driver, "keywords": None}
            base["method"] = method.lower().split("-d3")[0]

            return [dftd3keys, base]

        else:
            return [base]

    ## View functionality

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

    def download(
        self, local_path: Optional[Union[str, Path]] = None, verify: bool = True, progress_bar: bool = True
    ) -> None:
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
        progress_bar: bool, optional
            Display a download progress bar. Default: True
        """
        chunk_size = 8192
        if self._data.view_url_hdf5 is None:
            raise ValueError("A view for this dataset is not available on the server")

        if local_path is not None:
            local_path = Path(local_path)
        else:
            self._view_tempfile = tempfile.NamedTemporaryFile()  # keep temp file alive until self is destroyed
            local_path = self._view_tempfile.name

        r = requests.get(self._data.view_url_hdf5, stream=True)
        pbar = None
        if progress_bar:
            try:
                file_length = int(r.headers.get("content-length"))
                pbar = tqdm(total=file_length, initial=0, unit="B", unit_scale=True)
            except Exception:
                warnings.warn("Failed to create download progress bar", RuntimeWarning)

        with open(local_path, "wb") as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
                if pbar is not None:
                    pbar.update(chunk_size)

        with open(local_path, "rb") as f:
            magic = f.read(2)
            gzipped = magic == b"\x1f\x8b"
        if gzipped:
            extract_tempfile = tempfile.NamedTemporaryFile()  # keep temp file alive until self is destroyed
            with gzip.open(local_path, "rb") as fgz:
                with open(extract_tempfile.name, "wb") as f:
                    f.write(fgz.read())
            self._view_tempfile = extract_tempfile
            local_path = self._view_tempfile.name

        if verify:
            remote_checksum = self._data.view_metadata["blake2b_checksum"]
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


register_collection(Dataset)
