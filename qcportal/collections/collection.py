"""Base Collection classes.

"""

import abc
import copy
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

import pandas as pd
from tqdm import tqdm

from ...interface.models import ProtoModel, QCSpecification

if TYPE_CHECKING:  # pragma: no cover
    from .. import PortalClient
    from ..models import ObjectId


class Collection(abc.ABC):
    def __init__(self, name: str, client: Optional["PortalClient"] = None, **kwargs: Any):
        """Initialize a Collection.

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

        self._client = client

        if (self._client is not None) and not (self._client.__class__.__name__ == "PortalClient"):
            raise TypeError("Expected PortalClient as `client` kwarg, found {}.".format(type(self._client)))

        if "collection" not in kwargs:
            kwargs["collection"] = self.__class__.__name__.lower()

        kwargs["name"] = name

        # apply remappings from stored DB data if defined
        kwargs = self._apply_remappings(kwargs)

        # Create the data model
        self._data = self._DataModel(**kwargs)

    class _DataModel(ProtoModel):
        """
        Internal base model typed by PyDantic.

        This structure validates input, allows server-side validation,
        and puts information into a form that is passable between server and client.

        Subclasses of Collection can extend this class to supplement the data
        defined by the Collection.

        Attributes
        ----------
        id : str
            The server-side id of the Collection, if sourced from there.
            If "local", then sourced from the local PortalClient.
        name : str
            The name of the Collection.
        collection : str
            The Collection type. This is the name of the Collection subclass.
        provenance : Dict[str, str]
            Key-values giving point-in-time creation metadata for this Collection instance.
        tags : List[str]
            Individual strings (case-insensitive) for discoverability/queryability of
            this Collection.
        tagline : Optional[str]
            Short description of this Collection.
            Try to keep within 80 characters for displayability.
        description : Optional[str]
            Long description of this Collection.
        group : str
            The organization that submitted the Collection.
        visibility : bool
            If `True`, list Collection by default through the PortalClient.
        metadata : Dict[str, Any]
            Any additional user-generated metadata for the Collection.
        records : Dict[str, Any]
            A dict of all entries in the Collection, with entry names as keys,
            entries as values.
        history : Set[str]

        specs : Dict[str, Any]
            A dict of all specs applied to the Collection, with spec names as keys,
            specs as values.

        """

        id: str = "local"
        name: str

        collection: str
        provenance: Dict[str, str] = {}

        tags: List[str] = []
        tagline: Optional[str] = None
        description: Optional[str] = None

        group: str = "default"
        visibility: bool = True

        metadata: Dict[str, Any] = {}

        # NOTE: would really like to change this to `entries`
        # we should make it a principle that we *never* have result data here
        records: Dict[str, Any] = {}

        # NOTE: must complete docstring for history once we re-implement its usage
        history: Set[str] = set()
        specs: Dict[str, Any] = {}

        # NOTE: needed for backwards compatibility with existing datasets
        view_url_hdf5: Optional[str] = None
        view_url_plaintext: Optional[str] = None
        view_metadata: Optional[Dict[str, str]] = None
        view_available: bool = False

    def __str__(self) -> str:
        """
        A simple string representation of the Collection.

        Returns
        -------
        ret : str
            A representation of the Collection.

        Examples
        --------

        >>> repr(obj)
        Collection(name=`S22`, id='5b7f1fd57b87872d2c5d0a6d', client=`localhost:8888`)
        """

        client = None
        if self._client:
            client = self._client.address

        class_name = self.__class__.__name__
        ret = "{}(".format(class_name)
        ret += "name=`{}`, ".format(self._data.name)
        ret += "id='{}', ".format(self._data.id)
        ret += "client='{}') ".format(client)

        return ret

    def __repr__(self) -> str:
        return f"<{self}>"

    def __getitem__(self, spec: Union[List[str], str]):
        return self._get_records_for_spec(spec)

    def _get_records_for_spec(self, spec: Union[List[str], str]):
        if isinstance(spec, list):
            pad = max(map(len, spec))
            return {sp: self._query(sp, pad=pad) for sp in spec}
        else:
            return self._query(spec)

    def _check_client(self):
        if self._client is None:
            raise AttributeError("This method requires a PortalClient and no client was set")

    def _apply_remappings(self, datadict):
        return datadict

    @property
    def name(self) -> str:
        return self._data.name

    ## inits and export

    @classmethod
    def from_server(cls, client: "PortalClient", name: str) -> "Collection":
        """Creates a new class from a server

        Parameters
        ----------
        client : PortalClient
            A PortalClient connected to a server.
        name : str
            The name of the collection to pull from.

        Returns
        -------
        Collection
            A constructed collection.

        """

        if not (client.__class__.__name__ == "PortalClient"):
            raise TypeError("Expected a PortalClient as first argument, found {}.".format(type(client)))

        class_name = cls.__name__.lower()
        tmp_data = client.get_collection(class_name, name, full_return=True)
        if tmp_data.meta.n_found == 0:
            raise KeyError("Warning! `{}: {}` not found.".format(class_name, name))

        return cls.from_json(tmp_data.data[0], client=client)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], client: "FractalClient" = None) -> "Collection":
        """Creates a new Collection instance from a dict representation.

        Allows roundtrips from `Collection.to_dict`.

        Parameters
        ----------
        data : Dict[str, Any]
            A dict to create a new Collection instance from.
        client : FractalClient, optional
            A FractalClient connected to a server.

        Returns
        -------
        Collection
            A Collection instance.
        """
        # Check we are building the correct object
        class_name = cls.__name__.lower()
        if "collection" not in data:
            raise KeyError("Attempted to create Collection from JSON, but no `collection` field found.")

        if data["collection"] != class_name:
            raise KeyError(
                "Attempted to create Collection from JSON with class {}, but found collection type of {}.".format(
                    class_name, data["collection"]
                )
            )

        name = data.pop("name")
        # Allow PyDantic to handle type validation
        ret = cls(name, client=client, **data)
        return ret

    @classmethod
    def from_json(
        cls, jsondata: Optional[str] = None, filename: Optional[str] = None, client: "FractalClient" = None
    ) -> "Collection":
        """Creates a new Collection instance from a JSON string.

        Allows roundtrips from `Collection.to_json`.
        One of `jsondata` or `filename` must be provided.

        Parameters
        ----------
        jsondata : str, Optional, Default: None
            The JSON string to create a new Collection instance from.
        filename : str, Optional, Default: None
            The filename to read JSON data from.
        client : FractalClient, optional
            A FractalClient connected to a server.

        Returns
        -------
        Collection
            A Collection instance.
        """
        if (jsondata is not None) and (filename is not None):
            raise ValueError("One of `jsondata` or `filename` must be specified, not both")

        if jsondata is not None:
            data = json.loads(jsondata)
        elif filename is not None:
            with open(filename, "r") as jsonfile:
                data = json.load(jsonfile)
        else:
            raise ValueError("One of `jsondata` or `filename` must be specified")

        return cls.from_dict(data, client)

    def to_dict(self):
        """Return a copy of the current Collection data as a Python dict.

        Returns
        -------
        ret : dict
            A Python dict representation of the Collection data.
        """
        datadict = self.data.dict()
        return copy.deepcopy(datadict)

    def to_json(self, filename: Optional[str] = None):
        """Return JSON string representation of the Collection.

        If a filename is provided, dumps the file to disk.
        Otherwise returns data as a JSON string.

        Parameters
        ----------
        filename : str, Optional, Default: None
            The filename to write JSON data to.

        Returns
        -------
        ret : dict
            If `filename=None`, a JSON representation of the Collection.
            Otherwise `None`.
        """
        jsondata = self.json()
        if filename is not None:
            with open(filename, "w") as open_file:
                open_file.write(jsondata)
        else:
            return jsondata

    ## entry touchpoints

    @property
    def entry_names(self):
        """A list of all entry names."""
        return list(self.list_entries(as_dict=True).keys())

    @property
    def entries(self):
        """A dict with all entry names in this Collection as keys and entries as values."""
        return self.list_entries(as_dict=True)

    def list_entries(self, as_dict: bool = False):
        """Return all entries in this Collection.

        Parameters
        ----------
        as_dict: bool, optional
            Return output as a dict instead of a list.
            Entry names as keys, entries as values.

        Returns
        -------
        Union[List, Dict]
            Return all entries in a list.
            If `as_dict=True`, returns dict with entry names as keys, entries as values.

        """
        if as_dict:
            return {x.name: x for x in self._data.records.values()}
        else:
            return list(self._data.records.values())

    def get_entry(self, name: str) -> "Entry":
        """Get an individual entry by name from this Collection.

        Parameters
        ----------
        name : str
            The name of the entry.

        Returns
        -------
        entry : Entry
            An entry instance corresponding to this Collection type.

        """
        try:
            return self.entries[name]
        except KeyError:
            raise KeyError(f"Could not find entry name '{name}' in the dataset.")

    @abc.abstractmethod
    def add_entry(self, name: str, **entry: "Entry") -> None:
        """Add an entry to the Collection.

        Parameters
        ----------
        name : str
            The name of the entry.
        entry : Entry
            An entry instance corresponding to this Collection type.
        """
        pass

    ## spec touchpoints

    @property
    def spec_names(self):
        """A list of all spec names applied to this Collection."""
        return list(self.list_specs(as_dict=True).keys())

    @property
    def specs(self):
        """A dict with all spec names in this Collection as keys and specs as values."""
        return self.list_specs(as_dict=True)

    def list_specs(self, as_dict: bool = False):
        """Return all specs in this Collection.

        Parameters
        ----------
        as_dict: bool, optional
            Return output as a dict instead of a list.
            Spec names as keys, specs as values.

        Returns
        -------
        Union[List, Dict]
            Return all specs in a list.
            If `as_dict=True`, returns dict with spec names as keys, specs as values.

        """
        if as_dict:
            return {x.name: x for x in self._data.specs.values()}
        else:
            return list(self._data.specs.values())

    def get_spec(self, name: str) -> QCSpecification:
        """Get an individual spec by name from this Collection.

        Parameters
        ----------
        name : str
            The name of the spec.

        Returns
        -------
        spec : QCSpecification
            A full quantum chemistry specification.

        """
        try:
            return self.specs[name]
        except KeyError:
            raise KeyError(f"Could not find spec name '{name}' in the dataset.")

    # TODO: keyword id handling should not be in the UI
    # May require excising this from QCSpecification, or making
    # keywords explicit there
    # need to examine where else in the Fractal stack QCSpecification is used
    @abc.abstractmethod
    def add_spec(
        self,
        name: str,
        **spec: QCSpecification,
    ) -> None:
        """Add a compute spec to the Collection.

        Parameters
        ----------
        name : str
            The name of the specification.
        spec : QCSpecification
            A full quantum chemistry specification.
        """
        pass

    ## record touchpoints

    # @abc.abstractmethod
    def get_record(self, entry_name, spec_name):
        pass

    # @abc.abstractproperty
    def loc(self):
        pass

    # @abc.abstractproperty
    def iter(self):
        pass

    ## server interaction

    # @abc.abstractmethod
    def _pre_sync_prep(self, client: "PortalClient"):
        """Additional actions to take before syncing, done as the last step before data is written.

        This does not return anything but can prep the `self._data` object before storing it.
        Has access to the `client` if needed to do pre-conditioning.

        Parameters
        ----------
        client : PortalClient
            A PortalClient connected to a server used for storage access
        """
        pass

    def sync(self, client: Optional["PortalClient"] = None) -> "ObjectId":
        """Synchronizes Collection data to the server.

        Data synchronized includes e.g. indices, options, and new molecules.

        Parameters
        ----------
        client : PortalClient, optional
            A PortalClient connected to a server to upload to.

        Returns
        -------
        ObjectId
            The ObjectId of the saved collection.

        """
        class_name = self.__class__.__name__.lower()

        if self._data.name == "":
            raise AttributeError("Collection:save: {} must have a name!".format(class_name))

        if client is None:
            self._check_client()
            client = self._client

        self._pre_sync_prep(client)

        # For a collection that is not already on the database
        if self._data.id == self._data.__fields__["id"].default:
            response = client.add_collection(self._data.dict(), overwrite=False, full_return=True)
            if response.meta.success is False:
                raise KeyError(f"Error adding collection: \n{response.meta.error_description}")
            self._data.__dict__["id"] = response.data

        # for a collection that exists on the database
        else:
            response = client.add_collection(self._data.dict(), overwrite=True, full_return=True)
            if response.meta.success is False:
                raise KeyError(f"Error updating collection: \n{response.meta.error_description}")

        return self._data.id

    # @abc.abstractmethod
    def compute(
        self, spec_name: str, subset: Set[str] = None, tag: Optional[str] = None, priority: Optional[str] = None
    ) -> int:
        pass

    def status(
        self,
        spec_names: Optional[Union[str, List[str]]] = None,
        full: bool = False,
        as_list: bool = False,
        as_df: bool = False,
        status: Optional[Union[str, List[str]]] = None,
    ) -> Union[None, List]:
        """Print or return a status report for all existing compute specifications.

        Parameters
        ----------
        spec_names : Optional[Union[str, List[str]]]
            If given, only yield status of the listed compute specifications.
        full : bool, optional
            If True, expand to give status per entry.
        as_list: bool, optional
            Return output as a list instead of printing.
        as_df : bool, optional
            Return output as a `pandas` DataFrame instead of printing.
        status : Optional[Union[str, List[str]]], optional
            If not None, only returns results that match the provided statuses.

        Returns
        -------
        Union[None, List]
            Prints output as table to screen; if `aslist=True`,
            returns list of output content instead.

        """
        # TODO: consider instead creating a `status` REST API endpoint
        # no real need for us to populate data objects to get this
        from tabulate import tabulate

        # preprocess inputs
        if spec_names is None:
            spec_names = self.spec_names
        elif isinstance(spec_names, str):
            spec_names = [spec_names]

        if isinstance(status, str):
            status = [status.lower()]
        elif isinstance(status, list):
            status = [s.lower() for s in status]

        records = self._get_records_for_spec(spec_names)

        def get_status(record):
            if isinstance(record, dict):
                if hasattr(record["status"], "value"):
                    return record["status"].value
                else:
                    return record["status"]
            else:
                return None

        status_data = pd.DataFrame(records).applymap(get_status)

        # apply filters
        if status is not None:
            status_data = status_data[status_data.applymap(lambda x: x in status).any(axis=0)]

        # apply transformations
        if full:
            output = status_data
        else:
            output = status_data.apply(lambda x: x.value_counts())
            output.index.name = "status"

        # give representation
        if not (as_list or as_df):
            print(tabulate(output.reset_index().to_dict("records"), headers="keys"))
        elif as_list:
            return output.reset_index().to_dict("records")
        elif as_df:
            return output

    ## miscellaneous

    @staticmethod
    def _add_molecules_by_dict(client, molecules):

        flat_map_keys = []
        flat_map_mols = []
        for k, v in molecules.items():
            flat_map_keys.append(k)
            flat_map_mols.append(v)

        CHUNK_SIZE = client.query_limit
        mol_ret = []
        for i in range(0, len(flat_map_mols), CHUNK_SIZE):
            mol_ret.extend(client.add_molecules(flat_map_mols[i : i + CHUNK_SIZE]))

        return {k: v for k, v in zip(flat_map_keys, mol_ret)}


class BaseProcedureDataset(Collection):
    def __init__(self, name: str, client: "PortalClient" = None, **kwargs):
        """Initialize a ProcedureDataset Collection.

        Parameters
        ----------
        name : str
            The name of the Collection object; used to reference the collection on the server.
        client : PortalClient
            A PortalClient connected to a server.
        **kwargs : Dict[str, Any]
            Additional keywords passed to the Collection and the initial data constructor.
            It is up to Collection subclasses to make use of that data.
        """

        # NOTE: ProcedureDatasets require a client, but generally Collections don't
        # can we establish a clear reason? If not, then we can eliminate this variation

        if client is None:
            raise KeyError("{self.__class__.__name__} must initialize with a PortalClient.")

        super().__init__(name, client=client, **kwargs)

    @abc.abstractmethod
    def _internal_compute_add(self, spec: Any, entry: Any, tag: str, priority: str) -> "ObjectId":
        pass

    def _pre_sync_prep(self, client: "PortalClient") -> None:
        pass

    def _get_procedure_ids(self, spec: str, sieve: Optional[List[str]] = None) -> Dict[str, "ObjectId"]:
        """Get a mapping of record names to its object ID in the database.

        Parameters
        ----------
        spec : str
            The specification to get mapping for.
        sieve : Optional[List[str]], optional
            List of record names to restrict the mapping to.

        Returns
        -------
        Dict[str, ObjectId]
            A dictionary of identifier to id mappings.

        """

        spec = self.get_spec(spec)

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

    def compute(
        self, specification: str, subset: Set[str] = None, tag: Optional[str] = None, priority: Optional[str] = None
    ) -> int:
        """Computes a specification for all entries in the dataset.

        Parameters
        ----------
        specification : str
            The specification name.
        subset : Set[str], optional
            Computes only a subset of the dataset.
        tag : Optional[str], optional
            The queue tag to use when submitting compute requests.
        priority : Optional[str], optional
            The priority of the jobs low, medium, or high.

        Returns
        -------
        int
            The number of submitted computations
        """

        # TODO: review and refactor this method
        # try to migrate to `Collection` for full consistency

        specification = specification.lower()
        spec = self.get_spec(specification)
        if subset:
            subset = set(subset)

        submitted = 0
        for entry in self._data.records.values():
            if (subset is not None) and (entry.name not in subset):
                continue

            if spec.name in entry.object_map:
                continue

            entry.object_map[spec.name] = self._internal_compute_add(spec, entry, tag, priority)
            submitted += 1

        self._data.history.add(specification)

        # Nothing to save
        if submitted:
            self.sync()

        return submitted

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
        mapper = self._get_procedure_ids(specification)
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
