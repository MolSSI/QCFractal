"""
Mongo QCDB Abstract basic Collection class

Helper
"""

import abc
import copy
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

import pandas as pd

from ..models import ProtoModel

if TYPE_CHECKING:  # pragma: no cover
    from .. import FractalClient
    from ..models import ObjectId


class Collection(abc.ABC):
    def __init__(self, name: str, client: Optional["FractalClient"] = None, **kwargs: Any):
        """
        Initializer for the Collection objects. If no Portal is supplied or the Collection name
        is not present on the server that the Portal is connected to a blank Collection will be
        created.

        Parameters
        ----------
        name : str
            The name of the Collection object as ID'ed on the storage backend.
        client : FractalClient, optional
            A FractalClient connected to a server
        **kwargs : Dict[str, Any]
            Additional keywords which are passed to the Collection and the initial data constructor
            It is up to the individual implementations of the Collection to do things with that data
        """

        self.client = client
        if (self.client is not None) and not (self.client.__class__.__name__ == "FractalClient"):
            raise TypeError("Expected FractalClient as `client` kwarg, found {}.".format(type(self.client)))

        if "collection" not in kwargs:
            kwargs["collection"] = self.__class__.__name__.lower()

        kwargs["name"] = name

        # Create the data model
        self.data = self.DataModel(**kwargs)

    class DataModel(ProtoModel):
        """
        Internal Data structure base model typed by PyDantic

        This structure validates input, allows server-side validation and data security,
        and will create the information to pass back and forth between server and client

        Subclasses of Collection can extend this class internally to change the set of
        additional data defined by the Collection
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

        view_url_hdf5: Optional[str] = None
        view_url_plaintext: Optional[str] = None
        view_metadata: Optional[Dict[str, str]] = None
        view_available: bool = False

        metadata: Dict[str, Any] = {}

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
        if self.client:
            client = self.client.address

        class_name = self.__class__.__name__
        ret = "{}(".format(class_name)
        ret += "name=`{}`, ".format(self.data.name)
        ret += "id='{}', ".format(self.data.id)
        ret += "client='{}') ".format(client)

        return ret

    def __repr__(self) -> str:
        return f"<{self}>"

    def _check_client(self):
        if self.client is None:
            raise AttributeError("This method requires a FractalClient and no client was set")

    @property
    def name(self) -> str:
        return self.data.name

    @classmethod
    def from_server(cls, client: "FractalClient", name: str) -> "Collection":
        """Creates a new class from a server

        Parameters
        ----------
        client : FractalClient
            A FractalClient connected to a server
        name : str
            The name of the collection to pull from.

        Returns
        -------
        Collection
            A constructed collection.

        """

        if not (client.__class__.__name__ == "FractalClient"):
            raise TypeError("Expected a FractalClient as first argument, found {}.".format(type(client)))

        class_name = cls.__name__.lower()
        tmp_data = client.get_collection(class_name, name, full_return=True)
        if tmp_data.meta.n_found == 0:
            raise KeyError("Warning! `{}: {}` not found.".format(class_name, name))

        return cls.from_json(tmp_data.data[0], client=client)

    @classmethod
    def from_json(cls, data: Dict[str, Any], client: "FractalClient" = None) -> "Collection":
        """Creates a new class from a JSON blob

        Parameters
        ----------
        data : Dict[str, Any]
            The JSON blob to create a new class from.
        client : FractalClient, optional
            A FractalClient connected to a server

        Returns
        -------
        Collection
            A constructed collection.

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

    def to_json(self, filename: Optional[str] = None):
        """
        If a filename is provided, dumps the file to disk. Otherwise returns a copy of the current data.

        Parameters
        ----------
        filename : str, Optional, Default: None
            The filename to drop the data to.

        Returns
        -------
        ret : dict
            A JSON representation of the Collection
        """
        data = self.data.dict()
        if filename is not None:
            with open(filename, "w") as open_file:
                json.dump(data, open_file)
        else:
            return copy.deepcopy(data)

    @abc.abstractmethod
    def _pre_save_prep(self, client: "FractalClient"):
        """
        Additional actions to take before saving, done as the last step before data is written.

        This does not return anything but can prep the `self.data` field before storing it.

        Has access to the `client` in case its needed to do pre-conditioning.

        Parameters
        ----------
        client : FractalClient
            A FractalClient connected to a server used for storage access
        """

    # Setters
    def save(self, client: Optional["FractalClient"] = None) -> "ObjectId":
        """Uploads the overall structure of the Collection (indices, options, new molecules, etc)
        to the server.

        Parameters
        ----------
        client : FractalClient, optional
            A FractalClient connected to a server to upload to

        Returns
        -------
        ObjectId
            The ObjectId of the saved collection.

        """
        class_name = self.__class__.__name__.lower()
        if self.data.name == "":
            raise AttributeError("Collection:save: {} must have a name!".format(class_name))

        if client is None:
            self._check_client()
            client = self.client

        self._pre_save_prep(client)

        # Add the database
        if self.data.id == self.data.__fields__["id"].default:
            response = client.add_collection(self.data.dict(), overwrite=False, full_return=True)
            if response.meta.success is False:
                raise KeyError(f"Error adding collection: \n{response.meta.error_description}")
            self.data.__dict__["id"] = response.data
        else:
            response = client.add_collection(self.data.dict(), overwrite=True, full_return=True)
            if response.meta.success is False:
                raise KeyError(f"Error updating collection: \n{response.meta.error_description}")

        return self.data.id

    ### General helpers

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
    def __init__(self, name: str, client: "FractalClient" = None, **kwargs):
        if client is None:
            raise KeyError("{self.__class__.__name__} must initialize with a client.")

        super().__init__(name, client=client, **kwargs)

        self.df = pd.DataFrame(index=self._get_index())

    class DataModel(Collection.DataModel):

        records: Dict[str, Any] = {}
        history: Set[str] = set()
        specs: Dict[str, Any] = {}

        class Config(Collection.DataModel.Config):
            pass

    @abc.abstractmethod
    def _internal_compute_add(self, spec: Any, entry: Any, tag: str, priority: str) -> "ObjectId":
        pass

    def _pre_save_prep(self, client: "FractalClient") -> None:
        pass

    def _get_index(self):

        return [x.name for x in self.data.records.values()]

    def _add_specification(self, name: str, spec: Any, overwrite=False) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the specification
        spec : Any
            The specification object
        overwrite : bool, optional
            Overwrite existing specification names

        """

        lname = name.lower()
        if (lname in self.data.specs) and (not overwrite):
            raise KeyError(f"{self.__class__.__name__} '{name}' already present, use `overwrite=True` to replace.")

        self.data.specs[lname] = spec
        self.save()

    def _get_procedure_ids(self, spec: str, sieve: Optional[List[str]] = None) -> Dict[str, "ObjectId"]:
        """Aquires the

        Parameters
        ----------
        spec : str
            The specification to get the map of
        sieve : Optional[List[str]], optional
            A
            Description

        Returns
        -------
        Dict[str, ObjectId]
            A dictionary of identifier to id mappings.

        """

        spec = self.get_specification(spec)

        mapper = {}
        for rec in self.data.records.values():
            if sieve and rec.name not in sieve:
                continue

            try:
                td_id = rec.object_map[spec.name]
                mapper[rec.name] = td_id
            except KeyError:
                pass

        return mapper

    def get_specification(self, name: str) -> Any:
        """
        Parameters
        ----------
        name : str
            The name of the specification

        Returns
        -------
        Specification
            The requested specification.

        """
        try:
            return self.data.specs[name.lower()].copy()
        except KeyError:
            raise KeyError(f"Specification '{name}' not found.")

    def list_specifications(self, description=True) -> Union[List[str], pd.DataFrame]:
        """Lists all available specifications

        Parameters
        ----------
        description : bool, optional
            If True returns a DataFrame with
            Description

        Returns
        -------
        Union[List[str], 'DataFrame']
            A list of known specification names.

        """
        if description:
            data = [(x.name, x.description) for x in self.data.specs.values()]
            return pd.DataFrame(data, columns=["Name", "Description"]).set_index("Name")
        else:
            return [x.name for x in self.data.specs.values()]

    def _check_entry_exists(self, name):
        """
        Checks if an entry exists or not.
        """

        if name.lower() in self.data.records:
            raise KeyError(f"Record {name} already in the dataset.")

    def _add_entry(self, name, record, save):
        """
        Adds an entry to the records
        """

        self._check_entry_exists(name)
        self.data.records[name.lower()] = record
        if save:
            self.save()

    def get_entry(self, name: str) -> Any:
        """Obtains a record from the Dataset

        Parameters
        ----------
        name : str
            The record name to pull from.

        Returns
        -------
        Record
            The requested record
        """
        try:
            return self.data.records[name.lower()]
        except KeyError:
            raise KeyError(f"Could not find entry name '{name}' in the dataset.")

    def get_record(self, name: str, specification: str) -> Any:
        """Pulls an individual computational record of the requested name and column.

        Parameters
        ----------
        name : str
            The index name to pull the record of.
        specification : str
            The name of specification to pull the record of.

        Returns
        -------
        Any
            The requested Record

        """
        spec = self.get_specification(specification)
        rec_id = self.get_entry(name).object_map.get(spec.name, None)

        if rec_id is None:
            raise KeyError(f"Could not find a record for ({name}: {specification}).")

        return self.client.query_procedures(id=rec_id)[0]

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

        specification = specification.lower()
        spec = self.get_specification(specification)
        if subset:
            subset = set(subset)

        submitted = 0
        for entry in self.data.records.values():
            if (subset is not None) and (entry.name not in subset):
                continue

            if spec.name in entry.object_map:
                continue

            entry.object_map[spec.name] = self._internal_compute_add(spec, entry, tag, priority)
            submitted += 1

        self.data.history.add(specification)

        # Nothing to save
        if submitted:
            self.save()

        return submitted

    def query(self, specification: str, force: bool = False) -> pd.Series:
        """Queries a given specification from the server

        Parameters
        ----------
        specification : str
            The specification name to query
        force : bool, optional
            Force a fresh query if the specification already exists.

        Returns
        -------
        pd.Series
            Records collected from the server
        """
        # Try to get the specification, will throw if not found.
        spec = self.get_specification(specification)

        if not force and (spec.name in self.df):
            return spec.name

        mapper = self._get_procedure_ids(spec.name)
        query_ids = list(mapper.values())

        # Chunk up the queries
        procedures: List[Dict[str, Any]] = []
        for i in range(0, len(query_ids), self.client.query_limit):
            chunk_ids = query_ids[i : i + self.client.query_limit]
            procedures.extend(self.client.query_procedures(id=chunk_ids))

        proc_lookup = {x.id: x for x in procedures}

        data = []
        for name, oid in mapper.items():
            try:
                data.append([name, proc_lookup[oid]])
            except KeyError:
                data.append([name, None])

        df = pd.DataFrame(data, columns=["index", spec.name])
        df.set_index("index", inplace=True)

        self.df[spec.name] = df[spec.name]

        return df[spec.name]

    def status(
        self,
        specs: Union[str, List[str]] = None,
        collapse: bool = True,
        status: Optional[str] = None,
        detail: bool = False,
    ) -> pd.DataFrame:
        """Returns the status of all current specifications.

        Parameters
        ----------
        collapse : bool, optional
            Collapse the status into summaries per specification or not.
        status : Optional[str], optional
            If not None, only returns results that match the provided status.
        detail : bool, optional
            Shows a detailed description of the current status of incomplete jobs.

        Returns
        -------
        DataFrame
            A DataFrame of all known statuses

        """

        # Simple no detail case
        if detail is False:
            # detail = False can handle multiple specifications
            # If specs is None, then use all (via list_specifications)
            if isinstance(specs, str):
                specs = [specs]
            elif specs is None:
                specs = self.list_specifications(description=False)

            # Query all of the specs and make sure they are valid
            # Specs may not be loaded to self.df yet. This can be accomplished
            #     with self.query, which stores the info in self.df
            for spec in specs:
                self.query(spec)

            def get_status(item):
                try:
                    return item.status.value
                except AttributeError:
                    return None

            # apply status by column then by row
            df = self.df[specs].apply(lambda col: col.apply(get_status))

            if status:
                df = df[(df == status.upper()).all(axis=1)]

            if collapse:
                return df.apply(lambda x: x.value_counts())
            else:
                return df

        if status not in [None, "INCOMPLETE"]:
            raise KeyError("Detailed status is only available for incomplete procedures.")

        # Can only do detailed status for a single spec
        # If specs is a string, ok. If it is a list, then it should have length = 1
        if not (isinstance(specs, str) or len(specs) == 1):
            raise KeyError("Detailed status is only available for a single specification at a time.")

        # If specs is a list (of length = 1, checked above), then make it a string
        # (_get_procedure_ids expects a string)
        if not isinstance(specs, str):
            specs = specs[0]

        mapper = self._get_procedure_ids(specs)
        reverse_map = {v: k for k, v in mapper.items()}
        procedures = self.client.query_procedures(id=list(mapper.values()))

        data = []

        for proc in procedures:
            if proc.status == "COMPLETE":
                continue

            try:
                blob = proc.detailed_status()
            except:
                raise AttributeError("Detailed statuses are not available for this dataset type.")

            blob["Name"] = reverse_map[proc.id]
            data.append(blob)

        df = pd.DataFrame(data)
        df.rename(columns={x: x.replace("_", " ").title() for x in df.columns}, inplace=True)
        if df.shape[0]:
            df = df.set_index("Name")

        return df
