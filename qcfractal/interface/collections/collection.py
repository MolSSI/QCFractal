"""
Mongo QCDB Abstract basic Collection class

Helper
"""

import abc
import copy
import json
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
from pydantic import BaseModel

from ..models import json_encoders


class Collection(abc.ABC):
    def __init__(self, name: str, client: 'FractalClient'=None, **kwargs: Dict[str, Any]):
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

        if 'collection' not in kwargs:
            kwargs['collection'] = self.__class__.__name__.lower()

        kwargs['name'] = name

        # Create the data model
        self.data = self.DataModel(**kwargs)

    class DataModel(BaseModel):
        """
        Internal Data structure base model typed by PyDantic

        This structure validates input, allows server-side validation and data security,
        and will create the information to pass back and forth between server and client

        Subclasses of Collection can extend this class internally to change the set of
        additional data defined by the Collection
        """
        name: str
        collection: str = None
        provenance: Dict[str, str] = {}
        tagline: str = None
        tags: List[str] = []
        id: str = 'local'

        class Config:
            json_encoders = json_encoders
            extra = "forbid"

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

    @property
    def name(self) -> str:
        return self.data.name

    @classmethod
    def from_server(cls, client: 'FractalClient', name: str) -> 'Collection':
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
    def from_json(cls, data: Dict[str, Any], client: 'FractalClient'=None) -> 'Collection':
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
            raise KeyError("Attempted to create Collection from JSON with class {}, but found collection type of {}.".
                           format(class_name, data["collection"]))

        name = data.pop('name')
        # Allow PyDantic to handle type validation
        return cls(name, client=client, **data)

    def to_json(self, filename: Optional[str]=None):
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
            with open(filename, 'w') as open_file:
                json.dump(data, open_file)
        else:
            return copy.deepcopy(data)

    @abc.abstractmethod
    def _pre_save_prep(self, client: 'FractalClient'):
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
    def save(self, client: 'FractalClient'=None) -> 'ObjectId':
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
            if self.client is None:
                raise AttributeError("Collection:save: {} does not own a Storage Database "
                                     "instance and one was not passed in.".format(class_name))
            client = self.client

        self._pre_save_prep(client)

        # Add the database
        if (self.data.id == self.data.fields['id'].default):
            self.data.id = client.add_collection(self.data.dict(), overwrite=False)
        else:
            client.add_collection(self.data.dict(), overwrite=True)

        return self.data.id

### General helpers

    @staticmethod
    def _add_molecules_by_dict(client, molecules):

        flat_map_keys = []
        flat_map_mols = []
        for k, v in molecules.items():
            flat_map_keys.append(k)
            flat_map_mols.append(v)

        mol_ret = client.add_molecules(flat_map_mols)

        return {k: v for k, v in zip(flat_map_keys, mol_ret)}


class BaseProcedureDataset(Collection):
    def __init__(self, name: str, client: 'FractalClient'=None, **kwargs):
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

    def _pre_save_prep(self, client: 'FractalClient') -> None:
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

    def list_specifications(self, description=True) -> Union[List[str], 'DataFrame']:
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

    def _add_entry(self, name: str, record: 'Record') -> None:
        """Adds a record to the dataset using the lowered input name. Saves the dataset after adding a new entry.

        Parameters
        ----------
        name : str
            The name of the record
        record : Record
            The record itself

        """
        lname = name.lower()
        if lname in self.data.records:
            raise KeyError(f"Record {name} already in the dataset.")

        self.data.records[lname] = record
        self.save()

    def get_entry(self, name: str) -> 'Record':
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
        return self.data.records[name.lower()]

    def query(self, specification: str, force: bool=False) -> None:
        """Queries a given specification from the server

        Parameters
        ----------
        specification : str
            The specification name to query
        force : bool, optional
            Force a fresh query if the specification already exists.
        """
        # Try to get the specification, will throw if not found.
        spec = self.get_specification(specification)

        if not force and (spec.name in self.df):
            return spec.name

        query_ids = []
        mapper = {}
        for rec in self.data.records.values():
            try:
                td_id = rec.object_map[spec.name]
                query_ids.append(td_id)
                mapper[rec.name] = td_id
            except KeyError:
                pass

        procedures = self.client.query_procedures(id=query_ids)
        proc_lookup = {x.id: x for x in procedures}

        data = []
        for name, oid in mapper.items():
            data.append([name, proc_lookup[oid]])

        df = pd.DataFrame(data, columns=["index", spec.name])
        df.set_index("index", inplace=True)

        self.df[spec.name] = df[spec.name]

        return spec.name

    def status(self, specs: Union[str, List[str]]=None, collapse: bool=True,
               status: Optional[str]=None) -> 'DataFrame':
        """Returns the status of all current specifications.

        Parameters
        ----------
        collapse : bool, optional
            Collapse the status into summaries per specification or not.
        status : Optional[str], optional
            If not None, only returns results that match the provided status.

        Returns
        -------
        DataFrame
            A DataFrame of all known statuses

        """

        # Specifications
        if isinstance(specs, str):
            specs = [specs]

        # Query all of the specs and make sure they are valid
        if specs is None:
            list_specs = list(self.df.columns)
        else:
            list_specs = []
            for spec in specs:
                list_specs.append(self.query(spec))

        # apply status by column then by row
        df = self.df[list_specs].apply(lambda col: col.apply(lambda entry: entry.status.value))
        if status:
            df = df[(df == status.upper()).all(axis=1)]

        if collapse:
            return df.apply(lambda x: x.value_counts())
        else:
            return df
