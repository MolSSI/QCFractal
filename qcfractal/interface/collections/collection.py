"""
Mongo QCDB Abstract basic Collection class

Helper
"""

import abc
import copy
import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


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
        pass

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