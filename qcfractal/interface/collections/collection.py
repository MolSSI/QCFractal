"""
Mongo QCDB Abstract basic Collection class

Helper
"""

import abc
import copy
import json
from typing import Dict, Optional

from pydantic import BaseModel


class Collection(abc.ABC):
    def __init__(self, name: str, **kwargs):
        """
        Initializer for the Collections objects. If no Portal is supplied or the Collection name
        is not present on the server that the Portal is connected to a blank Collection will be
        created.

        Parameters
        ----------
        name : str
            The name of the Collection object as ID'ed on the storage backend.
        client : client.FractalClient, optional
            A Portal client to connect to a server
        **kwargs
            Additional keywords which are passed to the Collection and the initial data constructor
            It is up to the individual implementations of the Collection to do things with that data
        """

        self.client = kwargs.pop("client", None)
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

    @classmethod
    def from_server(cls, client, name):
        """Creates a new class from a server

        Parameters
        ----------
        client : client.FractalClient
            A Portal client to connected to a server
        name : str
            The name of the collection to pull from.

        Returns
        -------
        Collection
            A ODM of the data.
        """

        if not (client.__class__.__name__ == "FractalClient"):
            raise TypeError("Expected a FractalClient as first argument, found {}.".format(type(client)))

        class_name = cls.__name__.lower()
        tmp_data = client.get_collection(class_name, name, full_return=True)
        if tmp_data.meta.n_found == 0:
            raise KeyError("Warning! `{}: {}` not found.".format(class_name, name))

        return cls.from_json(tmp_data.data[0], client=client)

    @classmethod
    def from_json(cls, data, client=None):
        """Creates a new class from a JSON blob

        Parameters
        ----------
        data : dict
            The JSON blob to create a new class from.
        client : client.FractalClient
            A Portal client to connected to a server

        Returns
        -------
        Collection
            A ODM of the data.

        """
        # Check we are building the correct object
        class_name = cls.__name__.lower()
        if "collection" not in data:
            raise KeyError("Attempted to create Collection from JSON, but no `collection` field found.")

        if data["collection"] != class_name:
            raise KeyError("Attempted to create Collection from JSON with class {}, but found collection type of {}.".
                           format(class_name, data["collection"]))

        # Attempt to build class
        # First make sure external source provides ALL keys, including "optional" ones
        # Assumption here is that incoming source should have all this from a previous instance
        # and will not have missing fields. Also enforces that no default values are *assumed* from
        # external input.
        # PyDantic can only enforce required on init entries, but lets everything else have defaults,
        # this check asserts all fields are present, even if their default values are chosen
        # Also provides consistency check in case defaults ever change in the future.
        # This check could be removed though without any code failures
        req_fields = cls.DataModel.__fields__.keys()
        missing = req_fields - data.keys()
        if len(missing):
            raise KeyError("For class {} the following fields are missing {}.".format(class_name, missing))
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
    def _pre_save_prep(self, client):
        """
        Additional actions to take before saving, done as the last step before data is written.

        This does not return anything but can prep the `self.data` field before storing it.

        Has access to the `client` in case its needed to do pre-conditioning.

        Parameters
        ----------
        client : FractalClient
            Client to use for storage access
        """
        pass

    # Setters
    def save(self, client=None, overwrite: bool=False):
        """Uploads the overall structure of the Collection (indices, options, new molecules, etc)
        to the server.

        Parameters
        ----------
        client : None, optional
            A Portal object to the server to upload to
        overwrite : bool, optional
            Overwrite the data in the server on not

        """
        class_name = self.__class__.__name__.lower()
        if self.data.name == "":
            raise AttributeError("Collection:save: {} must have a name!".format(class_name))

        if client is None:
            if self.client is None:
                raise AttributeError("Collection:save: {} does not own a Storage Database "
                                     "instance and one was not passed in.".format(class_name))
            client = self.client

        if overwrite and (self.data.id == self.data.fields['id'].default):
            raise KeyError("Attempting to overwrite the {} class on the server, but no ID found.".format(class_name))

        self._pre_save_prep(client)

        # Add the database
        return client.add_collection(self.data.dict(), overwrite=overwrite)
