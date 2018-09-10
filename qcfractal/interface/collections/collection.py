"""
Mongo QCDB Abstract basic Collection class

Helper
"""

import abc
import json
import copy

from .. import FractalClient


class Collection(abc.ABC):

    __base_fields = {"name", "collection", "provenance"}

    def __init__(self, name, **kwargs):
        """
        Initializer for the Collections objects. If no Portal is supplied or the Collection name
        is not present on the server that the Portal is connected to a blank Collection will be
        created.

        Parameters
        ----------
        name : str
            The name of the Collection object as ID'ed on the storage backend@
        client : client.FractalClient, optional
            A Portal client to connect to a server
        **kwargs
            Additional keywords which are passed to the Collection and the initial data constructor
            It is up to the individual implementations of the Collection to do things with that data
        """

        self.client = kwargs.pop("client", None)

        # Init from raw json blob, ignore everything else
        if kwargs.get("json_data", False):
            self.data = kwargs["json_data"]

        # Base init
        else:
            class_name = self.__class__.__name__.lower()
            self.data = {
                "name": name.lower(),
                "collection": class_name,
                "provenance": {},
                **self._init_collection_data(kwargs)
            }

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
        class_name = cls.__name__.lower()
        tmp_data = client.get_collections([(class_name, name.lower())])
        if len(tmp_data) == 0:
            raise KeyError("Warning! `{}: {}` not found.".format(class_name, name))

        return cls.from_json(tmp_data[0], client=client)

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

        # Ensure all required fields are found.
        req_fields = cls.__base_fields | getattr(cls, "_" + cls.__name__ + "__required_fields")
        if len(req_fields - data.keys()):
            missing = req_fields - data.keys()
            raise KeyError("For class {} the following fields are missing {}.".format(class_name, missing))

        # Build and return object
        ret = cls(data["name"], json_data=data, client=client)

        return ret

    @abc.abstractmethod
    def _init_collection_data(self, additional_data_dict):
        """
        Additional data defined by the Collection

        This is in addition to the default data. If there is no additional data, simply return and empty dict

        Parameters
        ----------
        additional_data_dict : dict
            Additional data which the individual implementation can work with

        Returns
        -------
        collection_data : dict
            Additional data to be added as part of the collection.
            If the collection does not have additional data, return this as an empty dict
        """
        raise NotImplementedError()

    def to_json(self, filename=None):
        """
        If a filename is provided, dumps the file to disk. Otherwise returns a copy of the current data.

        Parameters
        ----------
        filename : str, Optional, Default: None
            The filename to drop the data to.

        Returns
        -------
        ret : dict
            A JSON representation of the Database
        """
        if filename is not None:
            with open(filename, 'w') as open_file:
                json.dump(self.data, open_file)

        else:
            return copy.deepcopy(self.data)

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
    def save(self, client=None, overwrite=False):
        """Uploads the overall structure of the Database (reactions, names, new molecules, etc)
        to the server.

        Parameters
        ----------
        client : None, optional
            A Portal object to the server to upload to
        overwrite : bool, optional
            Overwrite the data in the server on not

        """
        class_name = self.__class__.__name__.lower()
        if self.data["name"] == "":
            raise AttributeError("Collection:save: {} must have a name!".format(class_name))

        if client is None:
            if self.client is None:
                raise AttributeError("Collection:save: {} does not own a MongoDB "
                                     "instance and one was not passed in.".format(class_name))
            client = self.client

        if overwrite and ("id" not in self.data):
            raise KeyError("Attempting to overwrite the {} class on the server, but no ID found.".format(class_name))

        self._pre_save_prep(client)

        # Add the database
        return client.add_collection(self.data, overwrite=overwrite)
