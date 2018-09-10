"""
Mongo QCDB Abstract basic Collection class

Helper
"""

import abc
import json
import copy
import pandas as pd

from .. import FractalClient


class Collection(abc.ABC):

    def __init__(self, name, client=None, **kwargs):
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

        # Client and mongod objects
        self.client = client

        # Blank data object
        class_name = self.__class__.__name__.lower()
        self.data = {
            "name": name,
            "collection": class_name,
            "collection_index": (class_name, name),
            "provenance": {},
            **self._init_collection_data(kwargs)
        }

        if self.client is not None:

            if not isinstance(client, FractalClient):
                raise TypeError("Storage: client argument of unrecognized type '{}'".format(type(client)))

            tmp_data = self.client.get_collections([self.data["collection_index"]])
            if len(tmp_data) == 0:
                print("Warning! {} `{}: {}` not found, creating blank database.".format(
                    class_name, *self.data["collection_index"]))
            else:
                # Augment data with extra fields which may have come from the Collection itself
                self.data = {**self.data, **tmp_data[0]}

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

    def _pre_save_prep(self, client):
        """
        Additional actions to take before saving, done as the last step before data is written.

        This is not a required implementation, and as such does nothing at the moment

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
                raise AttributeError(
                    "Collection:save: {} does not own a MongoDB "
                    "instance and one was not passed in.".format(class_name))
            client = self.client

        if overwrite and ("id" not in self.data):
            raise KeyError("Attempting to overwrite the {} class on the server, but no ID found.".format(class_name))

        self._pre_save_prep(client)

        # Add the database
        return client.add_collection(self.data, overwrite=overwrite)


