"""Generic Collection class
"""

from typing import Any, Dict

from .collection import Collection
from .collection_utils import register_collection


class Generic(Collection):
    """
    This is a QCA GenericCollection class. This class behaves similarly to a dictionary, but
    can be serialized and saved to a QCFractal server.

    Attributes
    ----------
    client : client.FractalClient
        A FractalClient connected to a server
    """

    class DataModel(Collection.DataModel):
        """
        Internal Data structure base model typed by PyDantic.

        This structure validates input, allows server-side validation and data security,
        and will create the information to pass back and forth between server and client.
        """

        data: Dict[str, Any] = {}

        class Config:
            extra = "forbid"

    def _pre_save_prep(self, client):
        pass

    def __setitem__(self, key, item):
        self.data.data[key] = item

    def __getitem__(self, key):
        return self.data.data[key]

    def get_data(self, copy: bool = True):
        """Returns a copy of the underlying data object.

        Parameters
        ----------
        copy : bool, optional
            Whether to copy the object or not

        Returns
        -------
        DataModel
            The underlying DataModel
        """
        if copy:
            return self.data.copy(deep=True)
        else:
            return self.data


register_collection(Generic)
