import abc
import copy
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from ...interface.models import ProtoModel


class Record(abc.ABC):
    class _DataModel(ProtoModel):
        # TODO: populate me with structural fields of a basic record
        pass

    def __init__(self, **kwargs: Any):
        """

        Parameters
        ----------
        **kwargs : Dict[str, Any]
            Additional keywords passed to the Record and the initial data constructor.
        """
        # Create the data model
        self._data = self._DataModel(**kwargs)

    def __repr__(self):
        fields = [f"{key}={value}" for key, value in self.__repr_args__()]
        return f"{self.__class__.__name__}({', '.join(fields)})"

    def __repr_args__(self):

        return [("id", f"{self.id}"), ("status", f"{self.status}")]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Record":
        """Creates a new Record instance from a dict representation.

        Allows roundtrips from `Collection.to_dict`.

        Parameters
        ----------
        data : Dict[str, Any]
            A dict to create a new Record instance from.

        Returns
        -------
        Record
            A Record instance.
        """
        class_name = cls.__name__.lower()

        # Check we are building the correct object
        record_type = cls._type
        if "procedure" not in data:
            raise KeyError("Attempted to create Record from data, but no `procedure` field found.")

        if data["procedure"].lower() != record_type:
            raise KeyError(
                "Attempted to create Record from data with class {}, but found record type of {}.".format(
                    class_name, data["procedure"].lower()
                )
            )

        # Allow PyDantic to handle type validation
        ret = cls(**data)
        return ret

    @classmethod
    def from_json(cls, jsondata: Optional[str] = None, filename: Optional[str] = None) -> "Record":
        """Creates a new Record instance from a JSON string.

        Allows roundtrips from `Record.to_json`.
        One of `jsondata` or `filename` must be provided.

        Parameters
        ----------
        jsondata : str, Optional, Default: None
            The JSON string to create a new Record instance from.
        filename : str, Optional, Default: None
            The filename to read JSON data from.

        Returns
        -------
        Record
            A Record instance.
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

        return cls.from_dict(data)

    def to_dict(self) -> dict:
        """Returns a copy of the current Record data as a Python dict.

        Returns
        -------
        ret : dict
            A Python dict representation of the Record data.
        """
        datadict = self._data.dict()
        return copy.deepcopy(datadict)

    def to_json(self, filename: Optional[str] = None) -> str:
        """Returns the current Record data as JSON.

        If a filename is provided, dumps JSON to file.
        Otherwise returns data as a JSON string.

        Parameters
        ----------
        filename : str, Optional, Default: None
            The filename to write JSON data to.

        Returns
        -------
        ret : dict
            If `filename=None`, a JSON representation of the Record.
            Otherwise `None`.
        """
        jsondata = self._data.json()
        if filename is not None:
            with open(filename, "w") as open_file:
                open_file.write(jsondata)
        else:
            return jsondata

    @property
    def status(self):
        """Status of the calculation corresponding to this record."""
        pass

    @property
    def id(self):
        return self._data.id
