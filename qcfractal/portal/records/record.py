import abc
import copy
import json
import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from pydantic import Field, constr, validator
import qcelemental as qcel

from ...interface.models import ObjectId, ProtoModel


class RecordStatusEnum(str, Enum):
    """
    The state of a record object.
    The states which are available are a finite set.
    """

    complete = "COMPLETE"
    incomplete = "INCOMPLETE"
    running = "RUNNING"
    error = "ERROR"


class Record(abc.ABC):
    _type = None
    _SpecModel = None

    class _DataModel(ProtoModel):

        # Classdata
        # NOTE: do we want to change how these work?
        _hash_indices: Set[str]

        # Helper data
        client: Any = Field(None, description="The client object which the records are fetched from.")

        # Base identification
        id: ObjectId = Field(
            None, description="Id of the object on the database. This is assigned automatically by the database."
        )
        hash_index: Optional[str] = Field(
            None, description="Hash of this object used to detect duplication and collisions in the database."
        )
        procedure: str = Field(..., description="Name of the procedure which this Record targets.")
        program: str = Field(
            ...,
            description="The quantum chemistry program used for individual quantum chemistry calculations.",
        )
        version: int = Field(..., description="The version of this record object.")
        protocols: Optional[Dict[str, Any]] = Field(
            None, description="Protocols that change the data stored in top level fields."
        )

        # Extra fields
        extras: Dict[str, Any] = Field({}, description="Extra information to associate with this record.")
        stdout: Optional[ObjectId] = Field(
            None,
            description="The Id of the stdout data stored in the database which was used to generate this record from the "
            "various programs which were called in the process.",
        )
        stderr: Optional[ObjectId] = Field(
            None,
            description="The Id of the stderr data stored in the database which was used to generate this record from the "
            "various programs which were called in the process.",
        )
        error: Optional[ObjectId] = Field(
            None,
            description="The Id of the error data stored in the database in the event that an error was generated in the "
            "process of carrying out the process this record targets. If no errors were raised, this field "
            "will be empty.",
        )

        # Compute status
        manager_name: Optional[str] = Field(None, description="Name of the Queue Manager which generated this record.")
        status: RecordStatusEnum = Field(RecordStatusEnum.incomplete, description=str(RecordStatusEnum.__doc__))
        modified_on: datetime.datetime = Field(
            None, description="Last time the data this record points to was modified."
        )
        created_on: datetime.datetime = Field(
            None, description="Time the data this record points to was first created."
        )

        # Carry-ons
        provenance: Optional[qcel.models.Provenance] = Field(
            None,
            description="Provenance information tied to the creation of this record. This includes things such as every "
            "program which was involved in generating the data for this record.",
        )

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
        return self._data.status

    @property
    def id(self):
        return self._data.id

    @property
    def spec(self):
        """Includes keywords."""
        # example
        self._SpecModel(**self._data["spec"])

    @property
    def task(self):
        # will need to handle case of task key being present or not
        pass

    @property
    def stdout(self):
        pass

    @property
    def stderr(self):
        pass

    @property
    def error(self):
        pass

    @property
    def procedure(self):
        """Everything should be a procedure."""
        pass

    @property
    def created_on(self):
        # this is a datatime
        pass

    @property
    def modified_on(self):
        # this is a datatime
        pass

    def provenance(self):
        pass

    def manager(self):
        # the manager the result is currently being executed on, if currently running
        pass

    def extras(self):
        # dictionary
        pass

    # @abc.abstractproperty
    def protocols(self):
        # optional configuration items for e.g. storing wavefunction of point calculation
        # not so much how to run calculation, but what to return

        # this will be specific to each procedure type
        pass
