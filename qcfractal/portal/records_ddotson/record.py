import abc
import copy
import datetime
import json
from enum import Enum
from typing import Any, Dict, Optional, Set

import qcelemental as qcel
from pydantic import Field, validator
from qcelemental.models.results import AtomicResultProtocols

from ..outputstore import OutputStore
from ..records.models import RecordStatusEnum

# from ...interface.models import ObjectId, ProtoModel
ObjectId = int
from qcelemental.models import ProtoModel


class DriverEnum(str, Enum):
    """
    The type of calculation that is being performed (e.g., energy, gradient, Hessian, ...).
    """

    energy = "energy"
    gradient = "gradient"
    hessian = "hessian"
    properties = "properties"


class QCSpecification(ProtoModel):
    """
    The quantum chemistry metadata specification for individual computations such as energy, gradient, and Hessians.
    """

    driver: DriverEnum = Field(..., description=str(DriverEnum.__doc__))
    method: str = Field(..., description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...).")
    basis: Optional[str] = Field(
        None,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
        "methods without basis sets.",
    )
    keywords: Optional[ObjectId] = Field(
        None,
        description="The Id of the :class:`KeywordSet` registered in the database to run this calculation with. This "
        "Id must exist in the database.",
    )
    protocols: Optional[AtomicResultProtocols] = Field(
        AtomicResultProtocols(), description=str(AtomicResultProtocols.__base_doc__)
    )
    program: str = Field(
        ...,
        description="The quantum chemistry program to evaluate the computation with. Not all quantum chemistry programs"
        " support all combinations of driver/method/basis.",
    )

    def dict(self, *args, **kwargs):
        ret = super().dict(*args, **kwargs)

        # Maintain hash compatability
        if len(ret["protocols"]) == 0:
            ret.pop("protocols", None)

        return ret

    @validator("basis")
    def _check_basis(cls, v):
        return prepare_basis(v)

    @validator("program")
    def _check_program(cls, v):
        return v.lower()

    @validator("method")
    def _check_method(cls, v):
        return v.lower()


class Record(abc.ABC):
    _type = "record"
    _SpecModel = QCSpecification

    class _DataModel(ProtoModel):

        # Classdata
        # NOTE: do we want to change how these work?
        _hash_indices: Set[str]

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
        stdout_obj: Optional[OutputStore] = Field(
            None,
            description="The full stdout data stored in the database which was used to generate this record from the "
            "various programs which were called in the process.",
        )
        stderr: Optional[ObjectId] = Field(
            None,
            description="The Id of the stderr data stored in the database which was used to generate this record from the "
            "various programs which were called in the process.",
        )
        stderr_obj: Optional[OutputStore] = Field(
            None,
            description="The full stderr data stored in the database which was used to generate this record from the "
            "various programs which were called in the process.",
        )
        error: Optional[ObjectId] = Field(
            None,
            description="The Id of the error data stored in the database in the event that an error was generated in the "
            "process of carrying out the process this record targets. If no errors were raised, this field "
            "will be empty.",
        )
        error_obj: Optional[OutputStore] = Field(
            None,
            description="The full error output stored in the database which was used to generate this record from the "
            "various programs which were called in the process.",
        )

        # Compute status
        manager_name: Optional[str] = Field(None, description="Name of the Queue Manager which generated this record.")
        status: RecordStatusEnum = Field(..., description=str(RecordStatusEnum.__doc__))
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

    def __init__(self, client: Optional["PortalClient"] = None, **kwargs: Any):
        """
        Parameters
        ----------
        client : PortalClient, optional
            A PortalClient connected to a server.
        **kwargs : Dict[str, Any]
            Additional keywords passed to the Record and the initial data constructor.
        """
        self._client = client

        # Create the data model
        self._data = self._DataModel(**kwargs)

    def __repr__(self):
        fields = [f"{key}={value}" for key, value in self.__repr_args__()]
        return f"{self.__class__.__name__}({', '.join(fields)})"

    def __repr_args__(self):

        return [("id", f"{self.id}"), ("status", f"{self.status}")]

    @classmethod
    def from_dict(cls, data: Dict[str, Any], client: Optional["PortalClient"] = None) -> "Record":
        """Creates a new Record instance from a dict representation.

        Allows roundtrips from `Collection.to_dict`.

        Parameters
        ----------
        data : Dict[str, Any]
            A dict to create a new Record instance from.
        client : PortalClient, optional
            A PortalClient connected to a server.

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
        ret = cls(client=client, **data)
        return ret

    @classmethod
    def from_json(
        cls, *, jsondata: Optional[str] = None, filename: Optional[str] = None, client: Optional["PortalClient"] = None
    ) -> "Record":
        """Creates a new Record instance from a JSON string.

        Allows roundtrips from `Record.to_json`.
        One of `jsondata` or `filename` must be provided.

        Parameters
        ----------
        jsondata : str, Optional, Default: None
            The JSON string to create a new Record instance from.
        filename : str, Optional, Default: None
            The filename to read JSON data from.
        client : PortalClient, optional
            A PortalClient connected to a server.

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

        return cls.from_dict(data, client=client)

    def to_dict(self) -> dict:
        """Returns a copy of the current Record data as a Python dict.

        Returns
        -------
        ret : dict
            A Python dict representation of the Record data.
        """
        datadict = self._data.dict()
        return copy.deepcopy(datadict)

    # alias for to_json, for duck-typing parity with pydantic
    dict = to_dict

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

    # alias for to_json, for duck-typing parity with pydantic
    json = to_json

    @property
    def status(self):
        """Status of the calculation corresponding to this record."""
        return self._data.status.value if self._data.status else None

    @property
    def id(self):
        return self._data.id

    @property
    def spec(self):
        """Includes keywords."""
        # example
        # TODO: need to change `_DataModel` above to accommodate usage like this
        self._SpecModel(**self._data.spec)

    @property
    def task(self):
        # will need to handle case of task key being present or not
        pass

    def _outputstore_get(self, field_name):

        oid = self._data.__dict__[field_name]
        if oid is None:
            return None

        print("{} : '{}' || {}".format(self.__class__.__name__, self.id, self._client.address))
        result = self._client._get_outputs(oid)

        if field_name == "error":
            return result.as_json
        else:
            return result.as_string

    @property
    def stdout(self):
        """The STDOUT contents for this record, if it exists."""
        if self._data.stdout_obj is not None:
            return self._data.stdout_obj
        else:
            return self._outputstore_get("stdout")

    @property
    def stderr(self):
        """The STDERR contents for this record, if it exists."""
        return self._outputstore_get("stderr")

    @property
    def error(self):
        """The error traceback contents for this record, if it exists."""
        return self._outputstore_get("error")

    @property
    def procedure(self):
        """Everything should be a procedure."""
        pass

    @property
    def created_on(self):
        # this is a datetime
        return self._data.created_on

    @property
    def modified_on(self):
        # this is a datetime
        return self._data.modified_on

    @property
    def provenance(self):
        return self._data.provenance

    @property
    def manager(self):
        # the manager the result is currently being executed on, if currently running
        pass

    @property
    def extras(self):
        # dictionary
        pass

    # @abc.abstractproperty
    def protocols(self):
        # optional configuration items for e.g. storing wavefunction of point calculation
        # not so much how to run calculation, but what to return

        # this will be specific to each procedure type
        pass
