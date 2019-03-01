"""
A model for TorsionDrive
"""

import abc
import datetime
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import qcelemental as qcel
from pydantic import BaseModel, validator, constr

from .common_models import DriverEnum, ObjectId, QCSpecification
from .model_utils import hash_dictionary, prepare_basis, json_encoders, recursive_normalizer

__all__ = ["OptimizationRecord", "ResultRecord", "ProcedureRecord", "OptimizationRecord"]


class RecordStatusEnum(str, Enum):
    complete = "COMPLETE"
    incomplete = "INCOMPLETE"
    running = "RUNNING"
    error = "ERROR"


class Record(BaseModel, abc.ABC):
    """
    Record objects for Results and Procedures tables
    """

    # Base identification
    id: ObjectId = None
    version: int

    # Extra fields
    extras: Dict[str, Any] = {}
    stdout: Optional[ObjectId] = None
    stderr: Optional[ObjectId] = None

    # Compute status
    task_id: ObjectId = None
    status: RecordStatusEnum = "INCOMPLETE"
    modified_on: datetime.datetime = datetime.datetime.utcnow()
    created_on: datetime.datetime = datetime.datetime.utcnow()

    # Carry-ons
    provenance: qcel.models.Provenance = None

    def dict(self, *args, **kwargs):
        kwargs["exclude"] = (kwargs.pop("exclude", None) or set()) | {"client", "cache"}
        kwargs["skip_defaults"] = True
        return super().dict(*args, **kwargs)

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))

    class Config:
        json_encoders = json_encoders
        extra = "forbid"


class ResultRecord(Record):

    # Version data
    version: int = 1

    # Input data
    program: str
    driver: DriverEnum
    method: str
    basis: Optional[str] = None
    molecule: ObjectId
    keywords: Optional[ObjectId] = None


    # Output data
    return_results: Union[float, List[float], Dict[str, Any]] = None
    properties: qcel.models.ResultProperties = None
    error: qcel.models.ComputeError = None

    class Config:
        json_encoders = json_encoders
        extra = "forbid"

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    @validator('basis')
    def check_basis(cls, v):
        return prepare_basis(v)


class ProcedureRecord(Record):

    program: str
    hash_index: Optional[str] = None


class OptimizationRecord(ProcedureRecord):
    """
    A TorsionDrive Input base class
    """

    # Version data
    version: int = 1
    procedure: constr(strip_whitespace=True, regex="optimization") = "optimization"
    schema_version: int = 1
    success: bool = False

    # Input data
    initial_molecule: ObjectId
    qc_spec: QCSpecification
    input_specification: Any = None  # Deprecated
    keywords: Dict[str, Any] = {}

    # Results
    energies: List[float] = None
    final_molecule: ObjectId = None
    trajectory: List[ObjectId] = None

    class Config:
        allow_mutation = False
        json_encoders = json_encoders
        extra = "forbid"

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    @validator('keywords')
    def check_keywords(cls, v):
        if v is not None:
            v = recursive_normalizer(v)
        return v

    def __init__(self, **data):
        super().__init__(**data)

        # Set hash index if not present
        if self.hash_index is None:
            self.__values__["hash_index"] = self.get_hash_index()

    def __str__(self):
        """
        Simplified optimization string representation.

        Returns
        -------
        ret : str
            A representation of the current Optimization status.

        Examples
        --------

        >>> repr(optimization_obj)
        Optimization(id='5b7f1fd57b87872d2c5d0a6d', status='FINISHED', molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
        """

        ret = "Optimization("
        ret += "id='{}', ".format(self.id)
        ret += "success='{}', ".format(self.success)
        ret += "initial_molecule='{}') ".format(self.initial_molecule)
        return ret

    def get_hash_index(self):

        data = self.dict(include={"initial_molecule", "program", "procedure", "keywords", "qc_spec"})

        return hash_dictionary(data)

    def get_final_energy(self):
        """The final energy of the geometry optimization.

        Returns
        -------
        float
            The optimization molecular energy.
        """
        return self.energies[-1]

    def get_trajectory(self, projection=None):
        """Returns the raw documents for each gradient evaluation in the trajectory.

        Parameters
        ----------
        client : qcportal.FractalClient
            A active client connected to a server.
        projection : None, optional
            A dictionary of the project to apply to the document

        Returns
        -------
        list of dict
            A list of results documents
        """

        return self.client.get_results(id=self.trajectory)

    def get_final_molecule(self):
        """Returns the optimized molecule

        Returns
        -------
        Molecule
            The optimized molecule
        """

        ret = self.client.get_molecules(id=[self.final_molecule])
        return ret[0]
