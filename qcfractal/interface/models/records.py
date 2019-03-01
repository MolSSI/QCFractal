"""
A model for TorsionDrive
"""

import abc
import datetime
import json
from enum import Enum
from typing import Any, Dict, List, Set, Optional, Union

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

    # Classdata
    _hash_indices: Set[str]

    # Helper data
    client: Any = None
    cache: Dict[str, Any] = {}

    # Base identification
    id: ObjectId = None
    hash_index: Optional[str] = None
    procedure: str
    program: str
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

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    class Config:
        json_encoders = json_encoders
        extra = "forbid"

    def __init__(self, **data):
        super().__init__(**data)

        # Set hash index if not present
        if self.hash_index is None:
            self.hash_index = self.get_hash_index()

    def get_hash_index(self):

        data = self.json_dict(include=self._hash_indices)

        return hash_dictionary(data)

    def dict(self, *args, **kwargs):
        kwargs["exclude"] = (kwargs.pop("exclude", None) or set()) | {"client", "cache"}
        kwargs["skip_defaults"] = True
        return super().dict(*args, **kwargs)

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))


class ResultRecord(Record):

    # Version data
    version: int = 1
    procedure: constr(strip_whitespace=True, regex="single") = "single"

    # Input data
    driver: DriverEnum
    method: str
    basis: Optional[str] = None
    molecule: ObjectId
    keywords: Optional[ObjectId] = None

    # Output data
    return_results: Union[float, List[float], Dict[str, Any]] = None
    properties: qcel.models.ResultProperties = None
    error: qcel.models.ComputeError = None

    class Config(Record.Config):
        pass

    @validator('basis')
    def check_basis(cls, v):
        return prepare_basis(v)


class OptimizationRecord(Record):
    """
    A TorsionDrive Input base class
    """

    # Classdata
    _hash_indices = {"initial_molecule", "program", "procedure", "keywords", "qc_spec"}

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

    class Config(Record.Config):
        pass

    # @validator('program')
    # def check_program(cls, v):
    #     return v.lower()

    @validator('keywords')
    def check_keywords(cls, v):
        if v is not None:
            v = recursive_normalizer(v)
        return v

## QCSchema constructors

    def build_schema_input(self,
                           initial_molecule: 'Molecule',
                           qc_keywords: Optional['KeywordsSet']=None,
                           checks: bool=True) -> 'OptimizationInput':
        """
        Creates a OptimizationInput schema.
        """

        if checks:
            assert self.initial_molecule == initial_molecule.id
            if self.qc_spec.keywords:
                assert self.qc_spec.keywords == qc_keywords.id

        qcinput_spec = self.qc_spec.form_schema_object(keywords=qc_keywords, checks=checks)
        qcinput_spec.pop("program", None)

        model = qcel.models.OptimizationInput(
            id=self.id,
            initial_molecule=initial_molecule,
            keywords=self.keywords,
            extras=self.extras,
            hash_index=self.hash_index,
            input_specification=qcinput_spec)
        return model

## Standard function

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
