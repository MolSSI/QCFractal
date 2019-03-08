"""
A model for TorsionDrive
"""

import abc
import datetime
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

import qcelemental as qcel
from pydantic import BaseModel, constr, validator

from .common_models import DriverEnum, ObjectId, QCSpecification
from .model_utils import hash_dictionary, json_encoders, prepare_basis, recursive_normalizer

__all__ = ["OptimizationRecord", "ResultRecord", "OptimizationRecord"]


class RecordStatusEnum(str, Enum):
    complete = "COMPLETE"
    incomplete = "INCOMPLETE"
    running = "RUNNING"
    error = "ERROR"


class RecordBase(BaseModel, abc.ABC):
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
    error: Optional[ObjectId] = None

    # Compute status
    task_id: ObjectId = None
    status: RecordStatusEnum = "INCOMPLETE"
    modified_on: datetime.datetime = datetime.datetime.utcnow()
    created_on: datetime.datetime = datetime.datetime.utcnow()
    error: Optional[Any] = None

    # Carry-ons
    provenance: Optional[qcel.models.Provenance] = None

    class Config:
        json_encoders = json_encoders
        extra = "forbid"
        build_hash_index = True

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    def __init__(self, **data):
        # Make sure several fields are available and written to prevent sparse dic

        super().__init__(**data)

        # Set hash index if not present
        if self.Config.build_hash_index and (self.hash_index is None):
            self.hash_index = self.get_hash_index()

### Serialization helpers

    @classmethod
    def get_hash_fields(cls):
        return cls._hash_indices | {"procedure", "program"}

    def get_hash_index(self):

        data = self.json_dict(include=self.get_hash_fields())

        return hash_dictionary(data)

    def dict(self, *args, **kwargs):
        kwargs["exclude"] = (kwargs.pop("exclude", None) or set()) | {"client", "cache"}
        # kwargs["skip_defaults"] = True
        return super().dict(*args, **kwargs)

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))

### Checkers

    def check_client(self):
        if self.client is None:
            raise ValueError("Requested method requires a client, but client was '{}'.".format(self.client))

### KVStore Getters

    def _kvstore_getter(self, field_name):
        """
        Internal KVStore getting object
        """
        self.check_client()

        oid = self.__values__[field_name]
        if oid is None:
            return None

        if field_name not in self.cache:
            self.cache[field_name] = self.client.query_kvstore([oid])[oid]

        return self.cache[field_name]

    def get_stdout(self) -> Optional[str]:
        """Pulls the stdout from the denormalized KVStore and returns it to the user.

        Returns
        -------
        Optional[str]
            The requested stdout, none if no stderr present.
        """
        return self._kvstore_getter("stdout")

    def get_stderr(self) -> Optional[str]:
        """Pulls the stderr from the denormalized KVStore and returns it to the user.

        Returns
        -------
        Optional[str]
            The requested stderr, none if no stderr present.
        """

        return self._kvstore_getter("stderr")

    def get_error(self) -> Optional[qcel.models.ComputeError]:
        """Pulls the stderr from the denormalized KVStore and returns it to the user.

        Returns
        -------
        Optional[qcel.models.ComputeError]
            The requested compute error, none if no error present.
        """
        value = self._kvstore_getter("error")
        if value:
            return qcel.models.ComputeError(**value)
        else:
            return value


class ResultProperties(BaseModel):
    """
    Copy of QCElemental.ResultProperties wil updates trickle through. Remove ASAP.
    """

    # Calcinfo
    calcinfo_nbasis: int = None
    calcinfo_nmo: int = None
    calcinfo_nalpha: int = None
    calcinfo_nbeta: int = None
    calcinfo_natom: int = None

    # Canonical
    nuclear_repulsion_energy: float = None
    return_energy: float = None

    # SCF Keywords
    scf_one_electron_energy: float = None
    scf_two_electron_energy: float = None
    scf_vv10_energy: float = None
    scf_xc_energy: float = None
    scf_dispersion_correction_energy: float = None
    scf_dipole_moment: List[float] = None
    scf_total_energy: float = None
    scf_iterations: int = None

    # MP2 Keywords
    mp2_same_spin_correlation_energy: float = None
    mp2_opposite_spin_correlation_energy: float = None
    mp2_singles_energy: float = None
    mp2_doubles_energy: float = None
    mp2_total_correlation_energy: float = None
    mp2_total_energy: float = None

    class Config:
        allow_mutation = False
        extra = "forbid"

    def dict(self, *args, **kwargs):
        return super().dict(*args, **{**kwargs, **{"skip_defaults": True}})


class ResultRecord(RecordBase):

    # Classdata
    _hash_indices = {"driver", "method", "basis", "molecule", "keywords", "program"}

    # Version data
    version: int = 1
    procedure: constr(strip_whitespace=True, regex="single") = "single"

    # Input data
    driver: DriverEnum
    method: str
    molecule: ObjectId
    basis: Optional[str] = None
    keywords: Optional[ObjectId] = None

    # Output data
    return_result: Union[float, List[float], Dict[str, Any]] = None
    properties: ResultProperties = None  # deprecate for qcel.models.ResultProperties

    class Config(RecordBase.Config):
        build_hash_index = False

    @validator('method')
    def check_method(cls, v):
        return v.lower()

    @validator('basis')
    def check_basis(cls, v):
        return prepare_basis(v)

## QCSchema constructors

    def build_schema_input(self, molecule: 'Molecule', keywords: Optional['KeywordsSet']=None,
                           checks: bool=True) -> 'ResultInput':
        """
        Creates a OptimizationInput schema.
        """

        if checks:
            assert self.molecule == molecule.id
            if self.keywords:
                assert self.keywords == keywords.id

        model = {"method": self.method}
        if self.basis:
            model["basis"] = self.basis

        if not self.keywords:
            keywords = {}
        else:
            keywords = keywords.values

        model = qcel.models.ResultInput(
            id=self.id, driver=self.driver.name, model=model, molecule=molecule, keywords=keywords, extras=self.extras)
        return model

    def consume_output(self, data: Dict[str, Any], checks: bool=True):
        assert self.method == data["model"]["method"]

        # Result specific
        self.extras = data["extras"]
        self.extras.pop("_qcfractal_tags", None)
        self.return_result = data["return_result"]
        self.properties = data["properties"]

        # Standard blocks
        self.provenance = data["provenance"]
        self.error = data["error"]
        self.stdout = data["stdout"]
        self.stderr = data["stderr"]
        self.status = "COMPLETE"


class OptimizationRecord(RecordBase):
    """
    A OptimizationRecord for all optimization procedure data.
    """

    # Classdata
    _hash_indices = {"initial_molecule", "keywords", "qc_spec"}

    # Version data
    version: int = 1
    procedure: constr(strip_whitespace=True, regex="optimization") = "optimization"
    schema_version: int = 1

    # Input data
    initial_molecule: ObjectId
    qc_spec: QCSpecification
    keywords: Dict[str, Any] = {}

    # Results
    energies: List[float] = None
    final_molecule: ObjectId = None
    trajectory: List[ObjectId] = None

    class Config(RecordBase.Config):
        pass

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
        ret += "status='{}', ".format(self.status)
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

        return self.client.query_results(id=self.trajectory)

    def get_final_molecule(self):
        """Returns the optimized molecule

        Returns
        -------
        Molecule
            The optimized molecule
        """

        ret = self.client.query_molecules(id=[self.final_molecule])
        return ret[0]
