"""
Either pull in QCEl models or local models
"""

from typing import Union

from . import rest_models
from .common_models import (
    AutodocBaseSettings,
    Citation,
    ProtoModel,
    ComputeError,
    DriverEnum,
    FailedOperation,
    KeywordSet,
    CompressionEnum,
    KVStore,
    Molecule,
    MoleculeIdentifiers,
    ObjectId,
    OptimizationInput,
    OptimizationResult,
    OptimizationProtocols,
    OptimizationSpecification,
    QCSpecification,
    AtomicInput,
    AtomicResult,
    AtomicResultProtocols,
    WavefunctionProperties,
    AllInputTypes,
    AllResultTypes,
    UserInfo,
    RoleInfo,
)

from .query_meta import InsertMetadata, DeleteMetadata, QueryMetadata, UpdateMetadata
from .gridoptimization import GridOptimizationInput, GridOptimizationRecord
from .model_builder import build_procedure
from .model_utils import hash_dictionary, json_encoders, prepare_basis
from .records import OptimizationRecord, SinglePointRecord, RecordStatusEnum
from .rest_models import ComputeResponse, rest_model
from .task_models import (
    ManagerStatusEnum,
    PythonComputeSpec,
    TaskRecord,
    PriorityEnum,
    SingleProcedureSpecification,
    OptimizationProcedureSpecification,
    AllProcedureSpecifications,
    AllServiceSpecifications,
)
from .torsiondrive import TorsionDriveInput, TorsionDriveRecord
