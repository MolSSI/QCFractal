"""
Either pull in QCEl models or local models
"""

from . import rest_models
from .common_models import (
    AutodocBaseSettings,
    Citation,
    ProtoModel,
    ComputeError,
    DriverEnum,
    FailedOperation,
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
    AllInputTypes,
    AllResultTypes,
)

from .gridoptimization import GridOptimizationInput, GridOptimizationRecord
from .model_builder import build_procedure
from ...portal.utils import hash_dictionary
from .records import OptimizationRecord, SinglepointRecord
from .rest_models import ComputeResponse, rest_model
from .task_models import (
    PythonComputeSpec,
    TaskRecord,
    SingleProcedureSpecification,
    OptimizationProcedureSpecification,
    AllProcedureSpecifications,
    AllServiceSpecifications,
)
from ...portal.records import PriorityEnum, RecordStatusEnum
from ...portal.managers import ManagerStatusEnum
from .torsiondrive import TorsionDriveInput, TorsionDriveRecord
