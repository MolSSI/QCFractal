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
from qcfractal.portal.model_utils import hash_dictionary, json_encoders, prepare_basis
from .records import OptimizationRecord, SinglePointRecord
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
