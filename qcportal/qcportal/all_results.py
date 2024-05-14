from typing import Union

# All possible records we can get from the server
# These are all the possible result objects that might be returned by a manager
from qcelemental.models import AtomicResult, OptimizationResult, FailedOperation

from .generic_result import GenericTaskResult

AllResultTypes = Union[FailedOperation, AtomicResult, OptimizationResult, GenericTaskResult]
