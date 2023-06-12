import importlib
import operator
import os
import time
from typing import Dict, Any, Union

from qcelemental.models import Provenance, FailedOperation, ComputeError

from qcfractalcompute import __version__
from qcportal.generic_result import GenericTaskResult
from qcportal.utils import capture_all_output

_this_dir = os.path.abspath(os.path.dirname(__file__))
_script_path = os.path.join(_this_dir, "run_scripts/generic_script.py")


def wrap_generic_function(
    record_id: int, function: str, function_kwargs: Dict[str, Any]
) -> Union[GenericTaskResult, FailedOperation]:
    """
    Wraps a generic function call to return a GenericTaskResult (or FailedOperation)

    This function handles capturing stdout/sterr and timing as well. This function takes
    the origin task_info (basically TaskQueueORM as a dictionary) and runs the specified function
    """

    module_name, func_name = function.split(".", 1)
    module = importlib.import_module(module_name)
    func = operator.attrgetter(func_name)(module)

    with capture_all_output("") as (rdout, rderr):
        start_time = time.time()
        try:
            results = func(**function_kwargs)
        except Exception as e:
            err = ComputeError(
                error_type=type(e).__name__,
                error_message=str(e),
            )
            return FailedOperation(id=record_id, error=err)

        end_time = time.time()

    stdout = rdout.getvalue()
    stderr = rderr.getvalue()

    ret = GenericTaskResult(
        id=record_id,
        success=True,
        stdout=stdout if stdout else None,  # convert empty string to None
        stderr=stderr if stderr else None,
        results=results,
        provenance=Provenance(
            creator=__name__,
            version=__version__,
            walltime=(end_time - start_time),
        ),
    )

    return ret
