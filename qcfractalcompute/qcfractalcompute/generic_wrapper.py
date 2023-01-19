import importlib
import io
import operator
import time
from contextlib import redirect_stdout, redirect_stderr
from typing import Dict, Any, Union

from qcelemental.models import Provenance, FailedOperation, ComputeError

from qcfractalcompute import __version__
from qcportal.generic_result import GenericTaskResult


def wrap_generic_function(task_info: Dict[str, Any]) -> Union[GenericTaskResult, FailedOperation]:
    """
    Wraps a generic function call to return a GenericTaskResult (or FailedOperation)

    This function handles capturing stdout/sterr and timing as well. This function takes
    the origin task_info (basically TaskQueueORM as a dictionary) and runs the specified function
    """

    module_name, func_name = task_info["function"].split(".", 1)
    module = importlib.import_module(module_name)
    func = operator.attrgetter(func_name)(module)

    with redirect_stdout(io.StringIO()) as rdout:
        with redirect_stderr(io.StringIO()) as rderr:
            start_time = time.time()
            try:
                results = func(**task_info["function_kwargs"])
            except Exception as e:
                err = ComputeError(
                    error_type=type(e).__name__,
                    error_message=str(e),
                )
                return FailedOperation(id=task_info["record_id"], error=err)

            end_time = time.time()

    stdout = rdout.getvalue()
    stderr = rderr.getvalue()

    ret = GenericTaskResult(
        id=task_info["record_id"],
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
