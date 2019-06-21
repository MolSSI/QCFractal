"""
A special CLI function which Balsam can point to and assign as an App to behave in a similar way to other Queue
Manager's clients.

Users are not expected to manually run this CLI argument and should only be run from Balsam itself as an App
"""

import os
import traceback

from ..fractal_utils import get_function
from qcelemental.models import FailedOperation


def main():
    """
    Execute function provided by the job spec.

    This reads the Balsam DAG to get the job function, args, and kwargs to construct the work it should do.
    """
    try:
        from balsam.launcher.dag import current_job
    except ImportError:
        raise ImportError("Could not import balsam.launcher.dag.current_job. This application is designed to be run "
                          "by Balsam itself as an App, ")

    function_spec = current_job.data["function"]
    func = get_function(function_spec)
    args, kwargs = current_job.data["args"], current_job.data["kwargs"]
    try:
        engine_ret = func(*args, **kwargs)
    except Exception as e:
        blob = current_job.data
        blob["error_message"] = "Caught Balsam Processing Error:\n" + traceback.format_exc()
        blob["success"] = False
        key_data = blob
        engine_ret = FailedOperation(**key_data)
    job_id = current_job.job_id
    with open(os.path.join(current_job.working_directory, f"{job_id}-ret.json"), 'w') as f:
        f.write(engine_ret.json())


if __name__ == '__main__':
    main()
