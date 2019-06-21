"""
Queue adapter for Balsam
"""

import json
import logging
import os
import time
import traceback
from typing import Any, Dict, Hashable, Optional, Tuple, Union

from .base_adapter import BaseAdapter

from qcelemental.models import Result, Optimization, FailedOperation
from qcelemental.models.common_models import qcschema_optimization_output_default, qcschema_output_default

schema_mapper = {qcschema_output_default: Result,
                 qcschema_optimization_output_default: Optimization}


def _get_result(job: 'BalsamJob') -> Union[Result, Optimization, FailedOperation]:
    job_output_path = f"{job.job_id}-ret.json"
    ret = FailedOperation(error={"error_message": f"The job at {job.job_id} could not be retried for an unknown reason",
                                 "error_type": "UnknownError"
                                 },
                          success=False)
    try:
        with open(os.path.join(job.working_directory, job_output_path), 'r') as f:
            result_dict = json.load(f)
        if result_dict['success']:
            ret = schema_mapper[result_dict['schema_name']](**result_dict)
        else:
            ret = FailedOperation(**result_dict)
    except Exception:
        blob = {"error": {"error_message": traceback.format_exc(), "error_type": "UnknownError"},
                "success": False}
        ret = FailedOperation(**blob)
    finally:
        try:
            os.remove(job_output_path)
        except Exception:
            pass

    return ret


class BalsamAdapter(BaseAdapter):
    """An Adapter for Balsam.
    """

    def __init__(self, client: Any, logger: Optional[logging.Logger] = None, **kwargs):
        BaseAdapter.__init__(self, client, logger, **kwargs)

        import balsam.launcher.dag as dag
        from balsam.core.models import BalsamJob
        self.job_model = BalsamJob
        self.job_data = client.dict()
        self.client = dag

    def ended_jobs(self) -> 'QuerySet':
        from balsam.core.models import END_STATES
        return self.job_model.objects.filter(job_id__in=self.queue.values(), state__in=END_STATES)

    def incomplete_jobs(self)-> 'QuerySet':
        from balsam.core.models import END_STATES
        return self.job_model.objects.filter(job_id__in=self.queue.values()).exclude(state__in=END_STATES)

    def __repr__(self):
        return f"<BalsamAdapter BalsamJob=<data: '{self.job_data}'>>"

    def _submit_task(self, task_spec: Dict[str, Any]) -> Tuple[Hashable, Any]:

        # Form sub dict
        submit_data = {"function": task_spec["spec"]["function"],
                       "args": task_spec["spec"]["args"],
                       "kwargs": task_spec["spec"]["kwargs"]}
        job = self.job_model()
        # This is how Balsam's construct goes
        for k, v in self.job_data.items():
            try:
                getattr(job, k)
            except AttributeError:
                raise ValueError(f"Invalid field {k} for a BalsamJob")
            else:
                setattr(job, k, v)
        # Add the submission data
        job.data = submit_data
        job.save()

        return task_spec["id"], job.job_id

    def acquire_complete(self) -> Dict[str, Any]:
        ret = {}
        del_keys = []
        completed_jobs = {}
        for job in self.ended_jobs():
            completed_jobs[job.job_id] = _get_result(job)
        for key, job_id in self.queue.items():
            if job_id not in completed_jobs:
                continue
            ret[key] = completed_jobs[job_id]
            del_keys.append(key)

        for key in del_keys:
            del self.queue[key]

        return ret

    def await_results(self) -> bool:
        while True:
            nunfinished = len(self.incomplete_jobs())
            if nunfinished == 0:
                break
            time.sleep(0.1 * nunfinished)
        return True

    def close(self) -> bool:
        for job in self.incomplete_jobs():
            job.update_state('USER_KILLED')
        return True
