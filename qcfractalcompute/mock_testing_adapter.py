"""
Queue adapter for Dask
"""

import traceback
from typing import Any, Dict, Hashable, Tuple

from qcelemental.models import FailedOperation

from .base_adapter import BaseAdapter


def _get_future(future):
    try:
        return future.result()
    except Exception as e:
        msg = "Caught Executor Error:\n" + traceback.format_exc()
        ret = FailedOperation(**{"success": False, "error": {"error_type": e.__class__.__name__, "error_message": msg}})
        return ret


class MockTestingAdapter(BaseAdapter):
    """A Queue Adapter for mock testing

    This adapter doesn't run any calculation, and instead is given the
    results for the tasks directly
    """

    def __repr__(self):

        return "<MockTestingAdapter client=<{}>>".format(self.client.__class__.__name__)

    def _submit_task(self, task_spec: Dict[str, Any]) -> Tuple[Hashable, Any]:
        # Save the record id. That is what is the index is in result_data
        return task_spec["id"], task_spec["record_id"]

    def count_active_task_slots(self) -> int:
        return 10

    def acquire_complete(self) -> Dict[str, Any]:
        ret = {}
        for x, y in self.queue.items():
            r = self.client._result_data.get(y, None)
            if r is None:
                raise RuntimeError(f"Do not have result data for record id {y}")
            ret[x] = r

        self.queue = {}
        return ret

    def await_results(self) -> bool:
        return True

    def close(self) -> bool:
        return True
