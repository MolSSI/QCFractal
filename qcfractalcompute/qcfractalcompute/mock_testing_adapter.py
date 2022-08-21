"""
Queue adapter for Dask
"""

from typing import Any, Dict, Hashable, Tuple

from .base_adapter import BaseAdapter


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
