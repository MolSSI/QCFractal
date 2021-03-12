"""
Procedure for a failed task
"""

from datetime import datetime as dt

from .base import BaseTasks
from ..interface.models import RecordStatusEnum, KVStore, CompressionEnum, build_procedure, FailedOperation


class FailedOperationHandler(BaseTasks):
    """Handles FailedOperation that is sent from a manager

    This handles FailedOperation byt copying any info from that class that might be useful.
    """

    def verify_input(self, data):
        raise RuntimeError("verify_input is not available for FailedOperationHandler")

    def parse_input(self, data):
        raise RuntimeError("parse_input is not available for FailedOperationHandler")

    def handle_completed_output(self, task_id: int, base_result_id: int, manager_name: str, result: FailedOperation):

        failed_tasks = []

        fail_result = result.dict()

        err = fail_result.get("error")
        if err is None:
            err = {"error_type": "not_supplied", "error_message": "No error message found on task."}

        # Compress error dicts here. Should be ok since they are small
        err_compressed = KVStore.compress(err, CompressionEnum.lzma, 1)
        err_id = self.storage.add_kvstore([err_compressed])["data"][0]

        # A little hacky. Create a dict for stdout,stderr and then use retrieve_outputs
        # These are stored in input_data for a FailedOperation object
        inp_data = fail_result.get("input_data")

        if inp_data:
            prog_outputs = {
                "stdout": inp_data.get("stdout"),
                "stderr": inp_data.get("stderr"),
                "error": fail_result["error"],
            }

        # Will be a dictionary, but may be a real procedure or a plain result
        # We are only modifying attributes of base_result, though, so that doesn't
        # matter
        rec = self.storage.get_procedures(id=base_result_id)["data"][0]

        rec["error"] = err_id
        rec["status"] = RecordStatusEnum.error
        rec["manager_name"] = manager_name
        rec["modified_on"] = dt.utcnow()

        # TODO - must be done before marking result as error due to race condition
        # (will be fixed with better transaction handling)
        self.storage.queue_mark_error(task_id)

        # TODO - could use an update_procedures that can take single results, too
        proc = build_procedure(rec)
        if rec["procedure"] == "single":
            self.storage.update_results([proc])
        else:
            self.storage.update_procedures([proc])

        failed_tasks.append(task_id)

        # Return success/failures
        # (successes is a placeholder for now)
        return [], failed_tasks
