"""
Procedure for a failed task
"""

from __future__ import annotations

import logging
from datetime import datetime as dt

from .base import BaseProcedureHandler
from ....interface.models import (
    RecordStatusEnum,
    FailedOperation,
)
from ...models import TaskQueueORM

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...sqlalchemy_socket import SQLAlchemySocket
    from typing import Optional


# A generic error message if the FailedOperation doesn't contain one
_default_error = {"error_type": "not_supplied", "error_message": "No error message found on task."}


class FailedOperationHandler(BaseProcedureHandler):
    """Handles FailedOperation that is sent from a manager

    This handles FailedOperation byt copying any info from that class that might be useful.
    """

    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)

        BaseProcedureHandler.__init__(self)

    def validate_input(self, spec):
        raise RuntimeError("validate_input is not available for FailedOperationHandler")

    def create_records(self, session, molecule_ids, spec):
        raise RuntimeError("parse_input is not available for FailedOperationHandler")

    def create_tasks(self, session, proc_orm, tag, priority):
        raise RuntimeError("create_tasks is not available for FailedOperationHandler")

    def update_completed(self, session: Session, task_orm: TaskQueueORM, manager_name: str, result: FailedOperation):
        """
        Update the database with information from a completed (but failed) task

        The session is not flushed or committed
        """

        fail_result = result.dict()
        inp_data = fail_result.get("input_data")

        # Error is special in a FailedOperation
        error = fail_result.get("error", _default_error)

        base_result = task_orm.base_result_obj

        # Get the rest of the outputs
        # This is stored in "input_data" (I know...)
        stdout = None
        stderr = None

        if inp_data is not None:
            stdout = inp_data.get("stdout", None)
            stderr = inp_data.get("stderr", None)

        self._core_socket.procedure.update_outputs(session, base_result, stdout=stdout, stderr=stderr, error=error)

        # Change the status on the base result
        base_result.status = RecordStatusEnum.error
        base_result.manager_name = manager_name
        base_result.modified_on = dt.utcnow()
