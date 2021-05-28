"""
Procedure for a failed task
"""

from __future__ import annotations

import logging
from datetime import datetime as dt

from .base import BaseProcedureHandler
from ....interface.models import (
    RecordStatusEnum,
    TaskStatusEnum,
    FailedOperation,
)
from ...models import TaskQueueORM
from . import helpers

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...sqlalchemy_socket import SQLAlchemySocket


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

    def verify_input(self, data):
        raise RuntimeError("verify_input is not available for FailedOperationHandler")

    def create(self, session, molecule_ids, data):
        raise RuntimeError("parse_input is not available for FailedOperationHandler")

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
        base_result.error = self._core_socket.output_store.replace(base_result.error, error, session=session)

        # Get the rest of the outputs
        # This is stored in "input_data" (I know...)
        if inp_data is not None:
            stdout = inp_data.get("stdout", None)
            stderr = inp_data.get("stderr", None)
            base_result.stdout = self._core_socket.output_store.replace(base_result.stdout, stdout, session=session)
            base_result.stderr = self._core_socket.output_store.replace(base_result.stderr, stderr, session=session)

        # Change the status on the base result
        base_result.status = RecordStatusEnum.error
        base_result.manager_name = manager_name
        base_result.modified_on = dt.utcnow()
