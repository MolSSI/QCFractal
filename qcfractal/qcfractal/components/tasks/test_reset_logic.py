"""
Tests for the auto-reset logic, specifically for the dictionary comprehension bug
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcelemental.models import ComputeError, FailedOperation
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import load_test_data
from qcfractalcompute.compress import compress_result
from qcportal.record_models import PriorityEnum, RecordStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


def test_reset_logic_dict_comprehension_bug(postgres_server, pytestconfig):

    pg_harness = postgres_server.get_new_harness("reset_logic_dict_bug")
    encoding = pytestconfig.getoption("--client-encoding")
    
    # Configure auto_reset with a limit of 2 unknown_errors
    extra_config = {
        "auto_reset": {
            "enabled": True,
            "unknown_error": 2 
        }
    }
    
    with QCATestingSnowflake(pg_harness, encoding=encoding, extra_config=extra_config) as snowflake:
        storage_socket = snowflake.get_storage_socket()
        activated_manager_name, _ = snowflake.activate_manager()
        activated_manager_programs = snowflake.activated_manager_programs()
        
        # Load test data and submit a singlepoint calculation
        input_spec, molecule, result_data = load_test_data("sp_psi4_water_energy")
        meta, record_ids = storage_socket.records.singlepoint.add(
            [molecule], input_spec, "tag1", PriorityEnum.normal, None, True
        )
        record_id = record_ids[0]
        
        # Create FailedOperation objects for the two error types
        fop_bad_state = FailedOperation(
            error=ComputeError(
                error_type="BadStateException",
                error_message="QOSMaxSubmitJobPerUserLimit reached"
            )
        )
        fop_too_many = FailedOperation(
            error=ComputeError(
                error_type="TooManyJobFailuresError", 
                error_message="Wrapped Parsl exception"
            )
        )
        
        with storage_socket.session_scope() as session:
            rec = session.get(BaseRecordORM, record_id)
            
            # Failure 1: BadStateException
            tasks = storage_socket.tasks.claim_tasks(
                activated_manager_name.fullname, 
                activated_manager_programs, 
                ["*"]
            )
            assert len(tasks) == 1
            storage_socket.tasks.update_finished(
                activated_manager_name.fullname,
                {tasks[0]["id"]: compress_result(fop_bad_state.dict())}
            )
            session.expire(rec)
            assert rec.status == RecordStatusEnum.waiting
            assert len(rec.compute_history) == 1
            
            # Failures 2-6: TooManyJobFailuresError
            for i in range(2):
                tasks = storage_socket.tasks.claim_tasks(
                    activated_manager_name.fullname,
                    activated_manager_programs,
                    ["*"]
                )
                print(f"Iteration {i}: rec.status = {rec.status}, history length = {len(rec.compute_history)}")
                assert len(tasks) == 1
                storage_socket.tasks.update_finished(
                    activated_manager_name.fullname,
                    {tasks[0]["id"]: compress_result(fop_too_many.dict())}
                )
                session.expire(rec)
            
                # after each failure, check the compute_history length
                assert len(rec.compute_history) == i + 2
            
            session.expire(rec)
            assert len(rec.compute_history) == 3
            
            assert rec.status == RecordStatusEnum.error, (
                "after 6 errors, the record should not be waiting."
            )

