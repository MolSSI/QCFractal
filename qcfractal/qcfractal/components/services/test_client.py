from __future__ import annotations

from typing import TYPE_CHECKING

from qcelemental.models import FailedOperation

from qcfractal.components.torsiondrive.testing_helpers import (
    submit_test_data as submit_td_test_data,
    generate_task_key as generate_td_task_key,
)
from qcfractal.testing_helpers import run_service
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_service_client_error(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    client = snowflake.client()

    id_1, result_data_1 = submit_td_test_data(storage_socket, "td_H2O2_mopac_pm6", "test_tag", PriorityEnum.low)

    # Inject a failed computation
    failed_key = list(result_data_1.keys())[1]
    result_data_1[failed_key] = FailedOperation(
        error={"error_type": "test_error", "error_message": "this is just a test error"},
    )

    time_0 = now_at_utc()
    finished, n_optimizations = run_service(
        storage_socket, activated_manager_name, id_1, generate_td_task_key, result_data_1, 20
    )
    time_1 = now_at_utc()

    rec = client.get_torsiondrives(id_1)
    assert rec.status == RecordStatusEnum.error

    err_opts = []
    for optlist in rec.optimizations.values():
        err_opts.extend(opt for opt in optlist if opt.status == RecordStatusEnum.error)
    assert len(err_opts) == 1

    assert rec.children_status[RecordStatusEnum.error] == 1
    assert len(rec.children_errors) == 1

    assert rec.children_errors[0].status == RecordStatusEnum.error
    assert rec.children_errors[0].id == err_opts[0].id
    assert rec.children_errors[0].error["error_type"] == "test_error"
    assert rec.children_errors[0].error["error_message"] == "this is just a test error"
