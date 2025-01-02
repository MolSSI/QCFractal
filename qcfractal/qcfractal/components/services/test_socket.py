from __future__ import annotations

from typing import TYPE_CHECKING

from qcelemental.models import FailedOperation

from qcfractal.components.gridoptimization.testing_helpers import (
    submit_test_data as submit_go_test_data,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.torsiondrive.record_db_models import TorsiondriveRecordORM
from qcfractal.components.torsiondrive.testing_helpers import (
    submit_test_data as submit_td_test_data,
    generate_task_key as generate_td_task_key,
)
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service
from qcportal.managers import ManagerName
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


def test_service_socket_error(storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName):
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

    assert finished is True

    rec = session.get(TorsiondriveRecordORM, id_1)

    assert rec.status == RecordStatusEnum.error

    child_stat = storage_socket.records.torsiondrive.get_children_status(id_1, session=session)
    assert child_stat[RecordStatusEnum.error] == 1
    assert len(rec.compute_history) == 1
    assert len(rec.compute_history[-1].outputs) == 2  # stdout and error
    assert rec.compute_history[-1].status == RecordStatusEnum.error
    assert time_0 < rec.compute_history[-1].modified_on < time_1
    assert rec.service is not None

    err = rec.compute_history[-1].outputs["error"].get_output()
    assert "did not complete successfully" in err["error_message"]

    err_ids = [x.optimization_id for x in rec.optimizations if x.optimization_record.status == RecordStatusEnum.error]
    assert len(err_ids) == 1
    child_error_ids = storage_socket.records.torsiondrive.get_children_errors(id_1, session=session)
    assert len(child_error_ids) == 1

    assert child_error_ids[0] == err_ids[0]
    child_rec = session.get(BaseRecordORM, child_error_ids[0])
    assert child_rec.status == RecordStatusEnum.error


def test_service_socket_iterate_order(storage_socket: SQLAlchemySocket, session: Session):
    # TODO - Ugly
    max_active_services = storage_socket.services._max_active_services
    try:
        storage_socket.services._max_active_services = 1

        id_1, _ = submit_td_test_data(storage_socket, "td_H2O2_mopac_pm6", "*", PriorityEnum.normal)
        id_2, _ = submit_go_test_data(storage_socket, "go_H3NS_psi4_pbe", "*", PriorityEnum.high)

        with storage_socket.session_scope() as s:
            storage_socket.services.iterate_services(s)

        assert session.get(BaseRecordORM, id_1).status == RecordStatusEnum.waiting
        assert session.get(BaseRecordORM, id_2).status == RecordStatusEnum.running
    finally:
        storage_socket.services._max_active_services = max_active_services
