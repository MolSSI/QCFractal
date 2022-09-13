from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.internal_jobs.socket import InternalJobSocket
from qcportal import PortalRequestError
from qcportal.internal_jobs import InternalJobStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.client import PortalClient


# Add in another function to the internal_jobs socket for testing
def dummmy_internal_job(self, iterations: int, session, job_status):
    for i in range(iterations):
        time.sleep(1.0)
        job_status.update_progress(100 * ((i + 1) / iterations))
        print("Dummy internal job counter ", i)

        if job_status.cancelled():
            return "Internal job cancelled"

    return "Internal job finished"


setattr(InternalJobSocket, "dummy_job", dummmy_internal_job)


def test_internal_jobs_client_cancel_waiting(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", datetime.utcnow(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    snowflake_client.cancel_internal_job(id_1)

    job_1 = snowflake_client.get_internal_job(id_1)
    assert job_1.status == InternalJobStatusEnum.cancelled
    assert job_1.result is None
    assert job_1.started_date is None
    assert job_1.progress == 0


def test_internal_jobs_client_cancel_running(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", datetime.utcnow(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    # Faster updates for testing
    storage_socket.internal_jobs._update_frequency = 1

    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()
    time.sleep(4)

    try:
        job_1 = snowflake_client.get_internal_job(id_1)
        assert job_1.status == InternalJobStatusEnum.running
        assert job_1.progress > 10

        snowflake_client.cancel_internal_job(id_1)
        time.sleep(6)

        job_1 = snowflake_client.get_internal_job(id_1)
        assert job_1.status == InternalJobStatusEnum.cancelled
        assert job_1.progress < 50
        assert job_1.result == "Internal job cancelled"

    finally:
        end_event.set()
        th.join()


def test_internal_jobs_client_delete_waiting(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", datetime.utcnow(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    snowflake_client.delete_internal_job(id_1)

    with pytest.raises(PortalRequestError, match="Internal job.*not found"):
        snowflake_client.get_internal_job(id_1)


def test_internal_jobs_client_delete_running(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", datetime.utcnow(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    # Faster updates for testing
    storage_socket.internal_jobs._update_frequency = 1

    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()
    time.sleep(4)

    try:
        job_1 = snowflake_client.get_internal_job(id_1)
        assert job_1.status == InternalJobStatusEnum.running
        assert job_1.progress > 10

        snowflake_client.delete_internal_job(id_1)
        time.sleep(6)

        with pytest.raises(PortalRequestError, match="Internal job.*not found"):
            snowflake_client.get_internal_job(id_1)

    finally:
        end_event.set()
        th.join()
