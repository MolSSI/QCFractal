from __future__ import annotations

import threading
import time
import uuid
from typing import TYPE_CHECKING

from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.components.internal_jobs.socket import InternalJobSocket
from qcportal.internal_jobs import InternalJobStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


# Add in another function to the internal_jobs socket for testing
def dummmy_internal_job(self, iterations: int, session, job_progress):
    for i in range(iterations):
        time.sleep(1.0)
        job_progress.update_progress(100 * ((i + 1) / iterations))
        print("Dummy internal job counter ", i)

        if job_progress.cancelled():
            return "Internal job cancelled"

    return "Internal job finished"


setattr(InternalJobSocket, "dummy_job", dummmy_internal_job)


def test_internal_jobs_socket_add_unique(storage_socket: SQLAlchemySocket):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=True
    )

    id_2 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=True
    )

    assert id_1 == id_2


def test_internal_jobs_socket_add_non_unique(storage_socket: SQLAlchemySocket):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    id_2 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    id_3 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    assert len({id_1, id_2, id_3}) == 3


def test_internal_jobs_socket_run(storage_socket: SQLAlchemySocket, session: Session):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    # Faster updates for testing
    storage_socket.internal_jobs._update_frequency = 1

    time_0 = now_at_utc()
    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()
    time.sleep(6)
    time_1 = now_at_utc()

    try:
        job_1 = session.get(InternalJobORM, id_1)
        assert job_1.status == InternalJobStatusEnum.running
        assert job_1.progress > 10
        assert time_0 < job_1.last_updated < time_1

        time.sleep(8)
        time_2 = now_at_utc()

        session.expire(job_1)
        job_1 = session.get(InternalJobORM, id_1)
        assert job_1.status == InternalJobStatusEnum.complete
        assert job_1.progress == 100
        assert job_1.result == "Internal job finished"
        assert time_1 < job_1.ended_date < time_2
        assert time_1 < job_1.last_updated < time_2

    finally:
        end_event.set()
        th.join()


def test_internal_jobs_socket_recover(storage_socket: SQLAlchemySocket, session: Session):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 10}, None, unique_name=False
    )

    # Faster updates for testing
    storage_socket.internal_jobs._update_frequency = 1

    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()
    time.sleep(3)

    # Cancel/close the job runner
    end_event.set()
    th.join(20)
    assert not th.is_alive()

    job_1 = session.get(InternalJobORM, id_1)
    assert job_1.status == InternalJobStatusEnum.running
    assert job_1.progress > 10
    old_uuid = job_1.runner_uuid

    # Change uuid
    storage_socket.internal_jobs._uuid = str(uuid.uuid4())

    # Job is now running but orphaned. Should be picked up next time
    time.sleep(15)
    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()
    time.sleep(15)

    try:
        session.expire(job_1)
        job_1 = session.get(InternalJobORM, id_1)
        assert job_1.status == InternalJobStatusEnum.complete
        assert job_1.runner_uuid != old_uuid
        assert job_1.progress == 100
        assert job_1.result == "Internal job finished"
    finally:
        end_event.set()
        th.join()
