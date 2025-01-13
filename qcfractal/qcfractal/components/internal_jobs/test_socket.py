from __future__ import annotations

import threading
import time
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.components.internal_jobs.socket import InternalJobSocket
from qcportal.internal_jobs import InternalJobStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


# Add in another function to the internal_jobs socket for testing
def dummy_internal_job(self, iterations: int, session, job_progress):
    assert session is not None
    assert job_progress is not None
    for i in range(iterations):
        time.sleep(1.0)
        job_progress.update_progress(100 * ((i + 1) / iterations))
        # print("Dummy internal job counter ", i)

        job_progress.raise_if_cancelled()

    return "Internal job finished"


# Add in another function to the internal_jobs socket for testing
# This one doesn't have session or job_progress
def dummy_internal_job_2(self, iterations: int):
    for i in range(iterations):
        time.sleep(1.0)
        # print("Dummy internal job counter ", i)

    return "Internal job finished"


setattr(InternalJobSocket, "dummy_job", dummy_internal_job)
setattr(InternalJobSocket, "dummy_job_2", dummy_internal_job_2)


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


@pytest.mark.parametrize("job_func", ("internal_jobs.dummy_job", "internal_jobs.dummy_job_2"))
def test_internal_jobs_socket_run(storage_socket: SQLAlchemySocket, session: Session, job_func: str):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), job_func, {"iterations": 10}, None, unique_name=False
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
        assert time_0 < job_1.last_updated < time_1

        if job_func == "internal_jobs.dummy_job":
            assert job_1.progress > 10
        else:
            assert job_1.progress == 0

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


@pytest.mark.parametrize("job_func", ("internal_jobs.dummy_job", "internal_jobs.dummy_job_2"))
def test_internal_jobs_socket_run_serial(storage_socket: SQLAlchemySocket, session: Session, job_func: str):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), job_func, {"iterations": 10}, None, unique_name=False, serial_group="test"
    )
    id_2 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), job_func, {"iterations": 10}, None, unique_name=False, serial_group="test"
    )
    id_3 = storage_socket.internal_jobs.add(
        "dummy_job",
        now_at_utc(),
        job_func,
        {"iterations": 10},
        None,
        unique_name=False,
    )

    # Faster updates for testing
    storage_socket.internal_jobs._update_frequency = 1

    end_event = threading.Event()
    th1 = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th2 = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th3 = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th1.start()
    th2.start()
    th3.start()
    time.sleep(8)

    try:
        job_1 = session.get(InternalJobORM, id_1)
        job_2 = session.get(InternalJobORM, id_2)
        job_3 = session.get(InternalJobORM, id_3)
        assert job_1.status == InternalJobStatusEnum.running
        assert job_2.status == InternalJobStatusEnum.waiting
        assert job_3.status == InternalJobStatusEnum.running

    finally:
        end_event.set()
        th1.join()
        th2.join()
        th3.join()


def test_internal_jobs_socket_runnerstop(storage_socket: SQLAlchemySocket, session: Session):
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
    assert job_1.status == InternalJobStatusEnum.waiting
    assert job_1.progress == 0
    assert job_1.started_date is None
    assert job_1.last_updated is None
    assert job_1.runner_uuid is None

    return
    old_uuid = job_1.runner_uuid

    # Change uuid
    storage_socket.internal_jobs._uuid = str(uuid.uuid4())

    # Job is now running but orphaned. Should be picked up next time
    time.sleep(15)
    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()
    time.sleep(30)

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


def test_internal_jobs_socket_recover(storage_socket: SQLAlchemySocket, session: Session):
    id_1 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job", {"iterations": 5}, None, unique_name=False
    )

    # Faster updates for testing
    storage_socket.internal_jobs._update_frequency = 1

    # Manually make it seem like it's running
    old_uuid = str(uuid.uuid4())
    job_1 = session.get(InternalJobORM, id_1)
    job_1.status = InternalJobStatusEnum.running
    job_1.progress = 10
    job_1.last_updated = now_at_utc() - timedelta(seconds=60)
    job_1.runner_uuid = old_uuid
    session.commit()

    session.expire(job_1)
    job_1 = session.get(InternalJobORM, id_1)
    assert job_1.status == InternalJobStatusEnum.running
    assert job_1.runner_uuid == old_uuid

    # Job is now running but orphaned. Should be picked up next time
    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()
    time.sleep(10)

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
