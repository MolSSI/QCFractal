from __future__ import annotations

import threading
import time
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import wait_until
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


def _wait_for_job(session, job_id: int, condition, timeout: float = 60.0) -> InternalJobORM:
    """
    Polls an internal job until `condition` holds, returning the (refreshed) ORM object
    """

    def _check():
        session.expire_all()
        job = session.get(InternalJobORM, job_id)
        return job if (job is not None and condition(job)) else None

    return wait_until(
        _check, timeout=timeout, message=f"Job {job_id} did not reach the expected state within {timeout} seconds"
    )


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

    try:
        # The runner has the periodic jobs queued ahead of this one, so wait for it to be
        # picked up rather than assuming it happens within some fixed amount of time
        job_1 = _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.running)
        time_1 = now_at_utc()
        assert time_0 < job_1.last_updated < time_1

        if job_func == "internal_jobs.dummy_job":
            # Progress is reported by the job as it runs
            _wait_for_job(session, id_1, lambda j: j.progress > 10, timeout=30)
        else:
            # This one doesn't take a job_progress, so it never reports any
            assert job_1.progress == 0

        job_1 = _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.complete)
        time_2 = now_at_utc()

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

    try:
        # The job with no serial group runs regardless. Of the two in serial group "test", exactly
        # one may run at a time - which of the two wins is a race between the runners, so don't
        # assume it is the one that was added first
        def _serial_group_settled():
            session.expire_all()
            jobs = [session.get(InternalJobORM, i) for i in (id_1, id_2, id_3)]
            running = [j.status == InternalJobStatusEnum.running for j in jobs]
            return jobs if (running[2] and running[0] != running[1]) else None

        job_1, job_2, job_3 = wait_until(
            _serial_group_settled, timeout=60, message="Runners did not pick up the expected jobs"
        )

        assert job_3.status == InternalJobStatusEnum.running
        assert {job_1.status, job_2.status} == {InternalJobStatusEnum.running, InternalJobStatusEnum.waiting}

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

    try:
        # Wait until the dummy job is actually running and has made some progress. The runner
        # may have other (periodic) jobs queued ahead of this one, so how long that takes is
        # not predictable - especially on a loaded CI machine
        _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.running and j.progress > 0)
    finally:
        # Cancel/close the job runner
        end_event.set()

    th.join(20)
    assert not th.is_alive()

    session.expire_all()
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

    try:
        job_1 = _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.complete)
        assert job_1.runner_uuid != old_uuid
        assert job_1.progress == 100
        assert job_1.result == "Internal job finished"
    finally:
        end_event.set()
        th.join()
