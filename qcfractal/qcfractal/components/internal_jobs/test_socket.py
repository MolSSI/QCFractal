from __future__ import annotations

import threading
import time
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

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


# Blocks until the test releases it, so a test can control exactly when the job finishes.
# Takes no job_progress, so (like dummy_job_2) it never checks for cancellation
_job_gates: dict = {}


def gated_internal_job(self, gate_name: str):
    assert _job_gates[gate_name].wait(60), "test never released the job"
    return "Internal job finished"


setattr(InternalJobSocket, "dummy_job", dummy_internal_job)
setattr(InternalJobSocket, "dummy_job_2", dummy_internal_job_2)
setattr(InternalJobSocket, "gated_job", gated_internal_job)


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


def test_internal_jobs_socket_repeat_keeps_serial_group(storage_socket: SQLAlchemySocket, session: Session):
    # The repeat of a job must be an identical job - including its serial group, which the
    # re-add used to leave out
    id_1 = storage_socket.internal_jobs.add(
        "repeating_serial_job",
        now_at_utc(),
        "internal_jobs.dummy_job_2",
        {"iterations": 1},
        None,
        unique_name=False,
        repeat_delay=3600,  # far enough out that the repeat never runs during the test
        serial_group="repeat_test_group",
    )

    storage_socket.internal_jobs._update_frequency = 1

    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()

    try:
        _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.complete)
    finally:
        end_event.set()
        th.join()

    session.expire_all()
    stmt = select(InternalJobORM).where(InternalJobORM.name == "repeating_serial_job", InternalJobORM.id != id_1)
    repeats = session.execute(stmt).scalars().all()

    assert len(repeats) == 1
    assert repeats[0].status == InternalJobStatusEnum.waiting
    assert repeats[0].repeat_delay == 3600
    assert repeats[0].serial_group == "repeat_test_group"


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
        job_1 = _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.running and j.progress > 0)
        first_runner_uuid = job_1.runner_uuid
        assert first_runner_uuid is not None
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

    # The job is back to waiting, so a new runner should pick it up and run it to completion
    end_event = threading.Event()
    th = threading.Thread(target=storage_socket.internal_jobs.run_loop, args=(end_event,))
    th.start()

    try:
        job_1 = _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.complete)
        assert job_1.progress == 100
        assert job_1.result == "Internal job finished"
        assert job_1.runner_uuid != first_runner_uuid
    finally:
        end_event.set()
        th.join()


def test_internal_jobs_socket_delete_while_running(storage_socket: SQLAlchemySocket, session: Session):
    # gated_job takes no job_progress, so it never checks for cancellation. Deleting its row while
    # it runs used to leave _run_single flushing a row that no longer existed - SQLAlchemy raises
    # StaleDataError, which propagated out of run_loop and killed the whole runner thread
    _job_gates["delete_while_running"] = threading.Event()

    id_1 = storage_socket.internal_jobs.add(
        "gated_job",
        now_at_utc(),
        "internal_jobs.gated_job",
        {"gate_name": "delete_while_running"},
        None,
        unique_name=False,
    )

    # Runs after the one above, and is only reached if the runner survives the deletion
    id_2 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job_2", {"iterations": 1}, None, unique_name=False
    )

    # A long update period, so that once the progress-updating thread has polled once it will not
    # poll again for the rest of the test. That poll is the last chance it has to see the deletion
    storage_socket.internal_jobs._update_frequency = 30

    runner_error = []

    def _run_loop():
        try:
            storage_socket.internal_jobs.run_loop(end_event)
        except BaseException as ex:
            runner_error.append(ex)

    end_event = threading.Event()
    th = threading.Thread(target=_run_loop)
    th.start()

    try:
        job_1 = _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.running)
        claimed_at = job_1.last_updated

        # run_loop sets last_updated when it claims the job, and the updating thread sets it again
        # on each poll. Waiting for it to change is how we know that thread has polled at least
        # once, so that the deletion below lands after it rather than racing it
        _wait_for_job(session, id_1, lambda j: j.last_updated != claimed_at)

        storage_socket.internal_jobs.delete(id_1)

        # Let the job finish. The runner now writes its final state believing the row still exists
        _job_gates["delete_while_running"].set()

        # It should notice the row is gone, drop the job, and carry on to the next one
        _wait_for_job(session, id_2, lambda j: j.status == InternalJobStatusEnum.complete)

        session.expire_all()
        assert session.get(InternalJobORM, id_1) is None

    finally:
        _job_gates["delete_while_running"].set()
        end_event.set()
        th.join(30)

    assert not th.is_alive()
    assert not runner_error, f"Runner thread died: {runner_error[0]!r}"


def test_internal_jobs_socket_taken_over_while_running(storage_socket: SQLAlchemySocket, session: Session):
    # If another runner decides this one is dead and takes its job over, this runner must not
    # write anything to the row when the job finishes - the new owner's work would be clobbered
    _job_gates["taken_over"] = threading.Event()

    id_1 = storage_socket.internal_jobs.add(
        "gated_job", now_at_utc(), "internal_jobs.gated_job", {"gate_name": "taken_over"}, None, unique_name=False
    )

    # Runs after the one above, and is only reached if the runner survives losing the first job
    id_2 = storage_socket.internal_jobs.add(
        "dummy_job", now_at_utc(), "internal_jobs.dummy_job_2", {"iterations": 1}, None, unique_name=False
    )

    storage_socket.internal_jobs._update_frequency = 30

    runner_error = []

    def _run_loop():
        try:
            storage_socket.internal_jobs.run_loop(end_event)
        except BaseException as ex:
            runner_error.append(ex)

    end_event = threading.Event()
    th = threading.Thread(target=_run_loop)
    th.start()

    try:
        job_1 = _wait_for_job(session, id_1, lambda j: j.status == InternalJobStatusEnum.running)
        claimed_at = job_1.last_updated

        # Wait for the updating thread's first poll, so the takeover below lands after it
        _wait_for_job(session, id_1, lambda j: j.last_updated != claimed_at)

        # Stand in for another runner claiming the job out from under this one
        other_uuid = str(uuid.uuid4())
        job_1 = session.get(InternalJobORM, id_1)
        job_1.runner_uuid = other_uuid
        job_1.progress = 42
        session.commit()

        # Let the job finish. Its result must be thrown away
        _job_gates["taken_over"].set()

        # The runner should drop the job and carry on to the next one
        _wait_for_job(session, id_2, lambda j: j.status == InternalJobStatusEnum.complete)

        session.expire_all()
        job_1 = session.get(InternalJobORM, id_1)
        assert job_1.runner_uuid == other_uuid
        assert job_1.status == InternalJobStatusEnum.running
        assert job_1.progress == 42
        assert job_1.ended_date is None
        assert job_1.result is None

    finally:
        _job_gates["taken_over"].set()
        end_event.set()
        th.join(30)

    assert not th.is_alive()
    assert not runner_error, f"Runner thread died: {runner_error[0]!r}"


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
