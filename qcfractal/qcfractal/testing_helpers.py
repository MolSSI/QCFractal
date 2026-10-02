from __future__ import annotations

import logging
import time
from typing import Dict, Any, Tuple, Callable, Optional

from sqlalchemy import select

from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractalcompute.compress import compress_result
from qcportal.compression import decompress, CompressionEnum
from qcportal.internal_jobs import InternalJobStatusEnum
from qcportal.managers import ManagerName
from qcportal.qcschema_v1 import Molecule
from qcportal.record_models import RecordStatusEnum, RecordTask
from qcportal.utils import now_at_utc

mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="2234-5678-1234-5678")

# The runner uuid run_service claims internal jobs under
_run_service_runner_uuid = "1234-5678-9101-1213"


class DummyJobProgress:
    """
    Functor for updating progress and cancelling internal jobs

    This is a dummy version used for testing
    """

    def __init__(self):
        self._runner_uuid = _run_service_runner_uuid

    def update_progress(self, progress: int):
        pass

    def stop(self):
        pass

    def cancelled(self) -> bool:
        return False

    def deleted(self) -> bool:
        return False


def _run_service_iteration_job(storage_socket: SQLAlchemySocket, session, jobname: str) -> Optional[Any]:
    """
    Runs the internal job that iterates a service, standing in for the internal job runner

    Some tests also start a real job runner (for other background work), and its periodic
    iterate_services queues and runs these same jobs. So claim the job the way a runner would,
    so that exactly one of them runs it. If a real runner got there first, wait for it to finish
    and use its result instead of iterating the service a second time.

    Returns the job's result, or None if there was no such job
    """

    # Claim the job, exactly as run_loop does
    stmt = select(InternalJobORM).where(
        InternalJobORM.unique_name == jobname, InternalJobORM.status == InternalJobStatusEnum.waiting
    )
    job_orm = session.execute(stmt.with_for_update(skip_locked=True)).scalar_one_or_none()

    if job_orm is not None:
        job_id = job_orm.id
        job_orm.status = InternalJobStatusEnum.running
        job_orm.runner_uuid = _run_service_runner_uuid
        job_orm.started_date = job_orm.last_updated = now_at_utc()
        session.commit()

        storage_socket.internal_jobs._run_single(
            session,
            job_orm,
            logging.getLogger("internal_job"),
            DummyJobProgress(),
            runner_uuid=_run_service_runner_uuid,
        )
    else:
        # Not claimable. Either there is no such job, or a real runner has it (or is claiming it right now)
        job_id = session.execute(select(InternalJobORM.id).where(InternalJobORM.unique_name == jobname)).scalar()
        session.rollback()
        if job_id is None:
            return None

    # Read the outcome from the database rather than from job_orm: if anything other than this
    # function ended up running the job, _run_single will have discarded this copy

    finished = (InternalJobStatusEnum.complete, InternalJobStatusEnum.error, InternalJobStatusEnum.cancelled)
    deadline = time.monotonic() + 120
    while True:
        with storage_socket.session_scope() as s:
            status, result = s.execute(
                select(InternalJobORM.status, InternalJobORM.result).where(InternalJobORM.id == job_id)
            ).one()

        if status in finished:
            return result

        assert time.monotonic() < deadline, f"Internal job {job_id} ({jobname}) never finished"
        time.sleep(0.1)


def run_service(
    storage_socket: SQLAlchemySocket,
    manager_name: ManagerName,
    record_id: int,
    task_key_generator: Callable,
    result_data: Dict[str, Any],
    max_iterations: int = 20,
) -> Tuple[bool, int]:
    """
    Runs a service
    """

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, record_id)
        assert rec.status in [RecordStatusEnum.waiting, RecordStatusEnum.running]

        owner_user = rec.owner_user.username if rec.owner_user is not None else None

        tag = rec.service.compute_tag
        priority = rec.service.compute_priority
        service_id = rec.service.id

    n_records = 0
    n_iterations = 0
    finished = False

    while n_iterations < max_iterations:
        with storage_socket.session_scope() as session:
            n_services = storage_socket.services.iterate_services(session)

            # iterate_services will handle errors
            if n_services == 0:
                finished = True
                break

            # Kinda hacky...
            # Run any internal jobs that iterate_services added
            job_result = _run_service_iteration_job(storage_socket, session, f"iterate_service_{service_id}")

            if job_result is not None:
                # The function that iterates a service returns True if it is finished
                if job_result is True:
                    rec: BaseRecordORM = session.get(BaseRecordORM, record_id)

                    if rec.status == RecordStatusEnum.error:
                        print("Error in service dependency")
                        print(rec.status)
                        print(rec.compute_history[-1].status)
                        print(decompress(rec.compute_history[-1].outputs["error"].data, CompressionEnum.zstd))

                    assert rec.status == RecordStatusEnum.complete
                    assert rec.service is None
                    finished = True
                    break

        n_iterations += 1

        with storage_socket.session_scope() as session:
            rec = session.get(BaseRecordORM, record_id)
            assert rec.status == RecordStatusEnum.running

        # only do 5 tasks at a time. Tests iteration when stuff is not completed
        manager_programs = storage_socket.managers.get([manager_name.fullname])[0]["programs"]
        manager_tasks_d = storage_socket.tasks.claim_tasks(manager_name.fullname, manager_programs, ["*"], limit=5)
        manager_tasks = [RecordTask(**x) for x in manager_tasks_d]

        # Sometimes a task may be duplicated in the service dependencies.
        # The C8H6 test has this "feature"
        ids = set(x.record_id for x in manager_tasks)

        with storage_socket.session_scope() as session:
            recs = [session.get(BaseRecordORM, i) for i in ids]
            all_usernames = [x.owner_user.username if x.owner_user is not None else None for x in recs]
            assert all(x == owner_user for x in all_usernames)
            assert all(x.task.compute_priority == priority for x in recs)
            assert all(x.task.compute_tag == tag for x in recs)

        manager_ret = {}
        for t in manager_tasks:
            # The results dict has keys that are generated by a function
            # That same function is passed into this function
            task_key = task_key_generator(t)

            task_result = result_data.get(task_key, None)
            if task_result is None:
                raise RuntimeError(f"Cannot find task results! key = {task_key}")

            manager_ret[t.id] = compress_result(task_result.model_dump())

        rmeta = storage_socket.tasks.update_finished(manager_name.fullname, manager_ret)
        assert rmeta.n_accepted == len(manager_tasks)
        n_records += len(manager_ret)

    return finished, n_records


def compare_validate_molecule(m1: Molecule, m2: Molecule) -> bool:
    """
    Validates, and then compares molecules

    Molecules get validated when added to the server, so if we are comparing
    molecules, we often need to validate the input as well.
    """

    m1_v = Molecule(**m1.model_dump(), validate=True)
    m2_v = Molecule(**m2.model_dump(), validate=True)
    return m1_v == m2_v
