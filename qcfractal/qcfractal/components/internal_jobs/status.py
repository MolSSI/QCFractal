from __future__ import annotations

import threading
import weakref
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcportal.internal_jobs.models import InternalJobStatusEnum
from qcportal.utils import now_at_utc


class CancelledJobException(Exception):
    pass


class JobRunnerStoppingException(Exception):
    pass


class JobProgress:
    """
    Functor for updating progress and cancelling internal jobs
    """

    def __init__(self, job_id: int, runner_uuid: str, session: Session, update_frequency: int, end_event):
        self._update_frequency = update_frequency
        self._job_id = job_id
        self._runner_uuid = runner_uuid
        # Only ever write to the row while this runner still owns it and the job is still running.
        # Once it has been cancelled, deleted, or taken over by another runner (which reports its own
        # progress), this must not touch it - checking afterwards would be too late
        self._stmt = (
            update(InternalJobORM)
            .where(
                InternalJobORM.id == self._job_id,
                InternalJobORM.runner_uuid == self._runner_uuid,
                InternalJobORM.status == InternalJobStatusEnum.running,
            )
            .returning(InternalJobORM.id)
        )
        self._exists_stmt = select(InternalJobORM.id).where(InternalJobORM.id == self._job_id)
        self._progress = 0
        self._description = None

        self._cancelled = False
        self._runner_ending = False
        self._deleted = False

        self._end_event = end_event

        # A thread for periodically updating
        # _th_cancel is set when we want the updating thread to end
        self._th_cancel = threading.Event()
        self._th = threading.Thread(target=self._update_thread, args=(session, self._th_cancel))

        # Start the updating thread
        self._th.start()

        # Create the finalizer that will close the updating thread
        self._finalizer = weakref.finalize(self, self._stop_thread, self._th_cancel, self._th)

    def _update_thread(self, session: Session, end_thread: threading.Event):
        while True:
            # Update progress
            stmt = self._stmt.values(
                progress=self._progress, progress_description=self._description, last_updated=now_at_utc()
            )
            ret = session.execute(stmt).one_or_none()
            session.commit()

            if ret is None:
                # No longer ours to update - deleted, cancelled, or taken over by another runner.
                # There is nothing more for this thread to do, and it must not keep writing
                self._deleted = session.execute(self._exists_stmt).one_or_none() is None
                session.rollback()
                self._cancelled = True
                break

            # Are we ending/cancelling because the runner is stopping/closing?
            if self._end_event.is_set():
                self._runner_ending = True

            cancel = end_thread.wait(self._update_frequency)
            if cancel is True:
                break

    @staticmethod
    def _stop_thread(cancel_event: threading.Event, thread: threading.Thread):
        ####################################################################################
        # This is written as a class method so that it can be called by a weakref finalizer
        ####################################################################################

        cancel_event.set()
        thread.join()

    def stop(self):
        self._finalizer()

    def update_progress(self, progress: int, description: Optional[str] = None):
        self._progress = progress
        self._description = description

    @property
    def cancelled(self) -> bool:
        return self._cancelled or self._deleted

    @property
    def runner_ending(self) -> bool:
        return self._runner_ending

    @property
    def deleted(self) -> bool:
        return self._deleted

    def raise_if_cancelled(self):
        if self._cancelled or self._deleted:
            raise CancelledJobException("Job was cancelled or deleted")

        if self._runner_ending:
            raise JobRunnerStoppingException("Job runner is stopping/ending")


def raise_if_cancelled(job_progress: Optional[JobProgress]):
    """
    Raises a CancelledJobExcepion if job_progress exists and is in a cancelled or deleted state
    """

    if job_progress is not None:
        job_progress.raise_if_cancelled()
