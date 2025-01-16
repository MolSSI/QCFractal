from __future__ import annotations

import inspect
import logging
import select as io_select
import traceback
import uuid
from datetime import datetime, timedelta
from operator import attrgetter
from socket import gethostname
from typing import TYPE_CHECKING

import psycopg2.extensions
from sqlalchemy import select, delete, update, and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from qcfractal.components.auth.db_models import UserIDMapSubquery
from qcfractal.db_socket.helpers import get_query_proj_options
from qcportal.exceptions import MissingDataError
from qcportal.internal_jobs.models import InternalJobStatusEnum, InternalJobQueryFilters
from qcportal.serialization import encode_to_json
from qcportal.utils import now_at_utc
from .db_models import InternalJobORM
from .status import JobProgress, CancelledJobException, JobRunnerStoppingException

if TYPE_CHECKING:
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Dict, Any
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Dict, Optional, Any, List

_default_error = {"error_type": "not_supplied", "error_message": "No error message found on task."}


class InternalJobSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self._hostname = gethostname()

        # How often to update progress (in seconds)
        # Hardcoded for now
        self._update_frequency = 5

        # Old internal job cleanup
        self._delete_internal_job_frequency = 60 * 60 * 24  # one day
        self._internal_job_keep = root_socket.qcf_config.internal_job_keep

        if self._internal_job_keep > 0:
            with self.root_socket.session_scope() as session:
                self.add(
                    "delete_old_internal_jobs",
                    now_at_utc() + timedelta(seconds=2.0),
                    "internal_jobs.delete_old_internal_jobs",
                    {},
                    user_id=None,
                    unique_name=True,
                    repeat_delay=self._delete_internal_job_frequency,
                    session=session,
                )

    def add(
        self,
        name: str,
        scheduled_date: datetime,
        function: str,
        kwargs: Dict[str, Any],
        user_id: Optional[int],
        unique_name: bool = False,
        after_function: Optional[str] = None,
        after_function_kwargs: Optional[Dict[str, Any]] = None,
        repeat_delay: Optional[int] = None,
        serial_group: Optional[str] = None,
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Adds a new job to the internal job queue

        Parameters
        ----------
        name
            Descriptive name of the job
        scheduled_date
            When the job should be run. If the server is not running when this time elapses, it will
            run when it comes back up.
        function
            The function to run, as a string. Should be a member of the sqlalchemy socket.
            Example: `services.iterate_services`
        kwargs
            Arguments to pass to the function
        user_id
            The user making creating this job
        unique_name
            If true, do not add if a job with that name already exists in the job queue.
        after_function
            When this job is done, call this function
        after_function_kwargs
            Arguments to use when calling `after_function`
        repeat_delay
            If set, will submit a new, identical job to be run repeat_delay seconds after this one finishes
        serial_group
            Only one job within this group may be run at once. If None, there is no limit
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A unique ID representing this job in the queue
        """

        with self.root_socket.optional_session(session) as session:
            if unique_name:
                stmt = insert(InternalJobORM)
                stmt = stmt.values(
                    name=name,
                    status=InternalJobStatusEnum.waiting,
                    unique_name=name,
                    scheduled_date=scheduled_date,
                    function=function,
                    kwargs=kwargs,
                    after_function=after_function,
                    after_function_kwargs=after_function_kwargs,
                    repeat_delay=repeat_delay,
                    serial_group=serial_group,
                    user_id=user_id,
                )
                stmt = stmt.on_conflict_do_update(
                    constraint="ux_internal_jobs_unique_name",
                    set_={
                        "after_function": after_function,
                        "after_function_kwargs": after_function_kwargs,
                        "repeat_delay": repeat_delay,
                    },
                )
                stmt = stmt.returning(InternalJobORM.id)
                job_id = session.execute(stmt).scalar_one_or_none()

                if job_id is not None:
                    self._logger.debug(f"Job with name {name} added or updated - id: {job_id}")
                else:
                    # Nothing was returned, meaning nothing was inserted
                    self._logger.debug(f"Job with name {name} already found. Not adding?")
                    stmt = select(InternalJobORM.id).where(InternalJobORM.unique_name == name)
                    job_id = session.execute(stmt).scalar_one_or_none()

            else:
                job_orm = InternalJobORM(
                    name=name,
                    status=InternalJobStatusEnum.waiting,
                    scheduled_date=scheduled_date,
                    function=function,
                    kwargs=kwargs,
                    after_function=after_function,
                    after_function_kwargs=after_function_kwargs,
                    repeat_delay=repeat_delay,
                    serial_group=serial_group,
                    user_id=user_id,
                )
                if unique_name:
                    job_orm.unique_name = name

                session.add(job_orm)
                session.flush()
                job_id = job_orm.id

        return job_id

    def get(self, job_id: int, *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Obtain all the information about a job

        An exception is raised if a job with the given ID doesn't exist

        Parameters
        ----------
        job_id
            ID representing the job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Information about the job (as a dictionary)
        """
        stmt = select(InternalJobORM).where(InternalJobORM.id == job_id)
        with self.root_socket.optional_session(session) as session:
            job_orm = session.execute(stmt).scalar_one_or_none()
            if job_orm is None:
                raise MissingDataError(f"Internal job with id={job_id} not found")

            return job_orm.model_dict()

    def query(self, query_data: InternalJobQueryFilters, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        General query of internal jobs in the database

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        query_data
            Fields/filters to query for
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of job info (as dictionaries) that were found in the database.
        """

        proj_options = get_query_proj_options(InternalJobORM, query_data.include, query_data.exclude)

        stmt = select(InternalJobORM)

        and_query = []
        if query_data.job_id is not None:
            and_query.append(InternalJobORM.id.in_(query_data.job_id))
        if query_data.name is not None:
            and_query.append(InternalJobORM.name.in_(query_data.name))
        if query_data.runner_hostname is not None:
            and_query.append(InternalJobORM.runner_hostname.in_(query_data.runner_hostname))
        if query_data.status is not None:
            and_query.append(InternalJobORM.status.in_(query_data.status))
        if query_data.last_updated_before is not None:
            and_query.append(InternalJobORM.last_updated < query_data.last_updated_before)
        if query_data.last_updated_after is not None:
            and_query.append(InternalJobORM.last_updated > query_data.last_updated_after)
        if query_data.added_before is not None:
            and_query.append(InternalJobORM.added_date < query_data.added_before)
        if query_data.added_after is not None:
            and_query.append(InternalJobORM.added_date > query_data.added_after)
        if query_data.scheduled_before is not None:
            and_query.append(InternalJobORM.scheduled_date < query_data.scheduled_before)
        if query_data.scheduled_after is not None:
            and_query.append(InternalJobORM.scheduled_date > query_data.scheduled_after)
        if query_data.user:
            stmt = stmt.join(UserIDMapSubquery)

            int_ids = {x for x in query_data.user if isinstance(x, int) or x.isnumeric()}
            str_names = set(query_data.user) - int_ids

            and_query.append(or_(UserIDMapSubquery.username.in_(str_names), UserIDMapSubquery.id.in_(int_ids)))

        with self.root_socket.optional_session(session, True) as session:
            stmt = stmt.filter(and_(True, *and_query))
            stmt = stmt.options(*proj_options)

            if query_data.cursor is not None:
                stmt = stmt.where(InternalJobORM.id < query_data.cursor)

            stmt = stmt.order_by(InternalJobORM.id.desc())
            stmt = stmt.limit(query_data.limit)
            stmt = stmt.distinct(InternalJobORM.id)

            results = session.execute(stmt).scalars().all()
            result_dicts = [x.model_dict() for x in results]

        return result_dicts

    def delete(self, job_id: int, *, session: Optional[Session] = None):
        """
        Delete a job from the job queue

        Parameters
        ----------
        job_id
            ID representing the job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        stmt = delete(InternalJobORM).where(InternalJobORM.id == job_id)
        with self.root_socket.optional_session(session) as session:
            session.execute(stmt)

    def delete_old_internal_jobs(self, session: Session) -> None:
        """
        Deletes old internal jobs (as defined by the configuration)
        """

        # we check when adding the job, but double check here
        if self._internal_job_keep <= 0:
            return

        before = now_at_utc() - timedelta(seconds=self._internal_job_keep)

        stmt = delete(InternalJobORM)
        stmt = stmt.where(
            InternalJobORM.status.in_(
                (InternalJobStatusEnum.complete, InternalJobStatusEnum.error, InternalJobStatusEnum.cancelled)
            )
        )
        stmt = stmt.where(InternalJobORM.ended_date < before)
        r = session.execute(stmt)
        num_deleted = r.rowcount
        self._logger.info(f"Deleted {num_deleted} internal jobs before {before}")

    def cancel(self, job_id: int, *, session: Optional[Session] = None):
        """
        Cancels a job in the job queue

        Parameters
        ----------
        job_id
            ID representing the job
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        cancellable = [InternalJobStatusEnum.waiting, InternalJobStatusEnum.running]
        stmt = update(InternalJobORM)
        stmt = stmt.where(InternalJobORM.id == job_id)
        stmt = stmt.where(InternalJobORM.status.in_(cancellable))
        stmt = stmt.values(status=InternalJobStatusEnum.cancelled)

        with self.root_socket.optional_session(session) as session:
            session.execute(stmt)

    def _run_single(self, session: Session, job_orm: InternalJobORM, logger, job_progress: JobProgress):
        """
        Runs a single job
        """

        # For logging (ORM may end up detached or somthing)
        job_id = job_orm.id

        try:
            func_attr = attrgetter(job_orm.function)

            # Function must be part of the sockets
            func = func_attr(self.root_socket)

            # We need to determine the parameters of this function
            func_params = inspect.signature(func).parameters

            add_kwargs = {}

            # If the function has a "job_progress" and/or "session" args, pass those in
            if "job_progress" in func_params:
                add_kwargs["job_progress"] = job_progress

            if "session" in func_params:
                add_kwargs["session"] = session

            # Run the desired function
            # Raises an exception if cancelled
            result = func(**job_orm.kwargs, **add_kwargs)

            job_orm.status = InternalJobStatusEnum.complete
            job_orm.progress = 100
            job_orm.progress_description = "Complete"
            job_orm.result = encode_to_json(result)

        # Job itself is being cancelled
        except CancelledJobException:
            session.rollback()
            logger.info(f"Job {job_id} was cancelled")
            job_orm.status = InternalJobStatusEnum.cancelled
            job_orm.result = None

        # Runner is stopping down
        except JobRunnerStoppingException:
            session.rollback()
            logger.info(f"Job {job_id} was running, but runner is stopping")

            # Basically reset everything
            job_orm.status = InternalJobStatusEnum.waiting
            job_orm.progress = 0
            job_orm.progress_description = None
            job_orm.started_date = None
            job_orm.last_updated = None
            job_orm.result = None
            job_orm.runner_uuid = None

        # Function itself had an error
        except:
            session.rollback()
            job_orm.result = traceback.format_exc()
            logger.error(f"Job {job_id} failed with exception:\n{job_orm.result}")
            job_orm.status = InternalJobStatusEnum.error

        if job_progress.deleted:
            # Row does not exist anymore
            session.expunge(job_orm)
        else:
            # If status is waiting, that means the runner itself is stopping or something
            if job_orm.status != InternalJobStatusEnum.waiting:
                job_orm.ended_date = now_at_utc()
                job_orm.last_updated = job_orm.ended_date

                # Clear the unique name so we can add another one if needed
                has_unique_name = job_orm.unique_name is not None
                job_orm.unique_name = None

            # Flush but don't commit. This will prevent marking the task as finished
            # before the after_func has been run, but allow new ones to be added
            # with unique_name = True
            session.flush()

            # Run the function specified to be run after
            if job_orm.status == InternalJobStatusEnum.complete and job_orm.after_function is not None:
                try:
                    after_func_attr = attrgetter(job_orm.after_function)
                    after_func = after_func_attr(self.root_socket)

                    after_func_params = inspect.signature(after_func).parameters
                    add_after_kwargs = {}
                    if "session" in after_func_params:
                        add_after_kwargs["session"] = session
                    after_func(**job_orm.after_function_kwargs, **add_after_kwargs)
                except Exception:
                    # Don't rollback? not sure what to do here
                    result = traceback.format_exc()
                    logger.error(f"Job {job_orm.id} failed with exception:\n{result}")

                    job_orm.status = InternalJobStatusEnum.error

            if job_orm.status == InternalJobStatusEnum.complete and job_orm.repeat_delay is not None:
                self.add(
                    name=job_orm.name,
                    scheduled_date=now_at_utc() + timedelta(seconds=job_orm.repeat_delay),
                    function=job_orm.function,
                    kwargs=job_orm.kwargs,
                    user_id=job_orm.user_id,
                    unique_name=has_unique_name,
                    after_function=job_orm.after_function,
                    after_function_kwargs=job_orm.after_function_kwargs,
                    repeat_delay=job_orm.repeat_delay,
                    session=session,
                )

        session.commit()

    @staticmethod
    def _wait_for_job(session: Session, logger, conn, end_event):
        """
        Blocks until a job is possibly available to run
        """

        serial_cte = select(InternalJobORM.serial_group).distinct()
        serial_cte = serial_cte.where(InternalJobORM.status == InternalJobStatusEnum.running)
        serial_cte = serial_cte.where(InternalJobORM.serial_group.is_not(None))
        serial_cte = serial_cte.cte()

        next_job_stmt = select(InternalJobORM.scheduled_date)
        next_job_stmt = next_job_stmt.where(InternalJobORM.status == InternalJobStatusEnum.waiting)
        next_job_stmt = next_job_stmt.where(
            or_(InternalJobORM.serial_group.is_(None), InternalJobORM.serial_group.not_in(select(serial_cte)))
        )
        next_job_stmt = next_job_stmt.order_by(InternalJobORM.scheduled_date.asc())

        # Skip any that are being claimed for running right now
        next_job_stmt = next_job_stmt.with_for_update(skip_locked=True, read=True)
        next_job_stmt = next_job_stmt.limit(1)

        cursor = conn.cursor()

        # Start listening for notifications
        cursor.execute("LISTEN check_internal_jobs;")

        while True:
            # Remove any pending notifications
            conn.poll()
            conn.notifies.clear()

            # Find the next available job, and find out if we have to sleep until then
            next_job_time = session.execute(next_job_stmt).scalar_one_or_none()
            now = now_at_utc()
            if next_job_time is None:
                # Wait up to 5 minutes by default. This is just a catch all to prevent
                # programming mistakes from causing infinite waits
                total_to_wait = 300.0
            else:
                total_to_wait = (next_job_time - now).total_seconds()

            session.rollback()  # Release the transaction (and row level lock)

            # If this is <= 0, we don't have to wait
            if total_to_wait <= 0.0:
                logger.debug("not waiting, found possible job to run")
                break

            logger.debug(f"found future job scheduled for {next_job_time}, waiting up to {total_to_wait:.2f} seconds")

            # This will end either if we have waited long enough, or there
            # is a notification from postgres (this connection has run LISTEN)
            total_waited = 0.0

            # Wait in 2 second intervals (to check for end_event)
            while total_waited < total_to_wait:
                to_wait = min(total_to_wait - total_waited, 2.0)
                if to_wait <= 0.0:
                    break

                # waits until a notification is received, up to 5 seconds
                # https://docs.python.org/3/library/select.html#select.select
                try:
                    if io_select.select([conn], [], [], to_wait) != ([], [], []):
                        # We got a notification
                        logger.debug("received notification from check_internal_jobs")
                        conn.poll()

                        # We don't actually care about the individual notifications
                        conn.notifies.clear()

                        # Go back to the outer loop
                        break
                    else:
                        # select timed out. Check for event (we should shut down)
                        if end_event.is_set():
                            break
                        total_waited += to_wait
                except KeyboardInterrupt:
                    break

            if end_event.is_set():
                break

        # Stop listening for insertions on internal_jobs
        cursor.execute("UNLISTEN check_internal_jobs;")
        cursor.close()

    def run_loop(self, end_event):
        """
        Runs in a infinite loop, checking for jobs and running them

        Parameters
        ----------
        end_event
            An event (threading.Event, multiprocessing.Event) that, when set, will
            stop this loop
        """

        # give this loop a unique uuid
        runner_uuid = str(uuid.uuid4())

        # Get a uuid-specific logger
        logger = logging.getLogger(f"internal_job_runner:{runner_uuid}")

        # Two sessions - one for the object, and one for the job status object
        session_main = self.root_socket.Session()
        session_status = self.root_socket.Session()

        # Set up the listener for postgres. This will be notified when something is
        # added to the internal job queue
        # We use a raw psycopg2 connection; sqlalchemy doesn't directly support LISTEN/NOTIFY
        conn = self.root_socket.engine.raw_connection()
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        # Prepare a statement for finding jobs. Filters will be added in the loop
        serial_cte = select(InternalJobORM.serial_group).distinct()
        serial_cte = serial_cte.where(InternalJobORM.status == InternalJobStatusEnum.running)
        serial_cte = serial_cte.where(InternalJobORM.serial_group.is_not(None))
        serial_cte = serial_cte.cte()

        stmt = select(InternalJobORM)
        stmt = stmt.order_by(InternalJobORM.scheduled_date.asc()).limit(1)
        stmt = stmt.with_for_update(skip_locked=True)

        while True:
            if end_event.is_set():
                logger.info("shutting down")
                break

            logger.debug("checking for jobs")

            # Pick up anything waiting, or anything that hasn't been updated in a while (12 update periods)

            now = now_at_utc()
            dead = now - timedelta(seconds=(self._update_frequency * 12))
            logger.debug(f"checking for jobs before date {now}")

            # Job is waiting, schedule to be run now or in the past,
            # Serial group does not have any running or is not set
            cond1 = and_(
                InternalJobORM.status == InternalJobStatusEnum.waiting,
                InternalJobORM.scheduled_date <= now,
                or_(InternalJobORM.serial_group.is_(None), InternalJobORM.serial_group.not_in(select(serial_cte))),
            )

            # Job is running but runner is determined to be dead. Serial group doesn't matter
            cond2 = and_(InternalJobORM.status == InternalJobStatusEnum.running, InternalJobORM.last_updated < dead)

            stmt_now = stmt.where(or_(cond1, cond2))
            job_orm = session_main.execute(stmt_now).scalar_one_or_none()

            # If no job was found, wait for one
            if job_orm is None:
                session_main.rollback()  # release the transaction
                logger.debug("no jobs found")
                self._wait_for_job(session_main, logger, conn, end_event)

                if end_event.is_set():
                    logger.info("shutting down")
                    break
                else:
                    continue

            if end_event.is_set():
                logger.info("shutting down")
                break

            logger.info(f"running job {job_orm.name} id={job_orm.id} scheduled_date={job_orm.scheduled_date}")
            job_orm.started_date = now_at_utc()
            job_orm.last_updated = now_at_utc()
            job_orm.runner_hostname = self._hostname
            job_orm.runner_uuid = runner_uuid
            job_orm.status = InternalJobStatusEnum.running

            # For logging below - the object might be in an odd state after the exception, where accessing
            # it results in further exceptions
            serial_group = job_orm.serial_group

            # Violation of the unique constraint may occur - two runners attempting to take
            # different jobs of the same serial group at the same time
            try:
                # Releases the row-level lock (from the with_for_update() in the original query)
                session_main.commit()
            except IntegrityError:
                logger.info(
                    f"Attempting to run job from serial group '{serial_group}, but seems like another runner got to another job from the same group first"
                )
                session_main.rollback()
                continue

            job_progress = JobProgress(job_orm.id, runner_uuid, session_status, self._update_frequency, end_event)
            self._run_single(session_main, job_orm, logger, job_progress=job_progress)

            # Stop the updating thread and cleanup
            job_progress.stop()

        session_main.close()
        session_status.close()
        conn.close()
