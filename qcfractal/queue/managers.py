"""
Queue backend abstraction manager.
"""

import json
import logging
import sched
import socket
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import BaseModel, validator

import qcengine as qcng
from qcfractal.extras import get_information

from ..interface.data import get_molecule
from .adapters import build_queue_adapter
from .compress import compress_results

__all__ = ["QueueManager"]


class QueueStatistics(BaseModel):
    """
    Queue Manager Job statistics
    """

    # Dynamic quantities
    total_successful_tasks: int = 0
    total_failed_tasks: int = 0
    total_worker_walltime: float = 0.0
    total_task_walltime: float = 0.0
    maximum_possible_walltime: float = 0.0  # maximum_workers * time_delta, experimental
    active_task_slots: int = 0

    # Static Quantities
    max_concurrent_tasks: int = 0
    cores_per_task: int = 0
    memory_per_task: float = 0.0
    last_update_time: float = None

    def __init__(self, **kwargs):
        if kwargs.get("last_update_time", None) is None:
            kwargs["last_update_time"] = time.time()
        super().__init__(**kwargs)

    @property
    def total_completed_tasks(self) -> int:
        return self.total_successful_tasks + self.total_failed_tasks

    @property
    def theoretical_max_consumption(self) -> float:
        """In Core Hours"""
        return self.max_concurrent_tasks * self.cores_per_task * (time.time() - self.last_update_time) / 3600

    @property
    def active_cores(self) -> int:
        return self.active_task_slots * self.cores_per_task

    @property
    def active_memory(self) -> float:
        return self.active_task_slots * self.memory_per_task

    @validator("cores_per_task", pre=True)
    def cores_per_tasks_none(cls, v):
        if v is None:
            v = 1
        return v


class QueueManager:
    """
    This object maintains a computational queue and watches for finished tasks for different
    queue backends. Finished tasks are added to the database and removed from the queue.

    Attributes
    ----------
    client : FractalClient
        A FractalClient connected to a server.
    queue_adapter : QueueAdapter
        The DBAdapter class for queue abstraction
    errors : dict
        A dictionary of current errors
    logger : logging.logger. Optional, Default: None
        A logger for the QueueManager
    """

    def __init__(
        self,
        client: "FractalClient",
        queue_client: "BaseAdapter",
        logger: Optional[logging.Logger] = None,
        max_tasks: int = 200,
        queue_tag: Optional[Union[str, List[str]]] = None,
        manager_name: str = "unlabeled",
        update_frequency: Union[int, float] = 2,
        verbose: bool = True,
        server_error_retries: Optional[int] = 1,
        stale_update_limit: Optional[int] = 10,
        cores_per_task: Optional[int] = None,
        memory_per_task: Optional[float] = None,
        nodes_per_task: Optional[int] = None,
        cores_per_rank: Optional[int] = 1,
        scratch_directory: Optional[str] = None,
        retries: Optional[int] = 2,
        configuration: Optional[Dict[str, Any]] = None,
    ):
        """
        Parameters
        ----------
        client : FractalClient
            A FractalClient connected to a server
        queue_client : BaseAdapter
            The DBAdapter class for queue abstraction
        logger : Optional[logging.Logger], optional
            A logger for the QueueManager
        max_tasks : int, optional
            The maximum number of tasks to hold at any given time
        queue_tag : str, optional
            Allows managers to pull from specific tags
        manager_name : str, optional
            The cluster the manager belongs to
        update_frequency : Union[int, float], optional
            The frequency to check for new tasks in seconds
        verbose : bool, optional
            Whether or not to have the manager be verbose (logger level debug and up)
        server_error_retries : Optional[int], optional
            How many times finished jobs are attempted to be pushed to the server in
            in the event of a server communication error.
            After number of attempts, the failed jobs are dropped from this manager and considered "stale"
            Set to `None` to keep retrying
        stale_update_limit : Optional[int], optional
            Number of stale update attempts to keep around
            If this limit is ever hit, the server initiates as shutdown as best it can
            since communication with the server has gone wrong too many times.
            Set to `None` for unlimited
        cores_per_task : Optional[int], optional
            How many CPU cores per computation task to allocate for QCEngine
            None indicates "use however many you can detect"
        memory_per_task : Optional[float], optional
            How much memory, in GiB, per computation task to allocate for QCEngine
            None indicates "use however much you can consume"
        nodes_per_task : Optional[int], optional
            How many nodes to use per task. Used only for node-parallel tasks
        cores_per_rank: Optional[int], optional
            How many CPUs per rank of an MPI application. Used only for node-parallel tasks
        scratch_directory : Optional[str], optional
            Scratch directory location to do QCEngine compute
            None indicates "wherever the system default is"'
        retries : Optional[int], optional
            Number of retries that QCEngine will attempt for RandomErrors detected when running
            its computations. After this many attempts (or on any other type of error), the
            error will be raised.
        configuration : Optional[Dict[str, Any]], optional
            A JSON description of the settings used to create this object for the database.
        """

        # Setup logging
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger("QueueManager")

        self.name_data = {"cluster": manager_name, "hostname": socket.gethostname(), "uuid": str(uuid.uuid4())}
        self._name = self.name_data["cluster"] + "-" + self.name_data["hostname"] + "-" + self.name_data["uuid"]

        self.client = client
        self.cores_per_task = cores_per_task
        self.memory_per_task = memory_per_task
        self.nodes_per_task = nodes_per_task or 1
        self.scratch_directory = scratch_directory
        self.retries = retries
        self.cores_per_rank = cores_per_rank
        self.configuration = configuration
        self.queue_adapter = build_queue_adapter(
            queue_client,
            logger=self.logger,
            cores_per_task=self.cores_per_task,
            memory_per_task=self.memory_per_task,
            nodes_per_task=self.nodes_per_task,
            scratch_directory=self.scratch_directory,
            cores_per_rank=self.cores_per_rank,
            retries=self.retries,
            verbose=verbose,
        )
        self.max_tasks = max_tasks
        self.queue_tag = queue_tag
        self.verbose = verbose

        self.statistics = QueueStatistics(
            max_concurrent_tasks=self.max_tasks,
            cores_per_task=(cores_per_task or 0),
            memory_per_task=(memory_per_task or 0),
            update_frequency=update_frequency,
        )

        self.scheduler = None
        self.update_frequency = update_frequency
        self.periodic = {}
        self.active = 0
        self.exit_callbacks = []

        # Server response/stale job handling
        self.server_error_retries = server_error_retries
        self.stale_update_limit = stale_update_limit
        self._stale_updates_tracked = 0
        self._stale_payload_tracking = []
        self.n_stale_jobs = 0

        # QCEngine data
        self.available_programs = qcng.list_available_programs()
        self.available_procedures = qcng.list_available_procedures()

        # Display a warning if there are non-node-parallel programs and >1 node_per_task
        if self.nodes_per_task > 1:
            for name in self.available_programs:
                program = qcng.get_program(name)
                if not program.node_parallel:
                    self.logger.warning(
                        "Program {} is not node parallel," " but manager will use >1 node per task".format(name)
                    )

        # Print out configuration
        self.logger.info("QueueManager:")
        self.logger.info("    Version:         {}\n".format(get_information("version")))

        if self.verbose:
            self.logger.info("    Name Information:")
            self.logger.info("        Cluster:     {}".format(self.name_data["cluster"]))
            self.logger.info("        Hostname:    {}".format(self.name_data["hostname"]))
            self.logger.info("        UUID:        {}\n".format(self.name_data["uuid"]))

        self.logger.info("    Queue Adapter:")
        self.logger.info("        {}\n".format(self.queue_adapter))

        if self.verbose:
            self.logger.info("    QCEngine:")
            self.logger.info("        Version:        {}".format(qcng.__version__))
            self.logger.info("        Task Cores:     {}".format(self.cores_per_task))
            self.logger.info("        Task Mem:       {}".format(self.memory_per_task))
            self.logger.info("        Task Nodes:     {}".format(self.nodes_per_task))
            self.logger.info("        Cores per Rank: {}".format(self.cores_per_rank))
            self.logger.info("        Scratch Dir:    {}".format(self.scratch_directory))
            self.logger.info("        Programs:       {}".format(self.available_programs))
            self.logger.info("        Procedures:     {}\n".format(self.available_procedures))

        # DGAS Note: Note super happy about how this if/else turned out. Looking for alternatives.
        if self.connected():
            # Pull server info
            self.server_info = client.server_information()
            self.server_name = self.server_info["name"]
            self.server_version = self.server_info["version"]
            self.server_query_limit = self.server_info["query_limit"]
            if self.max_tasks > self.server_query_limit:
                self.max_tasks = self.server_query_limit
                self.logger.warning(
                    "Max tasks was larger than server query limit of {}, reducing to match query limit.".format(
                        self.server_query_limit
                    )
                )
            self.heartbeat_frequency = self.server_info["heartbeat_frequency"]

            # Tell the server we are up and running
            payload = self._payload_template()
            payload["data"]["operation"] = "startup"
            payload["data"]["configuration"] = self.configuration

            self.client._automodel_request("queue_manager", "put", payload)

            if self.verbose:
                self.logger.info("    Connected:")
                self.logger.info("        Version:     {}".format(self.server_version))
                self.logger.info("        Address:     {}".format(self.client.address))
                self.logger.info("        Name:        {}".format(self.server_name))
                self.logger.info("        Queue tag:   {}".format(self.queue_tag))
                self.logger.info("        Username:    {}\n".format(self.client.username))

        else:
            self.logger.info("    QCFractal server information:")
            self.logger.info("        Not connected, some actions will not be available")

    def _payload_template(self):
        meta = {
            **self.name_data.copy(),
            # Version info
            "qcengine_version": qcng.__version__,
            "manager_version": get_information("version"),
            # User info
            "username": self.client.username,
            # Pull info
            "programs": self.available_programs,
            "procedures": self.available_procedures,
            "tag": self.queue_tag,
            # Statistics
            "total_worker_walltime": self.statistics.total_worker_walltime,
            "total_task_walltime": self.statistics.total_task_walltime,
            "active_tasks": self.statistics.active_task_slots,
            "active_cores": self.statistics.active_cores,
            "active_memory": self.statistics.active_memory,
        }

        return {"meta": meta, "data": {}}

    ## Accessors

    def name(self) -> str:
        """
        Returns the Managers full name.
        """
        return self._name

    def connected(self) -> bool:
        """
        Checks the connection to the server.
        """
        return self.client is not None

    def assert_connected(self) -> None:
        """
        Raises an error for functions that require a server connection.
        """
        if self.connected() is False:
            raise AttributeError("Manager is not connected to a server, this operations is not available.")

    ## Start/stop functionality

    def start(self) -> None:
        """
        Starts up all IOLoops and processes.
        """

        self.assert_connected()

        self.scheduler = sched.scheduler(time.time, time.sleep)
        heartbeat_time = int(0.4 * self.heartbeat_frequency)

        def scheduler_update():
            self.update()
            self.scheduler.enter(self.update_frequency, 1, scheduler_update)

        def scheduler_heartbeat():
            self.heartbeat()
            self.scheduler.enter(heartbeat_time, 1, scheduler_heartbeat)

        self.logger.info("QueueManager successfully started.\n")

        self.scheduler.enter(0, 1, scheduler_update)
        self.scheduler.enter(0, 2, scheduler_heartbeat)

        self.scheduler.run()

    def stop(self, signame="Not provided", signum=None, stack=None) -> None:
        """
        Shuts down all IOLoops and periodic updates.
        """
        self.logger.info("QueueManager received shutdown signal: {}.\n".format(signame))

        # Cancel all events
        if self.scheduler is not None:
            for event in self.scheduler.queue:
                self.scheduler.cancel(event)

        # Push data back to the server
        self.shutdown()

        # Close down the adapter
        self.close_adapter()

        # Call exit callbacks
        for func, args, kwargs in self.exit_callbacks:
            func(*args, **kwargs)

        self.logger.info("QueueManager stopping gracefully.\n")

    def close_adapter(self) -> bool:
        """
        Closes down the underlying adapter.
        """

        return self.queue_adapter.close()

    ## Queue Manager functions

    def heartbeat(self) -> None:
        """
        Provides a heartbeat to the connected Server.
        """

        self.assert_connected()

        payload = self._payload_template()
        payload["data"]["operation"] = "heartbeat"
        try:
            self.client._automodel_request("queue_manager", "put", payload)
            self.logger.debug("Heartbeat was successful.")
        except IOError:
            self.logger.warning("Heartbeat was not successful.")

    def shutdown(self) -> Dict[str, Any]:
        """
        Shutdown the manager and returns tasks to queue.
        """
        self.assert_connected()

        self.update(new_tasks=False, allow_shutdown=False)

        payload = self._payload_template()
        payload["data"]["operation"] = "shutdown"
        try:
            response = self.client._automodel_request("queue_manager", "put", payload, timeout=5)
            response["success"] = True

            shutdown_string = "Shutdown was successful, {} tasks returned to master queue."

        except IOError:
            # TODO something as we didnt successfully add the data
            self.logger.warning("Shutdown was not successful. This may delay queued tasks.")
            response = {"nshutdown": 0, "success": False}
            shutdown_string = "Shutdown was not successful, {} tasks not returned."

        nshutdown = response["nshutdown"]
        if self.n_stale_jobs:
            shutdown_string = shutdown_string.format(
                f"{min(0, nshutdown-self.n_stale_jobs)} active and {nshutdown} stale"
            )
        else:
            shutdown_string = shutdown_string.format(nshutdown)

        self.logger.info(shutdown_string)

        response["info"] = shutdown_string
        return response

    def add_exit_callback(self, callback: Callable, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        """Adds additional callbacks to perform when closing down the server.

        Parameters
        ----------
        callback : callable
            The function to call at exit
        *args
            Arguments to call with the function.
        **kwargs
            Kwargs to call with the function.

        """
        self.exit_callbacks.append((callback, args, kwargs))

    def _post_update(self, payload_data, allow_shutdown=True):
        """Internal function to post payload update"""
        payload = self._payload_template()
        # Update with data
        payload["data"] = payload_data
        try:
            self.client._automodel_request("queue_manager", "post", payload, full_return=True)
        except IOError:

            # Trapped behavior elsewhere
            raise

        except Exception as fatal:
            # Non IOError, something has gone very wrong
            self.logger.error(
                "An error was detected which was not an expected requests-type error. The manager "
                "will attempt shutdown as best it can. Please report this error to the QCFractal "
                "developers as this block should not be "
                "seen outside of debugging modes. Error is as follows\n{}".format(fatal)
            )

            try:
                if allow_shutdown:
                    self.shutdown()
            finally:
                raise fatal

    def _update_stale_jobs(self, allow_shutdown=True):
        """
        Attempt to post the previous payload failures
        """
        clear_indices = []
        for index, (results, attempts) in enumerate(self._stale_payload_tracking):
            try:
                self._post_update(results)
                self.logger.info(f"Successfully pushed jobs from {attempts+1} updates ago")
                self.logger.info(f"Tasks pushed: " + str(list(results.keys())))
                clear_indices.append(index)
            except IOError:

                # Tried and failed
                attempts += 1
                # Case: Still within the retry limit
                if self.server_error_retries is None or self.server_error_retries > attempts:
                    self._stale_payload_tracking[index][-1] = attempts
                    self.logger.warning(f"Could not post jobs from {attempts} updates ago, will retry on next update.")

                # Case: Over limit
                else:
                    self.logger.warning(
                        f"Could not post jobs from {attempts} ago and over attempt limit, marking " f"jobs as stale."
                    )
                    self.n_stale_jobs += len(results)
                    clear_indices.append(index)
                    self._stale_updates_tracked += 1

        # Cleanup clear indices
        for index in clear_indices[::-1]:
            self._stale_payload_tracking.pop(index)

        # Check stale limiters
        if (
            self.stale_update_limit is not None
            and (len(self._stale_payload_tracking) + self._stale_updates_tracked) > self.stale_update_limit
        ):
            self.logger.error("Exceeded number of stale updates allowed! Attempting to shutdown gracefully...")

            # Log all not-quite stale jobs to stale
            for (results, _) in self._stale_payload_tracking:
                self.n_stale_jobs += len(results)
            try:
                if allow_shutdown:
                    self.shutdown()
            finally:
                raise RuntimeError("Exceeded number of stale updates allowed!")

    def update(self, new_tasks: bool = True, allow_shutdown=True) -> bool:
        """Examines the queue for completed tasks and adds successful completions to the database
        while unsuccessful are logged for future inspection.

        Parameters
        ----------
        new_tasks: bool, optional, Default: True
            Try to get new tasks from the server
        allow_shutdown: bool, optional, Default: True
            Allow function to attempt graceful shutdowns in the case of stale job or fatal error limits.
            Does not prevent errors from being raise, but mostly used to prevent infinite loops when update is
            called from `shutdown` itself
        """

        self.assert_connected()
        self._update_stale_jobs(allow_shutdown=allow_shutdown)

        results = self.queue_adapter.acquire_complete()

        # Compress the stdout/stderr/error outputs
        results = compress_results(results)

        # Stats fetching for running tasks, as close to the time we got the jobs as we can
        last_time = self.statistics.last_update_time
        now = self.statistics.last_update_time = time.time()
        time_delta_seconds = now - last_time

        try:
            self.statistics.active_task_slots = self.queue_adapter.count_active_task_slots()
            log_efficiency = True
        except NotImplementedError:
            log_efficiency = False

        timedelta_worker_walltime = time_delta_seconds * self.statistics.active_cores / 3600
        timedelta_maximum_walltime = (
            time_delta_seconds * self.statistics.max_concurrent_tasks * self.statistics.cores_per_task / 3600
        )
        self.statistics.total_worker_walltime += timedelta_worker_walltime
        self.statistics.maximum_possible_walltime += timedelta_maximum_walltime

        # Process jobs
        n_success = 0
        n_fail = 0
        n_result = len(results)
        task_cpu_hours = 0
        error_payload = []

        if n_result:
            # For logging
            failure_messages = {}

            try:
                self._post_update(results, allow_shutdown=allow_shutdown)
                task_status = {k: "sent" for k in results.keys()}
            except IOError:
                if self.server_error_retries is None or self.server_error_retries > 0:
                    self.logger.warning("Post complete tasks was not successful. Attempting again on next update.")
                    self._stale_payload_tracking.append([results, 0])
                    task_status = {k: "deferred" for k in results.keys()}
                else:
                    self.logger.warning("Post complete tasks was not successful. Data may be lost.")
                    self.n_stale_jobs += len(results)
                    task_status = {k: "unknown_error" for k in results.keys()}

            self.active -= n_result
            for key, result in results.items():
                wall_time_seconds = 0
                if result.success:
                    n_success += 1
                    if hasattr(result.provenance, "wall_time"):
                        wall_time_seconds = float(result.provenance.wall_time)

                    task_status[key] += " / success"
                else:
                    task_status[key] += f" / failed: {result.error.error_type}"
                    failure_messages[key] = result.error

                    # Try to get the wall time in the most fault-tolerant way
                    try:
                        wall_time_seconds = float(result.input_data.get("provenance", {}).get("wall_time", 0))
                    except AttributeError:
                        # Trap the result.input_data is None, but let other attribute errors go
                        if result.input_data is None:
                            wall_time_seconds = 0
                        else:
                            raise
                    except TypeError:
                        # Trap wall time corruption, e.g. float(None)
                        # Other Result corruptions will raise an error correctly
                        wall_time_seconds = 0

                task_cpu_hours += wall_time_seconds * self.statistics.cores_per_task / 3600
            n_fail = n_result - n_success

            # Now print out all the info
            self.logger.info(f"Processed {len(results)} tasks: {n_success} succeeded / {n_fail} failed).")
            self.logger.info(f"Task ids, submission status, calculation status below")
            for task_id, status_msg in task_status.items():
                self.logger.info(f"    Task {task_id} : {status_msg}")
            if n_fail:
                self.logger.info("The following tasks failed with the errors:")
                for task_id, error_info in failure_messages.items():
                    self.logger.info(f"Error message for task id {task_id}")
                    self.logger.info("    Error type: " + str(error_info.error_type))
                    self.logger.info("    Backtrace: \n" + str(error_info.error_message))

        open_slots = max(0, self.max_tasks - self.active)

        # Crunch Statistics
        self.statistics.total_failed_tasks += n_fail
        self.statistics.total_successful_tasks += n_success
        self.statistics.total_task_walltime += task_cpu_hours
        na_format = ""
        float_format = ",.2f"
        if self.statistics.total_completed_tasks == 0:
            task_stats_str = "Task statistics unavailable until first tasks return"
            worker_stats_str = None
        else:
            success_rate = self.statistics.total_successful_tasks / self.statistics.total_completed_tasks * 100
            success_format = float_format
            task_stats_str = (
                f"Task Stats: Processed={self.statistics.total_completed_tasks}, "
                f"Failed={self.statistics.total_failed_tasks}, "
                f"Success={success_rate:{success_format}}%"
            )
            worker_stats_str = (
                f"Worker Stats (est.): Core Hours Used={self.statistics.total_worker_walltime:{float_format}}"
            )

            # Handle efficiency calculations
            if log_efficiency:
                # Efficiency calculated as:
                # sum_task(task_wall_time * nthread / task)
                # -------------------------------------------------------------
                if self.statistics.total_task_walltime == 0 or self.statistics.maximum_possible_walltime == 0:
                    efficiency_of_running = "(N/A yet)"
                    efficiency_of_potential = "(N/A yet)"
                    efficiency_format = na_format
                else:
                    efficiency_of_running = (
                        self.statistics.total_task_walltime / self.statistics.total_worker_walltime * 100
                    )
                    efficiency_of_potential = (
                        self.statistics.total_worker_walltime / self.statistics.maximum_possible_walltime * 100
                    )
                    efficiency_format = float_format
                worker_stats_str += f", Core Usage Efficiency: {efficiency_of_running:{efficiency_format}}%"
                if self.verbose:
                    worker_stats_str += (
                        f", Core Usage vs. Max Resources Requested: " f"{efficiency_of_potential:{efficiency_format}}%"
                    )

        self.logger.info(task_stats_str)
        if worker_stats_str is not None:
            self.logger.info(worker_stats_str)

        if (new_tasks is False) or (open_slots == 0):
            return True

        # Get new tasks
        payload = self._payload_template()
        payload["data"]["limit"] = open_slots

        try:
            new_tasks = self.client._automodel_request("queue_manager", "get", payload)
        except IOError:
            # TODO something as we didnt successfully get data
            self.logger.warning("Acquisition of new tasks was not successful.")
            return False

        self.logger.info("Acquired {} new tasks.".format(len(new_tasks)))

        # Add new tasks to queue
        self.queue_adapter.submit_tasks(new_tasks)
        self.active += len(new_tasks)
        return True

    def await_results(self) -> bool:
        """A synchronous method for testing or small launches
        that awaits task completion.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """

        self.assert_connected()

        self.update()
        self.queue_adapter.await_results()
        self.update(new_tasks=False)
        return True

    def list_current_tasks(self) -> List[Any]:
        """Provides a list of tasks currently in the queue along
        with the associated keys.

        Returns
        -------
        ret : list of tuples
            All tasks currently still in the database
        """
        return self.queue_adapter.list_tasks()

    def test(self, n=1) -> bool:
        """
        Tests all known programs with simple inputs to check if the Adapter is correctly instantiated.
        """

        from qcfractal import testing

        self.logger.info("Testing requested, generating tasks")
        task_base = json.dumps(
            {
                "spec": {
                    "function": "qcengine.compute",
                    "args": [
                        {
                            "molecule": get_molecule("hooh.json").dict(encoding="json"),
                            "driver": "energy",
                            "model": {},
                            "keywords": {},
                        },
                        "program",
                    ],
                    "kwargs": {},
                },
                "parser": "single",
            }
        )

        programs = {
            "rdkit": {"method": "UFF", "basis": None},
            "torchani": {"method": "ANI1", "basis": None},
            "psi4": {"method": "HF", "basis": "sto-3g"},
        }
        tasks = []
        found_programs = []

        for program, model in programs.items():
            if testing.has_module(program):
                self.logger.info("Found program {}, adding to testing queue.".format(program))
            else:
                self.logger.warning("Could not find program {}, skipping tests.".format(program))
                continue
            for x in range(n):

                task = json.loads(task_base)
                program_id = program + str(x)
                task["id"] = program_id
                task["spec"]["args"][0]["model"] = model
                task["spec"]["args"][0]["keywords"] = {"e_convergence": (x * 1.0e-6 + 1.0e-6)}
                task["spec"]["args"][1] = program

                tasks.append(task)
                found_programs.append(program_id)

        self.queue_adapter.submit_tasks(tasks)

        self.logger.info("Testing tasks submitting, awaiting results.\n")
        self.queue_adapter.await_results()

        results = self.queue_adapter.acquire_complete()
        self.logger.info("Testing results acquired.")

        missing_programs = results.keys() - set(found_programs)
        if len(missing_programs):
            self.logger.error("Not all tasks were retrieved, missing programs {}.".format(missing_programs))
            raise ValueError("Testing failed, not all tasks were retrieved.")
        else:
            self.logger.info("All tasks retrieved successfully.")

        failures = 0
        fail_report = {}
        for k, result in results.items():
            if result.success:
                self.logger.info("  {} - PASSED".format(k))
            else:
                self.logger.error("  {} - FAILED!".format(k))
                failed_program = "Return Mangled!"  # This should almost never be seen, but is in place as a fallback
                for program in programs.keys():
                    if program in k:
                        failed_program = program
                        break
                if failed_program not in fail_report:
                    fail_report[failed_program] = (
                        f"On test {k}:"
                        f"\nException Type: {result.error.error_type}"
                        f"\nException Message: {result.error.error_message}"
                    )
                failures += 1

        if failures:
            self.logger.error("{}/{} tasks failed!".format(failures, len(results)))
            self.logger.error(
                f"A sample error from each program to help:\n" + "\n".join([e for e in fail_report.values()])
            )
            return False
        else:
            self.logger.info("All tasks completed successfully!")
            return True
