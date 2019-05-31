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

import qcengine as qcng
from qcfractal.extras import get_information

from .adapters import build_queue_adapter
from ..interface.data import get_molecule

__all__ = ["QueueManager"]


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

    def __init__(self,
                 client: Any,
                 queue_client: Any,
                 logger: Optional[logging.Logger] = None,
                 max_tasks: int = 200,
                 queue_tag: str = None,
                 manager_name: str = "unlabled",
                 update_frequency: Union[int, float] = 2,
                 verbose: bool = True,
                 server_error_retries: Optional[int] = 1,
                 stale_update_limit: Optional[int] = 10,
                 cores_per_task: Optional[int] = None,
                 memory_per_task: Optional[Union[int, float]] = None,
                 scratch_directory: Optional[str] = None):
        """
        Parameters
        ----------
        client : FractalClient
            A FractalClient connected to a server
        queue_client : QueueAdapter
            The DBAdapter class for queue abstraction
        logger : logging.Logger, Optional. Default: None
            A logger for the QueueManager
        max_tasks : int
            The maximum number of tasks to hold at any given time
        queue_tag : str
            Allows managers to pull from specific tags
        manager_name : str
            The cluster the manager belongs to
        update_frequency : int
            The frequency to check for new tasks in seconds
        verbose: bool, optional, Default: True
            Whether or not to have the manager be verbose (logger level debug and up)
        server_error_retries: int, optional, Default: 1
            How many times finished jobs are attempted to be pushed to the server in
            in the event of a server communication error.
            After number of attempts, the failed jobs are dropped from this manager and considered "stale"
            Set to `None` to keep retrying
        stale_update_limit: int, optional, Default: 10
            Number of stale update attempts to keep around
            If this limit is ever hit, the server initiates as shutdown as best it can
            since communication with the server has gone wrong too many times.
            Set to `None` for unlimited
        cores_per_task : int, optional, Default: None
            How many CPU cores per computation task to allocate for QCEngine
            None indicates "use however many you can detect"
        memory_per_task: int, optional, Default: None
            How much memory, in GiB, per computation task to allocate for QCEngine
            None indicates "use however much you can consume"
        scratch_directory: str, optional, Default: None
            Scratch directory location to do QCEngine compute
            None indicates "wherever the system default is"'
        """

        # Setup logging
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('QueueManager')

        self.name_data = {"cluster": manager_name, "hostname": socket.gethostname(), "uuid": str(uuid.uuid4())}
        self._name = self.name_data["cluster"] + "-" + self.name_data["hostname"] + "-" + self.name_data["uuid"]

        self.client = client
        self.cores_per_task = cores_per_task
        self.memory_per_task = memory_per_task
        self.scratch_directory = scratch_directory
        self.queue_adapter = build_queue_adapter(queue_client,
                                                 logger=self.logger,
                                                 cores_per_task=self.cores_per_task,
                                                 memory_per_task=self.memory_per_task,
                                                 scratch_directory=self.scratch_directory,
                                                 verbose=verbose)
        self.max_tasks = max_tasks
        self.queue_tag = queue_tag
        self.verbose = verbose

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
            self.logger.info("        Version:     {}".format(qcng.__version__))
            self.logger.info("        Task Cores:  {}".format(self.cores_per_task))
            self.logger.info("        Task Mem:    {}".format(self.memory_per_task))
            self.logger.info("        Scratch Dir: {}".format(self.scratch_directory))
            self.logger.info("        Programs:    {}".format(self.available_programs))
            self.logger.info("        Procedures:  {}\n".format(self.available_procedures))

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
                        self.server_query_limit))
            self.heartbeat_frequency = self.server_info["heartbeat_frequency"]

            # Tell the server we are up and running
            payload = self._payload_template()
            payload["data"]["operation"] = "startup"

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
            "tag": self.queue_tag
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
            self.logger.info("Heartbeat was successful.")
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
            response = self.client._automodel_request("queue_manager", "put", payload, timeout=2)
        except IOError:
            # TODO something as we didnt successfully add the data
            self.logger.warning("Shutdown was not successful. This may delay queued tasks.")
            return {"nshutdown": 0}

        nshutdown = response["nshutdown"]
        shutdown_string = "Shutdown was successful, {} tasks returned to master queue."
        if self.n_stale_jobs:
            shutdown_string = shutdown_string.format(
                f"{min(0, nshutdown-self.n_stale_jobs)} active and {nshutdown} stale")
        else:
            shutdown_string = shutdown_string.format(nshutdown)
        self.logger.info(shutdown_string)
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
            self.client._automodel_request("queue_manager", "post", payload)
        except IOError:

            # Trapped behavior elsewhere
            raise

        except Exception as fatal:
            # Non IOError, something has gone very wrong
            self.logger.error("An error was detected which was not an expected requests-type error. The manager "
                              "will attempt shutdown as best it can. Please report this error to the QCFractal "
                              "developers as this block should not be "
                              "seen outside of debugging modes. Error is as follows\n{}".format(fatal))

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
                clear_indices.append(index)
            except IOError:

                # Tried and failed
                attempts += 1
                # Case: Still within the retry limit
                if self.server_error_retries is None or self.server_error_retries > attempts:
                    self._stale_payload_tracking[index][-1] = attempts
                    self.logger.warning(f"Could not post jobs from {attempts} ago, will retry on next update.")

                # Case: Over limit
                else:
                    self.logger.warning(f"Could not post jobs from {attempts} ago and over attempt limit, marking "
                                        f"jobs as stale.")
                    self.n_stale_jobs += len(results)
                    clear_indices.append(index)
                    self._stale_updates_tracked += 1

        # Cleanup clear indices
        for index in clear_indices[::-1]:
            self._stale_payload_tracking.pop(index)

        # Check stale limiters
        if self.stale_update_limit is not None and (len(self._stale_payload_tracking) +
                                                    self._stale_updates_tracked) > self.stale_update_limit:
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
        n_success = 0
        n_fail = 0
        n_result = len(results)
        error_payload = []
        jobs_pushed = f"Pushed {n_result} complete tasks to the server "
        if n_result:
            try:
                self._post_update(results, allow_shutdown=allow_shutdown)
            except IOError:
                if self.server_error_retries is None or self.server_error_retries > 0:
                    self.logger.warning("Post complete tasks was not successful. Attempting again on next update.")
                    self._stale_payload_tracking.append([results, 0])
                    jobs_pushed = f"Tried to push {n_result} complete tasks to the server "
                else:
                    self.logger.warning("Post complete tasks was not successful. Data may be lost.")
                    self.n_stale_jobs += len(results)
                    jobs_pushed = f"Failed to push {n_result} complete tasks to the server "

            self.active -= n_result
            for key, result in results.items():
                if result.success:
                    n_success += 1
                else:
                    error_payload.append(f"Job {key} failed: {result.error.error_type} - "
                                         f"Msg: {result.error.error_message}")
            n_fail = n_result - n_success

        self.logger.info(jobs_pushed + f"({n_success} success / {n_fail} fail).")
        if n_fail:
            self.logger.warning("The following tasks failed with the errors:")
            for error in error_payload:
                self.logger.warning(error)

        open_slots = max(0, self.max_tasks - self.active)

        if (new_tasks is False) or (open_slots == 0):
            return True

        # Get new tasks
        payload = self._payload_template()
        payload["data"]["limit"] = open_slots
        try:
            new_tasks = self.client._automodel_request("queue_manager", "get", payload)
        except IOError as exc:
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
        task_base = json.dumps({
            "spec": {
                "function":
                "qcengine.compute",
                "args": [{
                    "molecule": get_molecule("hooh.json").json_dict(),
                    "driver": "energy",
                    "model": {},
                    "keywords": {},
                }, "program"],
                "kwargs": {}
            },
            "parser": "single",
        })

        programs = {
            "rdkit": {
                "method": "UFF",
                "basis": None
            },
            "torchani": {
                "method": "ANI1",
                "basis": None
            },
            "psi4": {
                "method": "HF",
                "basis": "sto-3g"
            },
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
                task["spec"]["args"][0]["keywords"] = {"e_convergence": (x * 1.e-6 + 1.e-6)}
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
                    fail_report[failed_program] = f"On test {k}:" \
                                                  f"\nException Type: {result.error.error_type}" \
                                                  f"\nException Message: {result.error.error_message}"
                failures += 1

        if failures:
            self.logger.error("{}/{} tasks failed!".format(failures, len(results)))
            self.logger.error(f"A sample error from each program to help:\n" +
                              "\n".join([e for e in fail_report.values()]))
            return False
        else:
            self.logger.info("All tasks completed successfully!")
            return True
