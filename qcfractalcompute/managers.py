"""
Queue backend abstraction manager.
"""
import json
import logging
import os
import sched
import socket
import threading
import time
import uuid
from collections import defaultdict
from typing import Dict, List, Optional, Union

import qcengine as qcng
from pydantic import BaseModel, validator
from qcelemental.models import Molecule, FailedOperation

from qcportal.serialization import serialize, deserialize
from . import __version__
from .adapters import build_queue_adapter
from .compress import compress_results

__all__ = ["ComputeManager"]

from qcportal import ManagerClient
from qcportal.utils import make_list
from qcportal.managers import ManagerName
from qcportal.metadata_models import TaskReturnMetadata
from qcportal.records import AllResultTypes
from requests.exceptions import Timeout


class SleepInterrupted(BaseException):
    """
    Exception class used to signal that an InterruptableSleep was interrupted

    This (like KeyboardInterrupt) derives from BaseException to prevent
    it from being handled with "except Exception".
    """

    pass


class InterruptableSleep:
    """
    A class for sleeping, but interruptable

    This class uses threading Events to wake up from a sleep before the entire sleep
    duration has run. If the sleep is interrupted, then an SleepInterrupted exception is raised.

    This class is a functor, so an instance can be passed as the delay function to a python
    sched.scheduler
    """

    def __init__(self):
        self._event = threading.Event()

    def __call__(self, delay: float):
        interrupted = self._event.wait(delay)
        if interrupted:
            raise SleepInterrupted()

    def interrupt(self):
        self._event.set()

    def clear(self):
        self._event.clear()


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


class ComputeManager:
    """
    This object maintains a computational queue and watches for finished tasks for different
    queue backends. Finished tasks are added to the database and removed from the queue.
    """

    def __init__(
        self,
        queue_client: "BaseAdapter",
        fractal_uri: str,
        max_tasks: int = 200,
        queue_tag: Optional[Union[str, List[str]]] = None,
        manager_name: str = "unlabeled",
        update_frequency: Union[int, float] = 2,
        verbose: bool = False,  # TODO: Remove verbose flag, always respect logging level
        server_error_retries: Optional[int] = 1,
        deferred_task_limit: Optional[int] = 50,
        cores_per_task: Optional[int] = None,
        memory_per_task: Optional[float] = None,
        nodes_per_task: Optional[int] = None,
        cores_per_rank: Optional[int] = 1,
        scratch_directory: Optional[str] = None,
        retries: Optional[int] = 2,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
    ):
        """
        Parameters
        ----------
        queue_client : BaseAdapter
            The DBAdapter class for queue abstraction
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
        deferred_task_limit : Optional[int], optional
            Number of deferred tasks to keep around
            If this limit is ever hit, the server will refuse to pull down new tasks
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
        """

        # Setup logging
        self.logger = logging.getLogger("QCFractalCompute")

        self.name_data = ManagerName(cluster=manager_name, hostname=socket.gethostname(), uuid=str(uuid.uuid4()))

        self.client = ManagerClient(
            name_data=self.name_data,
            address=fractal_uri,
            username=username,
            password=password,
            verify=verify,
        )

        self.save_results_path = None

        self.cores_per_task = cores_per_task
        self.memory_per_task = memory_per_task
        self.nodes_per_task = nodes_per_task or 1
        self.scratch_directory = scratch_directory
        self.retries = retries
        self.cores_per_rank = cores_per_rank
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
        if queue_tag is None:
            self.queue_tag = ["*"]
        else:
            self.queue_tag = make_list(queue_tag)
        self.verbose = verbose

        self.statistics = QueueStatistics(
            max_concurrent_tasks=self.max_tasks,
            cores_per_task=(cores_per_task or 0),
            memory_per_task=(memory_per_task or 0),
            update_frequency=update_frequency,
        )

        self.int_sleep = InterruptableSleep()
        self.scheduler = sched.scheduler(time.time, self.int_sleep)

        self.update_frequency = update_frequency
        self.periodic = {}
        self.active = 0

        # Server response/stale job handling
        self.server_error_retries = server_error_retries
        self.deferred_task_limit = deferred_task_limit

        # key = number of retries. value = dict of (task_id, result)
        self._deferred_tasks: Dict[int, Dict[int, AllResultTypes]] = defaultdict(dict)

        # All available programs
        # Add qcengine, with the version
        self.all_program_info = {'qcengine': qcng.__version__}

        # What do we get from qcengine
        qcng_programs = qcng.list_available_programs()
        qcng_procedures = qcng.list_available_procedures()

        # QCFractal treats procedures and programs as being the same
        # TODO - get version information
        self.all_program_info.update({x: None for x in qcng_programs})
        self.all_program_info.update({x: None for x in qcng_procedures})

        # Display a warning if there are non-node-parallel programs and >1 node_per_task
        if self.nodes_per_task > 1:
            for name in qcng_programs:
                program = qcng.get_program(name)
                if not program.node_parallel:
                    self.logger.warning(
                        "Program {} is not node parallel," " but manager will use >1 node per task".format(name)
                    )

        # Print out configuration
        self.logger.info("QueueManager:")
        self.logger.info("    Version:         {}\n".format(__version__))

        if self.verbose:
            self.logger.info("    Name Information:")
            self.logger.info("        Cluster:     {}".format(self.name_data.cluster))
            self.logger.info("        Hostname:    {}".format(self.name_data.hostname))
            self.logger.info("        UUID:        {}\n".format(self.name_data.uuid))

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
            self.logger.info("        Programs:       {}".format(qcng_programs))
            self.logger.info("        Procedures:     {}\n".format(qcng_procedures))

        # Pull server info
        self.server_info = self.client.get_server_information()
        self.server_name = self.server_info["name"]
        self.server_version = self.server_info["version"]
        self.heartbeat_frequency = self.server_info["manager_heartbeat_frequency"]

        self.client.activate(__version__, self.all_program_info, tags=self.queue_tag)

        if self.verbose:
            self.logger.info("    Connected:")
            self.logger.info("        Version:     {}".format(self.server_version))
            self.logger.info("        Address:     {}".format(self.client.address))
            self.logger.info("        Name:        {}".format(self.server_name))
            self.logger.info("        Queue tag:   {}".format(self.queue_tag))
            self.logger.info("        Username:    {}\n".format(self.client.username))

    @property
    def name(self) -> str:
        """
        Returns the Managers full name.
        """
        return self.name_data.fullname

    @property
    def n_deferred_tasks(self) -> int:
        return sum(len(x) for x in self._deferred_tasks.values())

    def start(self) -> None:
        """
        Starts the manager

        This will block until stop() is called
        """

        def scheduler_update():
            self.update(True)
            self.scheduler.enter(self.update_frequency, 1, scheduler_update)

        def scheduler_heartbeat():
            self.heartbeat()
            self.scheduler.enter(self.heartbeat_frequency, 1, scheduler_heartbeat)

        self.logger.info("QueueManager successfully started.")

        self.scheduler.enter(0, 1, scheduler_update)
        self.scheduler.enter(0, 2, scheduler_heartbeat)

        try:
            self.scheduler.run()
        except KeyboardInterrupt:
            self.logger.info("Caught SIGINT/Keyboard interrupt")

        except SleepInterrupted:
            self.logger.info("Running of services successfully interrupted")

        finally:
            # Push data back to the server & notify server of shutdown
            self.update(new_tasks=False)
            self.deactivate()

            # Close down the adapter
            self.queue_adapter.close()

            self.logger.info("QueueManager stopping gracefully.")

    def stop(self) -> None:
        """
        Interrupts a running worker, causing it to shut down
        """
        self.logger.info("Manager stopping/shutting down")

        # Interrupt the scheduler (will finish if in the middle of an update or something, but will
        # cancel running calculations)
        self.int_sleep.interrupt()

    def heartbeat(self) -> None:
        """
        Provides a heartbeat to the connected Server.
        """

        try:
            self.client.heartbeat(
                total_worker_walltime=self.statistics.total_worker_walltime,
                total_task_walltime=self.statistics.total_task_walltime,
                active_tasks=self.statistics.active_task_slots,
                active_cores=self.statistics.active_cores,
                active_memory=self.statistics.active_memory,
            )
        except ConnectionError as ex:
            self.logger.warning(f"Heartbeat failed: {str(ex).strip()}")

    def deactivate(self):
        """
        Shutdown the manager and returns tasks to queue.
        """

        try:
            # Notify the server of shutdown
            self.client.deactivate(
                total_worker_walltime=self.statistics.total_worker_walltime,
                total_task_walltime=self.statistics.total_task_walltime,
                active_tasks=self.statistics.active_task_slots,
                active_cores=self.statistics.active_cores,
                active_memory=self.statistics.active_memory,
            )

            shutdown_string = "Shutdown was successful, {} tasks returned to the fractal server"

        except Exception as ex:
            self.logger.warning(f"Deactivation failed: {str(ex).strip()}")
            shutdown_string = "Shutdown was not successful, {} tasks not returned."

        n_deferred = self.n_deferred_tasks
        if n_deferred:
            shutdown_string = shutdown_string.format(f"{self.active} active and {n_deferred} stale")
        else:
            shutdown_string = shutdown_string.format(self.active)

        self.logger.info(shutdown_string)

    def _return_finished(self, results: Dict[int, AllResultTypes]) -> TaskReturnMetadata:
        return_meta = self.client.return_finished(results)
        self.logger.info(f"Successfully return tasks to the fractal server")
        if return_meta.accepted_ids:
            self.logger.info(f"Accepted task ids: " + " ".join(str(x) for x in return_meta.accepted_ids))
        if return_meta.rejected_ids:
            self.logger.info(f"Rejected task ids: ")
            for tid, reason in return_meta.rejected_info:
                self.logger.warning(f"    Task id {tid}: {reason}")
        if not return_meta.success:
            self.logger.warning(f"Error in returning tasks: {str(return_meta.error_string)}")
        return return_meta

    def _update_deferred_tasks(self) -> None:
        """
        Attempt to post the previous payload failures
        """
        new_deferred_tasks = defaultdict(dict)

        for attempts, results in self._deferred_tasks.items():
            try:
                return_meta = self._return_finished(results)
                if return_meta.success:
                    self.logger.info(f"Successfully pushed jobs from {attempts+1} updates ago")
                else:
                    self.logger.warning(
                        f"Did not successfully push jobs from {attempts+1} updates ago. Error: {return_meta.error_string}"
                    )

            except ConnectionError:
                # Tried and failed
                attempts += 1

                # Case: Still within the retry limit
                if self.server_error_retries is None or attempts < self.server_error_retries:
                    new_deferred_tasks[attempts] = results
                    self.logger.warning(
                        f"Could not post jobs from {attempts-1} updates ago, will retry on next update."
                    )

                # Case: Over limit
                else:
                    self.logger.warning(
                        f"Could not post {len(results)} tasks from {attempts-1} updates ago and over attempt limit. Dropping"
                    )

        self._deferred_tasks = new_deferred_tasks

    def update(self, new_tasks) -> None:
        """Examines the queue for completed tasks and adds successful completions to the database
        while unsuccessful are logged for future inspection.

        Parameters
        ----------
        new_tasks
            Try to get new tasks from the server
        """

        # First, try pushing back any stale results
        self._update_deferred_tasks()

        results = self.queue_adapter.acquire_complete()

        # Compress the stdout/stderr/error outputs, and native files
        results = compress_results(results)

        # If requested, save the outputs to json
        if self.save_results_path is not None:
            if os.path.exists(self.save_results_path):
                with open(self.save_results_path, "r") as save_file:
                    data = deserialize(save_file.read(), "json")
            else:
                data = {}

            data.update(results)

            with open(self.save_results_path, "w") as save_file:
                save_file.write(serialize(data, "json"))

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

        if n_result:
            # For logging
            failure_messages = {}

            try:
                return_meta = self._return_finished(results)
                task_status = {k: "sent" for k in results.keys() if k in return_meta.accepted_ids}
                task_status.update({k: "rejected" for k in results.keys() if k in return_meta.rejected_ids})
            except (ConnectionError, Timeout):
                if self.server_error_retries is None or self.server_error_retries > 0:
                    self.logger.warning("Returning complete tasks failed. Attempting again on next update.")
                    self._deferred_tasks[0].update(results)
                    task_status = {k: "deferred" for k in results.keys()}
                else:
                    self.logger.warning("Returning complete tasks failed. Data may be lost.")
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
                    assert isinstance(result, FailedOperation)

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
            self.logger.info(f"Processed {len(results)} tasks: {n_success} success / {n_fail} failed")
            self.logger.info(f"Task ids, submission status, calculation status below")
            for task_id, status_msg in task_status.items():
                self.logger.info(f"    Task {task_id} : {status_msg}")
            if n_fail:
                self.logger.debug("The following tasks failed with the errors:")
                for task_id, error_info in failure_messages.items():
                    self.logger.debug(f"Error for task id {task_id}: {error_info.error_type}")
                    self.logger.debug("    Backtrace: \n" + str(error_info.error_message))

        # Crunch Statistics
        self.statistics.total_failed_tasks += n_fail
        self.statistics.total_successful_tasks += n_success
        self.statistics.total_task_walltime += task_cpu_hours
        na_format = ""
        float_format = ",.2f"

        task_stats_str = None
        worker_stats_str = None
        if self.statistics.total_completed_tasks > 0:
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

        if task_stats_str is not None:
            self.logger.info(task_stats_str)
        if worker_stats_str is not None:
            self.logger.info(worker_stats_str)

        open_slots = max(0, self.max_tasks - self.active)

        if self.deferred_task_limit is not None:
            open_slots = min(open_slots, max(0, self.deferred_task_limit - self.n_deferred_tasks))

        if new_tasks is True and open_slots > 0:
            try:
                new_tasks = self.client.claim(open_slots)
            except ConnectionError as ex:
                self.logger.warning(f"Acquisition of new tasks failed: {str(ex).strip()}")
                return

            self.logger.info("Acquired {} new tasks.".format(len(new_tasks)))

            # Add new tasks to queue
            self.queue_adapter.submit_tasks(new_tasks)
            self.active += len(new_tasks)

    def test(self, n=1) -> bool:
        """
        Tests all known programs with simple inputs to check if the Adapter is correctly instantiated.
        """

        from qcfractal import testing

        test_molecule = Molecule(
            name="HOOH",
            geometry=[
                1.848671612718783,
                1.4723466699847623,
                0.6446435664312682,
                1.3127881568370925,
                -0.1304193792618355,
                -0.2118922703584585,
                -1.3127927010942337,
                0.1334187339129038,
                -0.21189641512867613,
                -1.8386801669381663,
                -1.482348324549995,
                0.6446369709610646,
            ],
            symbols=["H", "O", "O", "H"],
            connectivity=[[0, 1, 1], [1, 2, 1], [2, 3, 1]],
        )

        self.logger.info("Testing requested, generating tasks")
        task_base = json.dumps(
            {
                "spec": {
                    "function": "qcengine.compute",
                    "args": [
                        {
                            "molecule": Molecule.from_file("hooh.json").dict(encoding="json"),
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
