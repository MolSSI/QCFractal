from __future__ import annotations

import logging
import sched
import socket
import threading
import time
import traceback
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Tuple

import parsl.executors.high_throughput.interchange
import tabulate
from packaging.version import parse as parse_version
from parsl.config import Config as ParslConfig
from parsl.dataflow.dflow import DataFlowKernel
from parsl.dataflow.futures import Future as ParslFuture
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor

try:
    from pydantic.v1 import BaseModel, Extra, Field
except ImportError:
    from pydantic import BaseModel, Extra, Field
from requests.exceptions import Timeout

from qcfractalcompute.apps.app_manager import AppManager
from qcportal import ManagerClient
from qcportal.managers import ManagerName
from qcportal.metadata_models import TaskReturnMetadata
from qcportal.record_models import RecordTask
from qcportal.utils import seconds_to_hms, apply_jitter
from . import __version__
from .apps.models import AppTaskResult
from .compress import compress_result
from .config import FractalComputeConfig
from .executors import build_executor

if TYPE_CHECKING:
    from parsl.executors.base import ParslExecutor


class InterruptableScheduler(sched.scheduler):
    """
    A scheduler that can be interrupted
    """

    def __init__(self):
        self._int_event = threading.Event()
        super().__init__(time.monotonic, self._int_event.wait)

    def interrupt(self):
        # Clear all events in the queue, then interrupt the current sleep period
        for event in self.queue:
            self.cancel(event)

        self._int_event.set()


class ManagerStatistics(BaseModel):
    """
    Manager statistics
    """

    class Config(BaseModel.Config):
        extra = Extra.forbid

    last_update_time: float = Field(default_factory=time.time)

    active_tasks: int = 0
    active_cores: int = 0
    active_memory: float = 0.0
    total_cpu_hours: float = 0.0

    total_successful_tasks: int = 0
    total_failed_tasks: int = 0
    total_rejected_tasks: int = 0

    @property
    def total_finished_tasks(self) -> int:
        return self.total_successful_tasks + self.total_failed_tasks


class ComputeManager:
    """
    This object maintains a computational queue and watches for finished tasks for different
    queue backends. Finished tasks are added to the database and removed from the queue.
    """

    def __init__(self, config: FractalComputeConfig):
        # Setup logging
        self.logger = logging.getLogger("ComputeManager")

        self.name_data = ManagerName(cluster=config.cluster, hostname=socket.gethostname(), uuid=str(uuid.uuid4()))

        self.client = ManagerClient(
            name_data=self.name_data,
            address=config.server.fractal_uri,
            username=config.server.username,
            password=config.server.password,
            verify=config.server.verify,
        )

        # Check the allowed manager versions
        manager_version_lower_limit = parse_version(self.client.server_info["manager_version_lower_limit"])
        manager_version_upper_limit = parse_version(self.client.server_info["manager_version_upper_limit"])

        manager_version = parse_version(__version__)

        if not manager_version_lower_limit <= manager_version <= manager_version_upper_limit:
            raise RuntimeError(
                f"This manager version {str(manager_version)} does not fall within the server's allowed "
                f"manager versions of [{str(manager_version_lower_limit)}, {str(manager_version_upper_limit)}]."
                f"You may need to upgrade or downgrade"
            )

        # Load the executors
        self.manager_config = config

        self.statistics = ManagerStatistics()

        self.scheduler = InterruptableScheduler()

        # key = number of retries. value = dict of (task_id, compressed result)
        self._deferred_tasks: Dict[int, Dict[int, AppTaskResult]] = defaultdict(dict)

        # key = executor label, value = (key = task_id, value = parsl future)
        self._task_futures: Dict[str, Dict[int, ParslFuture]] = {exl: {} for exl in config.executors.keys()}

        # Mapping of task_id to record_id
        self._record_id_map: Dict[int, int] = {}

        self.all_compute_tags = []
        for ex_label, ex_config in config.executors.items():
            if len(ex_config.compute_tags) == 0:
                raise ValueError(f"Executor {ex_label} has no compute tags")

            self.all_compute_tags.extend(ex_config.compute_tags)

        # Merge compute tags, preserving order
        self.all_compute_tags = list(dict.fromkeys(self.all_compute_tags))

        # These are more properly set up in the start() method
        self.parsl_config = None
        self.dflow_kernel = None

        # Set up the app manager
        # A bit hacky, but the app_manager may already be set if
        # we are running in a testing environment
        if not hasattr(self, "app_manager"):
            self.app_manager = AppManager(self.manager_config)

        self.executor_programs = {}
        for ex in self.manager_config.executors.keys():
            ex_programs = self.app_manager.all_program_info(ex)
            if len(ex_programs) == 0:
                raise ValueError(f"Executor {ex} has no available programs")

            self.executor_programs[ex] = ex_programs

        self.all_program_info = self.app_manager.all_program_info()

        self.logger.info("-" * 80)
        self.logger.info("QCFractal Compute Manager:")
        self.logger.info("    Version:     {}".format(__version__))
        self.logger.info("    Base folder: {}".format(self.manager_config.base_folder))
        self.logger.info("    Cluster:     {}".format(self.name_data.cluster))
        self.logger.info("    Hostname:    {}".format(self.name_data.hostname))
        self.logger.info("    UUID:        {}".format(self.name_data.uuid))

        self.logger.info("\n")

        self.logger.info("    Parsl Executors:")
        for label, ex_config in config.executors.items():
            self.logger.info("    {}:".format(label))
            self.logger.info("                    Type: {}".format(ex_config.type))
            self.logger.info("            Compute Tags: {}".format(ex_config.compute_tags))
            self.logger.info("                Programs:")
            executor_programs = self.app_manager.all_program_info(label)
            for program, info in executor_programs.items():
                self.logger.info("                    {}: {}".format(program, info))

        self.logger.info("\n")

        # Pull server info
        self.server_info = self.client.get_server_information()
        self.heartbeat_frequency = self.server_info["manager_heartbeat_frequency"]
        self.heartbeat_frequency_jitter = self.server_info.get("manager_heartbeat_frequency_jitter", 0.0)

        self.client.activate(__version__, self.all_program_info, self.all_compute_tags)

        self.logger.info("    Connected to:")
        self.logger.info("        Address:     {}".format(self.client.address))
        self.logger.info("        Name:        {}".format(self.server_info["name"]))
        self.logger.info("        Version:     {}".format(self.server_info["version"]))
        self.logger.info("        Username:    {}".format(self.client.username))
        self.logger.info("        Heartbeat:   {}".format(self.heartbeat_frequency))
        self.logger.info("-" * 80)

        # Is this manager in the process of shutting down?
        # This is used to prevent the manager from starting new tasks in the scheduler
        self._is_stopping = False

        # Number of failed heartbeats. After missing a bunch, we will shutdown
        self._failed_heartbeats = 0

        # Time at which the worker started idling (no jobs being run)
        self._idle_start_time = None

    @staticmethod
    def _get_max_workers(executor: ParslExecutor) -> int:
        """
        Obtain the maximum number of tasks that can be run on an executor
        """

        ####################################################################################
        # This function is broken out slightly, as there are some combinations
        # of executors and providers that are not yet supported (but may be in the future)
        ####################################################################################

        if isinstance(executor, ThreadPoolExecutor):
            return executor.max_threads

        if isinstance(executor, HighThroughputExecutor):
            prov = executor.provider

            # The maximum number of workers are there on a single node
            # (this is somewhat misnamed sometimes. The 'max_workers' also represents
            # the maximum number of workers *per node*, which is set by the user. The
            # 'workers_per_node' is calculated by the executor and takes into account
            # memory and cpu limitations
            workers_per_node = executor.workers_per_node

            # Block information comes from the provider
            # I *think* all providers have a max_blocks and nodes_per_block
            max_blocks = prov.max_blocks
            nodes_per_block = prov.nodes_per_block

            max_tasks = workers_per_node * nodes_per_block * max_blocks
            return max_tasks

        raise ValueError(f"Executor type not supported: {type(executor)}")

    @property
    def name(self) -> str:
        """
        Returns this manager's full name.
        """
        return self.name_data.fullname

    @property
    def n_active_tasks(self) -> Dict[str, int]:
        return {
            ex_label: sum(0 if task.done() else 1 for task in self._task_futures[ex_label].values())
            for ex_label in self._task_futures.keys()
        }

    @property
    def n_total_active_tasks(self) -> int:
        return sum(self.n_active_tasks.values())

    @property
    def n_deferred_tasks(self) -> int:
        return sum(len(x) for x in self._deferred_tasks.values())

    def start(self, manual_updates: bool = False):
        """
        Starts the manager

        This will block until stop() is called
        """

        ###########################################
        # Set up Parsl executors and DataFlowKernel
        ###########################################
        self.parsl_config = ParslConfig(
            executors=[],
            initialize_logging=False,
            run_dir=self.manager_config.parsl_run_dir,
            usage_tracking=self.manager_config.parsl_usage_tracking,
        )
        self.dflow_kernel = DataFlowKernel(self.parsl_config)

        for ex_label, ex_config in self.manager_config.executors.items():
            ex = build_executor(ex_label, ex_config)
            self.dflow_kernel.add_executors([ex])

        def scheduler_update():
            if not manual_updates:
                self.update(new_tasks=True)
            if not self._is_stopping:
                delay = apply_jitter(self.manager_config.update_frequency, self.manager_config.update_frequency_jitter)
                self.scheduler.enter(delay, 1, scheduler_update)

        def scheduler_heartbeat():
            if not manual_updates:
                self.heartbeat()
            if not self._is_stopping:
                delay = apply_jitter(self.heartbeat_frequency, self.heartbeat_frequency_jitter)
                self.scheduler.enter(delay, 1, scheduler_heartbeat)

        self.logger.info("Compute Manager successfully started.")

        self._failed_heartbeats = 0

        # Start the idle timer to be right now, since we aren't doing anything
        self._idle_start_time = time.time()

        self.scheduler.enter(0, 1, scheduler_update)
        self.scheduler.enter(0, 2, scheduler_heartbeat)

        # Blocks until the ComputeManager.stop() method is called
        self.scheduler.run(blocking=True)

        #############################################
        # If we got here, the scheduler has stopped
        # Now handle the shutdown
        #############################################
        self.update(new_tasks=False)

        try:
            # Notify the server of shutdown
            self.client.deactivate(
                active_tasks=self.statistics.active_tasks,
                active_cores=self.statistics.active_cores,
                active_memory=self.statistics.active_memory,
                total_cpu_hours=self.statistics.total_cpu_hours,
            )

            shutdown_str = "Shutdown was successful, {} tasks returned to the fractal server"

        except Exception as ex:
            self.logger.warning(f"Deactivation failed for {self.name}: {str(ex).strip()}")
            shutdown_str = "Shutdown was not successful, {} tasks not returned."

        shutdown_str = shutdown_str.format(f"{self.n_total_active_tasks} active and {self.n_deferred_tasks} stale")
        self.logger.info(f"Manager {self.name}: {shutdown_str}")

        # Close down the parsl stuff. Can sometimes get called after the
        # DataFlowKernel atexit handler has been called
        if not self.dflow_kernel.cleanup_called:
            self.dflow_kernel.cleanup()

        self.dflow_kernel = None
        self.parsl_config = None

        self.logger.info("Compute manager stopping gracefully.")

    def stop(self) -> None:
        """
        Interrupts a running worker, causing it to shut down
        """
        self.logger.info("Manager stopping")

        self._is_stopping = True

        # This interrupts the scheduler, which will cause the rest of the start() method to run
        self.scheduler.interrupt()

    def heartbeat(self) -> None:
        """
        Provides a heartbeat to the connected Server.
        """

        try:
            self.client.heartbeat(
                active_tasks=self.statistics.active_tasks,
                active_cores=self.statistics.active_cores,
                active_memory=self.statistics.active_memory,
                total_cpu_hours=self.statistics.total_cpu_hours,
            )
            self._failed_heartbeats = 0

        except (ConnectionError, Timeout) as ex:
            self._failed_heartbeats += 1
            self.logger.warning(f"Heartbeat failed: {str(ex).strip()}. QCFractal server down?")
            self.logger.warning(f"Missed {self._failed_heartbeats} heartbeats so far")
            if self._failed_heartbeats > self.client.server_info["manager_heartbeat_max_missed"]:
                self.logger.warning("Too many failed heartbeats, shutting down.")
                self.stop()

    def _acquire_complete_tasks(self) -> Dict[str, Dict[int, AppTaskResult]]:
        # First key is name of executor
        # Second key is task_id
        # Value is the result (including compressed computation result)
        ret: Dict[str, Dict[int, AppTaskResult]] = {}

        for executor_label, task_futures in self._task_futures.items():
            ret.setdefault(executor_label, {})

            # Finished tasks - will be removed from the task_futures dict later
            finished: List[int] = []

            for task_id, task in task_futures.items():
                if task.done():
                    try:
                        ret[executor_label][task_id] = task.result()

                    except parsl.executors.high_throughput.interchange.ManagerLost as e:
                        msg = "Compute worker lost:\n" + traceback.format_exc()
                        failed_op = {
                            "success": False,
                            "error": {"error_type": e.__class__.__name__, "error_message": msg},
                        }

                        ret[executor_label][task_id] = AppTaskResult(
                            success=False, walltime=0.0, result_compressed=compress_result(failed_op)
                        )

                    except Exception as e:
                        msg = "Error getting task result:\n" + traceback.format_exc()
                        failed_op = {
                            "success": False,
                            "error": {"error_type": e.__class__.__name__, "error_message": msg},
                        }

                        ret[executor_label][task_id] = AppTaskResult(
                            success=False, walltime=0.0, result_compressed=compress_result(failed_op)
                        )

                    finished.append(task_id)

            for task_id in finished:
                del task_futures[task_id]

        # Print out the table of finished tasks
        # columns: task_id, record_id, executor, walltime, status
        table_rows: List[Tuple[int, int, str, str, str]] = []
        for executor_label, executor_results in ret.items():
            for task_id, task_result in executor_results.items():
                if task_result.success:
                    status_str = "success"
                else:
                    status_str = "error: " + task_result.result["error"]["error_type"]

                table_rows.append(
                    (
                        task_id,
                        self._record_id_map[task_id],
                        executor_label,
                        seconds_to_hms(task_result.walltime),
                        status_str,
                    )
                )

        log_str = f"Acquired {len(table_rows)} finished tasks from the executors"

        if table_rows:
            log_str += "\n" + tabulate.tabulate(
                sorted(table_rows), headers=["task id", "record id", "executor", "walltime", "status"]
            )

        self.logger.info(log_str)

        return ret

    def _submit_tasks(self, executor_label: str, tasks: List[RecordTask]):
        """
        Submits tasks to the parsl queue to be run
        """

        for task in tasks:
            task_app = self.app_manager.get_app(self.dflow_kernel, executor_label, task)
            task_future = task_app(
                task.record_id,
                task.function_kwargs_compressed,
                executor_config=self.manager_config.executors[executor_label],
            )
            self._task_futures[executor_label][task.id] = task_future
            self._record_id_map[task.id] = task.record_id

    def _return_finished(self, results: Dict[int, AppTaskResult]) -> TaskReturnMetadata:
        # Handling of exceptions is expected to be done in the calling function
        to_send = {k: v.result_compressed for k, v in results.items()}
        return_meta = self.client.return_finished(to_send)

        if return_meta.success:
            self.logger.info(f"Successfully returned {return_meta.n_accepted} tasks to the fractal server")
        else:
            self.logger.warning(f"Error in returning tasks: {str(return_meta.error_string)}")

        return return_meta

    def _update_deferred_tasks(self) -> Dict[int, TaskReturnMetadata]:
        """
        Attempt to post the previous payload failures
        """
        new_deferred_tasks = defaultdict(dict)

        # key = number of attempts. value = metadata
        ret: Dict[int, TaskReturnMetadata] = {}

        for attempts, results in self._deferred_tasks.items():
            try:
                return_meta = self._return_finished(results)
                ret[attempts] = return_meta

                if return_meta.success:
                    self.logger.info(f"Successfully pushed jobs from {attempts+1} updates ago")
                else:
                    self.logger.warning(
                        f"Did not successfully push jobs from {attempts+1} updates ago. Error: {return_meta.error_string}"
                    )

            except (Timeout, ConnectionError):
                # Tried and failed
                attempts += 1

                new_deferred_tasks[attempts] = results
                self.logger.warning(f"Could not post jobs from {attempts-1} updates ago, will retry on next update.")

        self._deferred_tasks = new_deferred_tasks
        return ret

    def _update(self, new_tasks) -> None:
        # First, try pushing back any stale results
        deferred_return_info = self._update_deferred_tasks()

        results = self._acquire_complete_tasks()

        server_up = True

        # Stores rows of the status table printed at the end
        # Columns: task_id, status, reason
        # record_id will be added later
        status_rows: List[Tuple[int, str, str]] = []

        # Add the info from updating deferred tasks to the table
        for attempts, return_meta in deferred_return_info.items():
            status_rows.extend(
                [(task_id, f"sent (was deferred {attempts})", "") for task_id in return_meta.accepted_ids]
            )
            status_rows.extend(
                [
                    (task_id, f"rejected (was deferred {attempts})", reason)
                    for task_id, reason in return_meta.rejected_info
                ]
            )
            self.statistics.total_rejected_tasks += return_meta.n_rejected

        # Return results to the server (per executor)
        for executor_label, executor_results in results.items():
            # Any post-processing tasks
            # Sometimes used for saving data for later
            self.postprocess_results(executor_results)

            n_result = len(executor_results)

            if n_result:
                n_success = 0

                try:
                    return_meta = self._return_finished(executor_results)

                    status_rows.extend([(task_id, "sent", "") for task_id in return_meta.accepted_ids])

                    status_rows.extend([(task_id, "rejected", reason) for task_id, reason in return_meta.rejected_info])
                    self.statistics.total_rejected_tasks += return_meta.n_rejected

                except (ConnectionError, Timeout):
                    self.logger.warning("Returning complete tasks failed. Attempting again on next update.")
                    self._deferred_tasks[0].update(executor_results)

                    status_rows.extend([(task_id, "deferred", "") for task_id in executor_results.keys()])
                    server_up = False

                for task_id, app_result in executor_results.items():
                    walltime_seconds = app_result.walltime

                    if app_result.success:
                        n_success += 1
                    else:
                        self.logger.debug(f"Task {task_id} (record {self._record_id_map[task_id]}) failed:")
                        self.logger.debug(app_result.result["error"]["error_message"])

                    cores_per_worker = self.manager_config.executors[executor_label].cores_per_worker
                    self.statistics.total_cpu_hours += walltime_seconds * cores_per_worker / 3600

                n_fail = n_result - n_success

                self.logger.info(
                    f"Executor {executor_label}: Processed {n_result} tasks: {n_success} success / {n_fail} failed"
                )

                # Update the statistics
                self.statistics.total_successful_tasks += n_success
                self.statistics.total_failed_tasks += n_fail

        ########################################################################
        # Update a few more statistics
        # total_successful_tasks/n_failed_tasks are updated above, per executor
        ########################################################################

        n_active_tasks = self.n_active_tasks

        # Active cores - active tasks * cores per task (worker) for each executor
        self.statistics.active_cores = sum(
            n_active_tasks[ex_label] * ex_config.cores_per_worker
            for ex_label, ex_config in self.manager_config.executors.items()
        )

        # Same as above, but for memory
        self.statistics.active_memory = sum(
            n_active_tasks[ex_label] * ex_config.memory_per_worker
            for ex_label, ex_config in self.manager_config.executors.items()
        )

        if status_rows:
            log_str = "Task return status:\n"

            # Add record_id
            new_status_rows = [
                (task_id, self._record_id_map[task_id], status, reason)
                for task_id, status, reason in sorted(status_rows)
            ]
            log_str += tabulate.tabulate(new_status_rows, headers=["task id", "record id", "status", "reason"])
            self.logger.info(log_str)

        ###########################################
        # Write statistics to the log
        #######################################
        task_stats_str = (
            f"Task Stats: Total finished={self.statistics.total_finished_tasks}, "
            f"Failed={self.statistics.total_failed_tasks}, "
            f"Success={self.statistics.total_successful_tasks}, "
            f"Rejected={self.statistics.total_rejected_tasks}"
        )

        worker_stats_str = f"Worker Stats (est.): Core Hours Used={self.statistics.total_cpu_hours:,.2f}"

        self.logger.info(task_stats_str)
        self.logger.info(worker_stats_str)
        self.statistics.last_update_time = time.time()

        if new_tasks and server_up:
            # What do we have for each executor?
            active_tasks = self.n_active_tasks

            for executor_label, executor_config in self.manager_config.executors.items():
                executor = self.dflow_kernel.executors[executor_label]

                # How many slots do we have?
                # TODO - intelligently figure out the number tasks to claim over the number of slots
                open_slots = (3 * self._get_max_workers(executor)) - active_tasks[executor_label]

                self.logger.info(
                    f"Executor {executor_label} has {active_tasks[executor_label]} active tasks and {open_slots} open slots"
                )

                if open_slots > 0:
                    try:
                        executor_programs = self.executor_programs[executor_label]
                        new_task_info = self.client.claim(executor_programs, executor_config.compute_tags, open_slots)
                    except (Timeout, ConnectionError) as ex:
                        self.logger.warning(f"Acquisition of new tasks failed: {str(ex).strip()}")
                        return

                    self.logger.info("Acquired {} new tasks.".format(len(new_task_info)))

                    # Add new tasks to queue
                    self.preprocess_new_tasks(new_task_info)
                    self._submit_tasks(executor_label, new_task_info)

    def update(self, new_tasks) -> None:
        """Examines the queue for completed tasks and adds successful completions to the database
        while unsuccessful are logged for future inspection.

        Parameters
        ----------
        new_tasks
            Try to get new tasks from the server
        """

        self._update(new_tasks=new_tasks)

        if self.manager_config.max_idle_time is None:
            return

        # Check if we are idle. If we are beyond the max idle time, then shut down
        is_idle = (self.n_total_active_tasks == 0) and (self.n_deferred_tasks == 0)

        if is_idle and self._idle_start_time is None:
            self._idle_start_time = time.time()

        if not is_idle:
            self._idle_start_time = None
        else:
            idle_time = time.time() - self._idle_start_time
            if idle_time >= self.manager_config.max_idle_time:
                self.logger.warning(
                    f"Manager has been idle for {idle_time:.2f} seconds - max is "
                    f"{self.manager_config.max_idle_time}, shutting down"
                )
                self.stop()
            else:
                self.logger.info(f"Manager has been idle for {idle_time:.2f} seconds")

    def preprocess_new_tasks(self, new_tasks: List[RecordTask]):
        """
        Any processing to do to the new tasks

        To be overridden by a derived class. Sometimes used to save the results for testing
        """
        pass

    def postprocess_results(self, results: Dict[int, AppTaskResult]):
        """
        Any processing to do to the results

        To be overridden by a derived class. Sometimes used to save the results for testing
        """
        pass
