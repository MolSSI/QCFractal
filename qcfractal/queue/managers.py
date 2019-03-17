"""
Queue backend abstraction manager.
"""

import asyncio
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
from ..interface.models.rest_models import (QueueManagerGETBody, QueueManagerGETResponse, QueueManagerMeta,
                                            QueueManagerPOSTBody, QueueManagerPOSTResponse, QueueManagerPUTBody,
                                            QueueManagerPUTResponse)

__all__ = ["QueueManager"]


class QueueManager:
    """
    This object maintains a computational queue and watches for finished tasks for different
    queue backends. Finished tasks are added to the database and removed from the queue.

    Attributes
    ----------
    client : FractalClient
        A Portal client to connect to a server
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
                 logger: Optional[logging.Logger]=None,
                 max_tasks: int=200,
                 queue_tag: str=None,
                 manager_name: str="unlabled",
                 update_frequency: Union[int, float]=2,
                 verbose: bool=True,
                 cores_per_task: Optional[int]=None,
                 memory_per_task: Optional[Union[int, float]]=None):
        """
        Parameters
        ----------
        client : FractalClient
            A Portal client to connect to a server
        queue_client : QueueAdapter
            The DBAdapter class for queue abstraction
        storage_socket : DBSocket
            A socket for the backend database
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
        cores_per_task : int, optional, Default: None
            How many CPU cores per computation task to allocate for QCEngine
            None indicates "use however many you can detect"
        memory_per_task: int, optional, Default: None
            How much memory, in GiB, per computation task to allocate for QCEngine
            None indicates "use however much you can consume"
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
        self.queue_adapter = build_queue_adapter(
            queue_client, logger=self.logger, cores_per_task=self.cores_per_task, memory_per_task=self.memory_per_task)
        self.max_tasks = max_tasks
        self.queue_tag = queue_tag
        self.verbose = verbose

        self.scheduler = None
        self.update_frequency = update_frequency
        self.periodic = {}
        self.active = 0
        self.exit_callbacks = []

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
            put_body = QueueManagerPUTBody(**payload)
            r = self.client._request("put", "queue_manager", data=put_body.json())
            _ = QueueManagerPUTResponse.parse_raw(r.text)  # Validate

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
        meta = QueueManagerMeta(
            **self.name_data.copy(),

            # Version info
            qcengine_version=qcng.__version__,
            manager_version=get_information("version"),

            # User info
            username=self.client.username,

            # Pull info
            programs=self.available_programs,
            procedures=self.available_procedures,
            tag=self.queue_tag, )

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
        Starts up all IOLoops and processes
        """

        self.assert_connected()

        self.scheduler = sched.scheduler(time.time, time.sleep)
        heartbeat_time = int(0.8 * self.heartbeat_frequency)

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
        Shuts down all IOLoops and periodic updates
        """
        self.logger.info("QueueManager recieved shutdown signal: {}.\n".format(signame))

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
        Closes down the underlying adapter
        """

        return self.queue_adapter.close()

## Queue Manager functions

    def heartbeat(self) -> None:
        """
        Provides a heartbeat to the connected Server
        """

        self.assert_connected()

        payload = self._payload_template()
        payload["data"]["operation"] = "heartbeat"
        put_body = QueueManagerPUTBody(**payload)
        r = self.client._request("put", "queue_manager", data=put_body.json(), noraise=True)
        if r.status_code != 200:
            # TODO something as we didnt successfully add the data
            self.logger.warning("Heartbeat was not successful.")
        else:
            self.logger.info("Heartbeat was successful.")

        _ = QueueManagerPUTResponse.parse_raw(r.text)  # Validate

    def shutdown(self) -> Dict[str, Any]:
        """
        Shutdown the manager and returns tasks to queue.
        """
        self.assert_connected()

        self.update(new_tasks=False)

        payload = self._payload_template()
        payload["data"]["operation"] = "shutdown"
        put_body = QueueManagerPUTBody(**payload)
        r = self.client._request("put", "queue_manager", data=put_body.json(), noraise=True, timeout=2)

        if r.status_code != 200:
            # TODO something as we didnt successfully add the data
            self.logger.warning("Shutdown was not successful. This may delay queued tasks.")
            return {"nshutdown": 0}

        response = QueueManagerPUTResponse.parse_raw(r.text)
        nshutdown = response.data["nshutdown"]
        self.logger.info("Shutdown was successful, {} tasks returned to master queue.".format(nshutdown))
        return response.data

    def add_exit_callback(self, callback: Callable, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        """Adds additional callbacks to perform when closing down the server

        Parameters
        ----------
        callback : callable
            The function to call at exit
        *args
            Arguements to call with the function.
        **kwargs
            Kwargs to call with the function.

        """
        self.exit_callbacks.append((callback, args, kwargs))

    def update(self, new_tasks: bool=True) -> bool:
        """Examines the queue for completed tasks and adds successful completions to the database
        while unsuccessful are logged for future inspection

        """

        self.assert_connected()

        results = self.queue_adapter.acquire_complete()
        if len(results):
            payload = self._payload_template()

            # Upload new results
            payload["data"] = results
            body = QueueManagerPOSTBody(**payload)
            r = self.client._request("post", "queue_manager", data=body.json(), noraise=True)
            if r.status_code != 200:
                # TODO something as we didnt successfully add the data
                self.logger.warning("Post complete tasks was not successful. Data may be lost.")

            _ = QueueManagerPOSTResponse.parse_raw(r.text)  # Ensure validation from server

            self.active -= len(results)

        self.logger.info("Pushed {} complete tasks to the server.".format(len(results)))

        open_slots = max(0, self.max_tasks - self.active)

        if (new_tasks is False) or (open_slots == 0):
            return True

        # Get new tasks
        payload = self._payload_template()
        payload["data"]["limit"] = open_slots
        body = QueueManagerGETBody(**payload)
        r = self.client._request("get", "queue_manager", data=body.json(), noraise=True)
        if r.status_code != 200:
            # TODO something as we didnt successfully get data
            self.logger.warning("Acquisition of new tasks was not successful.")

        response = QueueManagerGETResponse.parse_raw(r.text)
        new_tasks = response.data

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
        with the associated keys

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
        for k, result in results.items():
            if result.success:
                self.logger.info("  {} - PASSED".format(k))
            else:
                self.logger.error("  {} - FAILED!".format(k))
                failures += 1

        if failures:
            self.logger.error("{}/{} tasks failed!".format(failures, len(results)))
            return False
        else:
            self.logger.info("All tasks completed successfully!")
            return True
