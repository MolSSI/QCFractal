"""
Queue backend abstraction manager.
"""

import asyncio
import json
import logging
import socket
import uuid

from typing import Any, Callable, Dict, List, Optional

import tornado.ioloop

from qcfractal import testing
from ..interface.data import get_molecule
from .adapters import build_queue_adapter

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
                 loop: Any=None,
                 logger: Optional[logging.Logger]=None,
                 max_tasks: int=1000,
                 queue_tag: str=None,
                 cluster: str="unknown",
                 update_frequency: int=2):
        """
        Parameters
        ----------
        client : FractalClient
            A Portal client to connect to a server
        queue_client : QueueAdapter
            The DBAdapter class for queue abstraction
        storage_socket : DBSocket
            A socket for the backend database
        loop : IOLoop
            The running Tornado IOLoop
        logger : logging.Logger, Optional. Default: None
            A logger for the QueueManager
        max_tasks : int
            The maximum number of tasks to hold at any given time
        queue_tag : str
            Allows managers to pull from specific tags
        cluster : str
            The cluster the manager belongs to
        update_frequency : int
            The frequency to check for new tasks in seconds
        """

        # Setup logging
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('QueueManager')

        self.name_data = {"cluster": cluster, "hostname": socket.gethostname(), "uuid": str(uuid.uuid4())}
        self._name = self.name_data["cluster"] + "-" + self.name_data["hostname"] + "-" + self.name_data["uuid"]

        self.client = client
        self.queue_adapter = build_queue_adapter(queue_client, logger=self.logger)
        self.max_tasks = max_tasks
        self.queue_tag = queue_tag

        self.update_frequency = update_frequency
        self.periodic = {}
        self.active = 0
        self.exit_callbacks = []

        # Pull the current loop if we need it
        self.loop = loop or tornado.ioloop.IOLoop.current()

        # Build a meta header
        meta_packet = self.name_data.copy()
        meta_packet["tag"] = self.queue_tag
        meta_packet["max_tasks"] = self.max_tasks
        self.meta_packet = json.dumps(meta_packet)

        self.logger.info("QueueManager:")
        self.logger.info("    Name Information:")
        self.logger.info("        cluster:     {}".format(self.name_data["cluster"]))
        self.logger.info("        hostname:    {}".format(self.name_data["hostname"]))
        self.logger.info("        uuid:        {}\n".format(self.name_data["uuid"]))

        self.logger.info("    Queue Adapter:")
        self.logger.info("        {}\n".format(self.queue_adapter))

        # DGAS Note: Note super happy about how this if/else turned out. Looking for alternatives.
        if self.connected():
            # Pull server info
            self.server_info = client.server_information()
            self.server_name = self.server_info["name"]
            self.heartbeat_frequency = self.server_info["heartbeat_frequency"]

            # Tell the server we are up and running
            payload = self._payload_template()
            payload["data"]["operation"] = "startup"
            self.client._request("put", "queue_manager", payload)

            self.logger.info("    QCFractal server information:")
            self.logger.info("        address:     {}".format(self.client.address))
            self.logger.info("        name:        {}".format(self.server_name))
            self.logger.info("        queue tag:   {}".format(self.queue_tag))
            self.logger.info("        username:    {}\n".format(self.client.username))

        else:
            self.logger.info("    QCFractal server information:")
            self.logger.info("        Not connected, some actions will not be available")

    def _payload_template(self):
        return {"meta": json.loads(self.meta_packet), "data": {}}

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

        self.logger.info("QueueManager successfully started. Starting IOLoop.\n")

        # Add services callback, cb freq is given in milliseconds
        update = tornado.ioloop.PeriodicCallback(self.update, 1000 * self.update_frequency)
        update.start()
        self.periodic["update"] = update

        # Add heartbeat
        heartbeat_frequency = int(0.8 * 1000 * self.heartbeat_frequency)  # Beat at 80% of cutoff time
        heartbeat = tornado.ioloop.PeriodicCallback(self.heartbeat, heartbeat_frequency)
        heartbeat.start()
        self.periodic["heartbeat"] = heartbeat

        # Soft quit with a keyboard interupt
        self.running = True
        self.loop.start()

    def stop(self) -> None:
        """
        Shuts down all IOLoops and periodic updates
        """

        # Push data back to the server
        self.shutdown()

        # Close down the adapter
        self.close_adapter()

        # Stop callbacks
        for cb in self.periodic.values():
            cb.stop()

        # Call exit callbacks
        for func, args, kwargs in self.exit_callbacks:
            func(*args, **kwargs)

        # Stop loop
        if not asyncio.get_event_loop().is_running():  # Only works on Py3
            self.loop.stop()

        self.loop.close(all_fds=True)
        self.logger.info("QueueManager stopping gracefully. Stopped IOLoop.\n")

    def close_adapter(self) -> bool:
        """
        Closes down the underlying adapater
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
        r = self.client._request("put", "queue_manager", payload, noraise=True)
        if r.status_code != 200:
            # TODO something as we didnt successfully add the data
            self.logger.warning("Heartbeat was not successful.")

    def shutdown(self) -> Dict[str, Any]:
        """
        Shutsdown the manager and returns tasks to queue.
        """
        self.assert_connected()

        payload = self._payload_template()
        payload["data"]["operation"] = "shutdown"
        r = self.client._request("put", "queue_manager", payload, noraise=True)
        if r.status_code != 200:
            # TODO something as we didnt successfully add the data
            self.logger.warning("Shutdown was not successful. This may delay queued tasks.")
            return {"nshutdown": 0}
        else:
            nshutdown = r.json()["data"]["nshutdown"]
            self.logger.info("Shutdown was successful, {} tasks returned to master queue.".format(nshutdown))
            return r.json()["data"]

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
            payload["data"] = results
            r = self.client._request("post", "queue_manager", payload, noraise=True)
            if r.status_code != 200:
                # TODO something as we didnt successfully add the data
                self.logger.warning("Post complete tasks was not successful. Data may be lost.")

            self.active -= len(results)

        open_slots = max(0, self.max_tasks - self.active)

        if (new_tasks is False) or (open_slots == 0):
            return True

        # Get new tasks
        payload = self._payload_template()
        payload["data"]["limit"] = open_slots
        r = self.client._request("get", "queue_manager", payload, noraise=True)
        if r.status_code != 200:
            # TODO something as we didnt successfully get data
            self.logger.warning("Aquisition of new tasks was not successful.")

        new_tasks = r.json()["data"]

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

    def test(self) -> bool:
        """
        Tests all known programs with simple inputs to check if the Adapter is correctly instantiated.
        """

        self.logger.info("Testing requested, generating tasks")
        task_base = json.dumps({
            "spec": {
                "function":
                "qcengine.compute",
                "args": [{
                    "schema_name": "qc_schema_input",
                    "schema_version": 1,
                    "molecule": get_molecule("hooh.json").to_json(),
                    "driver": "energy",
                    "model": {},
                    "keywords": {},
                    "return_output": False
                }, "program"],
                "kwargs": {}
            },
            "parser": "nothing",
            "hooks": []
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
                found_programs.append(program)
            else:
                self.logger.warning("Could not find program {}, skipping tests.".format(program))
                continue

            task = json.loads(task_base)
            task["id"] = program
            task["spec"]["args"][0]["model"] = model
            task["spec"]["args"][1] = program

            tasks.append(task)

        self.queue_adapter.submit_tasks(tasks)

        self.logger.info("Testing tasks submitting, awaiting results.\n")
        self.queue_adapter.await_results()

        results = self.queue_adapter.acquire_complete()
        self.logger.info("Testing results aquired.")

        missing_programs = results.keys() - set(found_programs)
        if len(missing_programs):
            self.logger.error("Not all tasks were retrieved, missing programs {}.".format(missing_programs))
            raise ValueError("Testing failed, not all tasks were retrived.")
        else:
            self.logger.info("All tasks retrieved successfully.")

        failures = 0
        for k, result in results.items():
            if result[0]["success"]:
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
