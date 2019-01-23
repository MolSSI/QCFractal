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

        # Pull server info
        self.server_info = client.server_information()
        self.server_name = self.server_info["name"]
        self.heartbeat_frequency = self.server_info["heartbeat_frequency"]

        # Build a meta header
        meta_packet = self.name_data.copy()
        meta_packet["tag"] = self.queue_tag
        meta_packet["max_tasks"] = self.max_tasks
        self.meta_packet = json.dumps(meta_packet)

        # Tell the server we are up and running
        payload = self._payload_template()
        payload["data"]["operation"] = "startup"
        self.client._request("put", "queue_manager", payload)

        self.logger.info("QueueManager:")
        self.logger.info("    Name Information:")
        self.logger.info("        cluster:     {}".format(self.name_data["cluster"]))
        self.logger.info("        hostname:    {}".format(self.name_data["hostname"]))
        self.logger.info("        uuid:        {}\n".format(self.name_data["uuid"]))

        self.logger.info("    Queue Adapter:")
        self.logger.info("        {}\n".format(self.queue_adapter))

        if self.connected():
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

    def shutdown(self) -> bool:
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
            return True
        else:
            nshutdown = r.json()["data"]["nshutdown"]
            self.logger.info("Shutdown was successful, {} tasks returned to master queue.".format(nshutdown))
            return False

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
