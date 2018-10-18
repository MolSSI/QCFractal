"""
Queue backend abstraction manager.
"""

import logging
import socket
import uuid

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

    def __init__(self, client, queue_client, loop=None, logger=None, max_tasks=1000, queue_tag=None,
                 cluster="unknown"):
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
        """

        # Setup logging
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('QueueManager')

        self.name = {"cluster": cluster, "hostname": socket.gethostname(), "uuid": str(uuid.uuid4())}
        self.name_str = self.name["cluster"] + "-" + self.name["hostname"] + "-" + self.name["uuid"]

        self.client = client
        self.queue_adapter = build_queue_adapter(queue_client, logger=self.logger)
        self.max_tasks = max_tasks
        self.queue_tag = queue_tag

        self.periodic = {}
        self.active = 0

        # Pull the current loop if we need it
        if loop is None:
            self.loop = tornado.ioloop.IOLoop.current()
        else:
            self.loop = loop

        self.logger.info("QueueManager '{}' successfully initialized.\n"
                         "Queue credential username: {}\n"
                         "Pulling tasks from {} with tag '{}'.\n".format(self.name_str, self.client.username,
                                                                         self.client.address, self.queue_tag))

    def start(self):
        """
        Starts up all IOLoops and processes
        """

        self.logger.info("QueueManager successfully started. Starting IOLoop.\n")

        # Add services callback
        update = tornado.ioloop.PeriodicCallback(self.update, 2000)
        update.start()
        self.periodic["update"] = update

        # Soft quit with a keyboard interupt
        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Shuts down all IOLoops and periodic updates
        """
        self.loop.stop()
        for cb in self.periodic.values():
            cb.stop()

        self.shutdown()
        self.logger.info("QueueManager stopping gracefully. Stopped IOLoop.\n")

    def shutdown(self):

        task_ids = [x[0] for x in self.list_current_tasks()]
        if len(task_ids) == 0:
            return True

        payload = {"meta": {"name": self.name_str, "tag": self.queue_tag, "operation": "shutdown"}, "data": task_ids}
        r = self.client._request("put", "queue_manager", payload, noraise=True)
        if r.status_code != 200:
            # TODO something as we didnt successfully add the data
            self.logger.warning("Shutdown was not successful. This may delay queued tasks.")
        else:
            self.logger.info("Shutdown was successful, {} tasks returned to master queue.".format(len(task_ids)))

    def update(self, new_tasks=True):
        """Examines the queue for completed tasks and adds successful completions to the database
        while unsuccessful are logged for future inspection

        """
        results = self.queue_adapter.aquire_complete()
        if len(results):
            payload = {"meta": {"name": self.name_str, "tag": self.queue_tag}, "data": results}
            r = self.client._request("post", "queue_manager", payload, noraise=True)
            if r.status_code != 200:
                # TODO something as we didnt successfully add the data
                self.logger.warning("Post complete tasks was not successful. Data may be lost.")

            self.active -= len(results)


        open_slots = max(0, self.max_tasks - self.active)

        if (new_tasks is False) or (open_slots == 0):
            return True

        # Get new tasks
        payload = {"meta": {"name": self.name_str, "tag": self.queue_tag, "limit": open_slots}, "data": {}}
        r = self.client._request("get", "queue_manager", payload, noraise=True)
        if r.status_code != 200:
            # TODO something as we didnt successfully get data
            self.logger.warning("Aquisition of new tasks was not successful.")

        new_tasks = r.json()["data"]

        # Add new tasks to queue
        self.queue_adapter.submit_tasks(new_tasks)
        self.active += len(new_tasks)
        return True

    def await_results(self):
        """A synchronous method for testing or small launches
        that awaits task completion.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """

        self.update()
        self.queue_adapter.await_results()
        self.update(new_tasks=False)
        return True

    def list_current_tasks(self):
        """Provides a list of tasks currently in the queue along
        with the associated keys

        Returns
        -------
        ret : list of tuples
            All tasks currently still in the database
        """
        return self.queue_adapter.list_tasks()
