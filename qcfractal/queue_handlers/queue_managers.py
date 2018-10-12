"""
Queue backend abstraction manager.
"""

import logging
import traceback
import collections

from ..web_handlers import APIHandler
from .. import procedures
from .. import services


class QueueManager:
    """
    This object maintains a computational queue and watches for finished jobs for different
    queue backends. Finished jobs are added to the database and removed from the queue.

    Attributes
    ----------
    storage_socket : StorageSocket
        A socket for the backend storage platform
    queue_adapter : QueueAdapter
        The DBAdapter class for queue abstraction
    errors : dict
        A dictionary of current errors
    logger : logging.logger. Optional, Default: None
        A logger for the QueueManager
    """

    def __init__(self, queue_adapter, storage_socket, logger=None, max_tasks=1000, max_services=20):
        """Summary

        Parameters
        ----------
        queue_adapter : QueueAdapter
            The DBAdapter class for queue abstraction
        storage_socket : DBSocket
            A socket for the backend database
        logger : logging.Logger, Optional. Default: None
            A logger for the QueueManager
        """
        self.queue_adapter = queue_adapter
        self.storage_socket = storage_socket
        self.errors = {}
        self.max_tasks = max_tasks
        self.max_services = max_services

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('QueueManager')

    def update(self):
        """Examines the queue for completed jobs and adds successful completions to the database
        while unsuccessful are logged for future inspection

        """
        results = self.queue_adapter.aquire_complete()

        # Add new jobs to queue
        new_jobs = self.storage_socket.queue_get_next(n=open_slots)
        self.queue_adapter.submit_tasks(new_jobs)

    def list_current_tasks(self):
        """Provides a list of tasks currently in the queue along
        with the associated keys

        Returns
        -------
        ret : list of tuples
            All jobs currently still in the database
        """
        return self.queue_adapter.list_tasks()


def build_queue_adapter(queue_socket, logger=None, **kwargs):
    """Constructs a queue manager based off the incoming queue socket type.

    Parameters
    ----------
    queue_socket : object ("distributed.Client", "fireworks.LaunchPad")
        A object wrapper for different queue types
    logger : logging.Logger, Optional. Default: None
        Logger to report to
    **kwargs
        Additional kwargs for the QueueManager

    Returns
    -------
    ret : (Nanny, Scheduler)
        Returns a valid Nanny and Scheduler for the selected computational queue

    """

    queue_type = type(queue_socket).__module__ + "." + type(queue_socket).__name__

    if queue_type == "distributed.client.Client":
        try:
            import dask.distributed
        except ImportError:
            raise ImportError(
                "Dask.distributed not installed, please install dask.distributed for the dask queue client.")

        from . import dask_handler

        adapter = dask_handler.DaskAdapter(queue_socket)

    elif queue_type == "fireworks.core.launchpad.LaunchPad":
        try:
            import fireworks
        except ImportError:
            raise ImportError("Fireworks not installed, please install fireworks for the fireworks queue client.")

        from . import fireworks_handler

        adapter = fireworks_handler.FireworksAdapter(queue_socket)

    else:
        raise KeyError("Queue type '{}' not understood".format(queue_type))

    return adapter
