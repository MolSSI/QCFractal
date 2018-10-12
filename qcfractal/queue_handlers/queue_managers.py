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

        # Pivot data so that we group all results in categories
        new_results = collections.defaultdict(list)
        error_data = []

        for key, (result, parser, hooks) in self.queue_adapter.aquire_complete().items():
            try:

                # Successful job
                if result["success"] is True:
                    self.logger.info("Update: {}".format(key))
                    result["queue_id"] = key
                    new_results[parser].append((result, hooks))

                # Failed job
                else:
                    if "error" in result:
                        error = result["error"]
                    else:
                        error = "No error supplied"

                    self.logger.info("Computation key did not complete successfully:\n\t{}\n"
                                     "Because: {}".format(str(key), error))

                    error_data.append((key, error))
            except Exception as e:
                msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                self.errors[key] = msg
                self.logger.info("update: ERROR\n{}".format(msg))
                error_data.append((key, msg))

        # Run output parsers
        completed = []
        hooks = []
        for k, v in new_results.items():
            ret = procedures.get_procedure_output_parser(k)(self.storage_socket, v)
            completed.extend(ret[0])
            error_data.extend(ret[1])
            hooks.extend(ret[2])

        # Handle hooks and complete jobs
        self.storage_socket.handle_hooks(hooks)
        self.storage_socket.queue_mark_complete(completed)
        self.storage_socket.queue_mark_error(error_data)

        # Get new jobs
        open_slots = max(0, self.max_tasks - self.queue_adapter.task_count())
        if open_slots == 0:
            return

        # Add new jobs to queue
        new_jobs = self.storage_socket.queue_get_next(n=open_slots)
        self.queue_adapter.submit_tasks(new_jobs)

    def update_services(self):
        """Runs through all active services and examines their current status.
        """

        # Grab current services
        current_services = self.storage_socket.get_services({"status": "RUNNING"})["data"]

        # Grab new services if we have open slots
        open_slots = max(0, self.max_services - len(current_services))
        if open_slots > 0:
            new_services = self.storage_socket.get_services({"status": "READY"}, limit=open_slots)["data"]
            current_services.extend(new_services)

        # Loop over the services and iterate
        running_services = 0
        new_procedures = []
        complete_ids = []
        for data in current_services:
            obj = services.build(data["service"], self.storage_socket, data)

            finished = obj.iterate()
            self.storage_socket.update_services([(data["id"], obj.get_json())])
            # print(obj.get_json())

            if finished is not False:

                # Add results to procedures, remove complete_ids
                new_procedures.append(finished)
                complete_ids.append(data["id"])
            else:
                running_services += 1

        # Add new procedures and services
        self.storage_socket.add_procedures(new_procedures)
        self.storage_socket.del_services(complete_ids)

        return running_services

    def list_current_tasks(self):
        """Provides a list of tasks currently in the queue along
        with the associated keys

        Returns
        -------
        ret : list of tuples
            All jobs currently still in the database
        """
        return self.queue_adapter.list_tasks()


def build_queue_manager(queue_socket, db_socket, logger=None, **kwargs):
    """Constructs a queue manager based off the incoming queue socket type.

    Parameters
    ----------
    queue_socket : object ("distributed.Client", "fireworks.LaunchPad")
        A object wrapper for different queue types
    db_socket : DBSocket
        A socket to the underlying database
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

    manager = QueueManager(adapter, db_socket, logger=logger, **kwargs)

    return manager
