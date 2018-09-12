"""
Queue backend abstraction manager.
"""

import logging
import traceback
import collections

from ..web_handlers import APIHandler
from .. import procedures
from .. import services


class QueueNanny:
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
        A logger for the QueueNanny
    """

    def __init__(self, queue_adapter, storage_socket, logger=None, max_tasks=1000):
        """Summary

        Parameters
        ----------
        queue_adapter : QueueAdapter
            The DBAdapter class for queue abstraction
        storage_socket : DBSocket
            A socket for the backend database
        logger : logging.Logger, Optional. Default: None
            A logger for the QueueNanny
        """
        self.queue_adapter = queue_adapter
        self.storage_socket = storage_socket
        self.errors = {}
        self.services = set()
        self.max_tasks = max_tasks

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('QueueNanny')

    def submit_tasks(self, tasks):
        """Submits tasks to the queue for the Nanny to manage and watch for completion

        Parameters
        ----------
        tasks : dict
            A dictionary of key : JSON job representations

        Returns
        -------
        ret : str
            A list of jobs added to the queue
        """
        tmp = self.storage_socket.queue_submit(tasks)
        self.update()
        self.logger.info("Queue: Added {} tasks.".format(len(tmp)))
        return tmp
        # return self.queue_adapter.submit_tasks(tasks)

    def submit_services(self, tasks):
        """Submits tasks to the queue for the Nanny to manage and watch for completion

        Parameters
        ----------
        tasks : dict
            A dictionary of key : JSON job representations

        Returns
        -------
        ret : str
            A list of jobs added to the queue
        """

        new_tasks = []
        for task in tasks:
            new_tasks.append(task.get_json())

        task_ids = self.storage_socket.add_services(new_tasks)["data"]
        task_ids = [x[1] for x in task_ids]

        self.services |= set(task_ids)

        self.logger.info("Queue: Added {} services.\n".format(len(new_tasks)))
        self.update()

        return task_ids

    def update(self):
        """Examines the queue for completed jobs and adds successful completions to the database
        while unsuccessful are logged for future inspection

        """

        # Pivot data so that we group all results in categories
        new_results = collections.defaultdict(dict)
        complete_ids = []
        error_data = []

        for key, (result, parser, hooks) in self.queue_adapter.aquire_complete().items():
            try:
                if not result["success"]:
                    if "error" in result:
                        error = result["error"]
                    else:
                        error = "No error supplied"

                    self.logger.info("Computation key did not complete successfully:\n\t{}\n"
                                           "Because: {}".format(str(key), error))

                    error_data.append((key, error))
                else:
                    self.logger.info("update: {}".format(key))
                    new_results[parser][key] = (result, hooks)
                    complete_ids.append(key)
            except Exception as e:
                msg = "Internal Server Error:\n"
                msg = "".join(traceback.format_tb(e.__traceback__))
                msg += str(type(e).__name__) + ":" + str(e)
                self.errors[key] = msg
                self.logger.info("update: ERROR\n{}".format(msg))
                error_data.append((key, msg))

        # Run output parsers
        hooks = []
        for k, v in new_results.items():
            ret, h = procedures.get_procedure_output_parser(k)(self.storage_socket, v)
            hooks.extend(h)

        # Handle hooks and complete jobs
        self.storage_socket.handle_hooks(hooks)
        self.storage_socket.queue_mark_complete(complete_ids)
        self.storage_socket.queue_mark_error(error_data)

        # Get new jobs
        open_slots = max(0, self.max_tasks - self.queue_adapter.task_count())
        if open_slots == 0:
            return

        # Submit new jobs
        new_jobs = self.storage_socket.queue_get_next(n=open_slots)
        self.queue_adapter.submit_tasks(new_jobs)

    def update_services(self):
        """Runs through all active services and examines their current status.
        """

        new_procedures = []
        complete_ids = []
        for data in self.storage_socket.get_services(list(self.services), by_id=True)["data"]:
            obj = services.build(data["service"], self.storage_socket, self, data)

            finished = obj.iterate()
            self.storage_socket.update_services([(data["id"], obj.get_json())])
            # print(obj.get_json())

            if finished is not False:
                # Decrement service lookup
                self.services -= {
                    data["id"],
                }

                # Add results to procedures, remove complete_ids
                new_procedures.append(finished)
                complete_ids.append(data["id"])


        self.storage_socket.add_procedures(new_procedures)
        self.storage_socket.del_services(complete_ids)

    def await_results(self):
        """A synchronous method for testing or small launches
        that awaits job completion before adding all queued results
        to the database and returning.

        Returns
        -------
        TYPE
            Description
        """
        self.queue_adapter.await_results()
        self.update()
        return True

    def await_services(self, max_iter=10):
        """A synchronous method for testing or small launches
        that awaits all service completion before adding all service results
        to the database and returning.

        Returns
        -------
        TYPE
            Description
        """

        for x in range(max_iter):
            self.logger.info("\nAwait services {0:d} : {1:s}\n".format(x + 1, str(self.services)))
            self.update_services()
            self.await_results()
            if len(self.services) == 0:
                break

        return True

    def list_current_tasks(self):
        """Provides a list of tasks currently in the queue along
        with the associated keys

        Returns
        -------
        ret : list of tuples
            All jobs currently still in the database
        """
        return self.queue_adapter.list_tasks()


class QueueScheduler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def post(self):
        """Summary
        """
        self.authenticate("compute")

        # Grab objects
        storage = self.objects["storage_socket"]
        queue_nanny = self.objects["queue_nanny"]

        # Format tasks
        full_tasks, errors = procedures.get_procedure_input_parser(self.json["meta"]["procedure"])(storage, self.json)

        # Add tasks to Nanny
        ret = queue_nanny.submit_tasks(full_tasks)
        ret["meta"]["errors"].extend(errors)

        self.write(ret)

    # def get(self):

    #     # _check_auth(self.objects, self.request.headers)

    #     self.objects["db_socket"].set_project(header["project"])
    #     queue_nanny = self.objects["queue_nanny"]
    #     ret = {}
    #     ret["queue"] = list(queue_nanny.queue)
    #     ret["error"] = queue_nanny.errors
    #     self.write(ret)


class ServiceScheduler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def post(self):
        """Summary
        """
        self.authenticate("compute")

        # Grab objects
        storage = self.objects["storage_socket"]
        queue_nanny = self.objects["queue_nanny"]

        # Build return metadata
        meta = {"errors": [], "n_inserted": 0, "success": False, "duplicates": [], "error_description": False}

        ordered_mol_dict = {x: mol for x, mol in enumerate(self.json["data"])}
        mol_query = storage.mixed_molecule_get(ordered_mol_dict)

        new_services = []
        for idx, mol in mol_query["data"].items():
            tmp = services.initializer(self.json["meta"]["service"], storage, queue_nanny, self.json["meta"], mol)
            new_services.append(tmp)

        # Add tasks to Nanny
        submitted = queue_nanny.submit_services(new_services)

        # Return anything of interest
        meta["success"] = True
        meta["n_inserted"] = len(submitted)
        meta["errors"] = []  # TODO
        ret = {"meta": meta, "data": submitted}

        self.write(ret)


def build_queue(queue_socket, db_socket, logger=None, **kwargs):
    """Constructs a queue and nanny based off the incoming queue socket type.

    Parameters
    ----------
    queue_socket : object ("distributed.Client", "fireworks.LaunchPad")
        A object wrapper for different queue types
    db_socket : DBSocket
        A socket to the underlying database
    logger : logging.Logger, Optional. Default: None
        Logger to report to
    **kwargs
        Additional kwargs for the QueueNanny

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

    nanny = QueueNanny(adapter, db_socket, logger=logger, **kwargs)
    queue = QueueScheduler
    service = ServiceScheduler

    return nanny, queue, service
