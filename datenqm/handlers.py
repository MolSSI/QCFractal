import json
import os
import time
import uuid
import traceback
import datetime
import logging
import pandas as pd

from . import molecule
from . import compute

from tornado.options import options, define
import tornado.ioloop
import tornado.web
import pymongo

# Fireworks
import fireworks


class DaskNanny(object):
    """
    This object can add to the Dask queue and watches for finished jobs. Jobs that are finished
    are automatically posted to the associated MongoDB and removed from the queue.
    """

    def __init__(self, queue_socket, mongod_socket, logger=None):

        self.queue_socket = queue_socket
        self.mongod_socket = mongod_socket
        self.queue = {}
        self.errors = {}

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('DaskNanny')

    def add_future(self, future):
        uid = str(uuid.uuid4())
        self.queue[uid] = future
        self.logger.info("MONGO ADD: FUTURE %s" % uid)
        return uid

    def update(self):
        del_keys = []
        for key, future in self.queue.items():
            if future.done():
                try:
                    tmp_data = future.result()
                    if not tmp_data["success"]:
                        raise ValueError("Computation (%s, %s) did not complete successfully!:\n%s\n" %
                                         (tmp_data["molecule_hash"], tmp_data["modelchem"], tmp_data["error"]))
                    # res = self.mongod_socket.del_page_by_data(tmp_data)
                    res = self.mongod_socket.add_page(tmp_data)
                    self.logger.info("MONGO ADD: (%s, %s) - %s" % (tmp_data["molecule_hash"], tmp_data["modelchem"],
                                                                   str(res)))
                except Exception as e:
                    ename = str(type(e).__name__) + ":" + str(e)
                    msg = "".join(traceback.format_tb(e.__traceback__))
                    msg += str(type(e).__name__) + ":" + str(e)
                    self.errors[key] = msg
                    self.logger.info("MONGO ADD: ERROR\n%s" % msg)

                del_keys.append(key)

        for key in del_keys:
            del self.queue[key]


class FireworksNanny(object):
    """
    This object can add to the Dask queue and watches for finished jobs. Jobs that are finished
    are automatically posted to the associated MongoDB and removed from the queue.
    """

    def __init__(self, queue_socket, mongod_socket, logger=None):

        self.queue_socket = queue_socket
        self.mongod_socket = mongod_socket
        self.queue = []
        self.errors = []

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('FireworksNanny')

    def add_future(self, future):
        self.queue.append(future) # Should be unique via fireworks
        self.logger.info("MONGO ADD: FUTURE %s" % future)
        return uid

    def update(self):

        # Find completed projects
        cursor = self.mongod_socket.fireworks.launches.find({
            "fw_id": {
                "$in": self.queue
            },
            "state": "COMPLETED"
        }, {"action.stored_data.results": True,
            "_id": False,
            "fw_id": True})

        for data in cursor:
            self.queue.remove(data["fw_id"])

            try:
                result_page = data["stored_data"]["results"]
                if not result_page["success"]:
                    raise ValueError("Computation (%s, %s) did not complete successfully!:\n%s\n" %
                                     (result_page["molecule_hash"], result_page["modelchem"], result_page["error"]))

                res = self.mongod_socket.add_page(result_page)
                self.logger.info("MONGO ADD: (%s, %s) - %s" % (result_page["molecule_hash"], result_page["modelchem"],
                                                               str(res)))


            except Exception as e:
                ename = str(type(e).__name__) + ":" + str(e)
                msg = "".join(traceback.format_tb(e.__traceback__))
                msg += str(type(e).__name__) + ":" + str(e)
                self.errors.append(msg)
                self.logger.info("MONGO ADD: ERROR\n%s" % msg)


def _check_auth(objects, header):
    auth = False
    try:
        objects["mongod_socket"].client.database_names()
        username = "default"
        auth = True
    except pymongo.errors.OperationFailure:

        # The authenticate method should match a username and password
        # to a username and password hash in the database users table.
        db = self.objects["mongod_socket"][header["project"]]
        try:
            auth = db.authenticate(header["username"], header["password"])
        except pymongo.errors.OperationFailure:
            auth = False

    if auth is not True:
        raise KeyError("Could not authenticate user.")


def _verify_input(data, mongo, logger=None, options=None):

    if options is not None:
        data["options"] = options

    # Check if the minimum is present
    for req in ["molecule_hash", "modelchem", "options"]:
        if req not in list(data):
            err = "Missing required field '%s'" % req
            data["error"] = err
            if logger:
                logger.info("SCHEDULER: %s" % err)
            return data

    # Grab out molecule
    mol = mongo.get_molecule(data["molecule_hash"])
    if molecule is None:
        err = "Molecule hash '%s' was not found." % data["molecule_hash"]
        data["error"] = err
        if logger:
            logger.info("SCHEDULER: %s" % err)
        return data

    molecule_str = molecule.Molecule(mol, dtype="json").to_string(dtype="psi4")

    data["molecule"] = molecule_str
    data["method"] = data["modelchem"]
    data["driver"] = "energy"

    return data


def _unpack_tasks(data, mongo, logger):
    # Parse out data
    program = "psi4"
    tasks = []

    # Multiple jobs
    if ("multi_header" in list(data)) and (data["multi_header"] == "QCDB_batch"):
        for task in data["tasks"]:
            tasks.append(_verify_input(task, mongo, options=data["options"], logger=logger))
        program = data["program"]

    # Single job
    else:
        tasks.append(_verify_input(data, mongo, logger=logger))
        if "program" in list(data):
            program = data["program"]

    return tasks, program


class DaskScheduler(tornado.web.RequestHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def initialize(self, **objects):
        self.objects = objects

        if "logger" in list(self.objects):
            self.logger = self.objects["logger"]
        else:
            self.logger = logging.getLogger('Scheduler')

    def post(self):

        # Decode the data
        data = json.loads(self.request.body.decode('utf-8'))
        header = self.request.headers
        _check_auth(self.objects, self.request.headers)

        # Grab objects
        self.objects["mongod_socket"].set_project(header["project"])
        dask = self.objects["queue_socket"]
        queue_nanny = self.objects["queue_nanny"]

        tasks, program = _unpack_tasks(data, self.objects["mongod_socket"], self.logger)

        # Submit
        ret = {}
        ret["error"] = []
        ret["Nanny ID"] = []
        for task in tasks:
            if "internal_error" in list(task):
                ret["error"].append(task["internal_error"])
                continue
            fut = dask.submit(compute.computers[program], task)
            ret["Nanny ID"].append(self.objects["queue_nanny"].add_future(fut))

        # Return anything of interest
        ret["success"] = True
        self.write(json.dumps(ret))

    def get(self):

        header = self.request.headers
        _check_auth(self.objects, self.request.headers)

        self.objects["mongod_socket"].set_project(header["project"])
        queue_nanny = self.objects["queue_nanny"]
        ret = {}
        ret["queue"] = list(queue_nanny.queue)
        ret["error"] = queue_nanny.errors
        self.write(json.dumps(ret))


class FireworksScheduler(tornado.web.RequestHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def initialize(self, **objects):
        self.objects = objects

        if "logger" in list(self.objects):
            self.logger = self.objects["logger"]
        else:
            self.logger = logging.getLogger('Scheduler')

    def post(self):

        # Decode the data
        data = json.loads(self.request.body.decode('utf-8'))
        header = self.request.headers
        _check_auth(self.objects, self.request.headers)

        # Grab objects
        self.objects["mongod_socket"].set_project(header["project"])
        dask = self.objects["queue_socket"]
        queue_nanny = self.objects["queue_nanny"]

        tasks, program = _unpack_tasks(data, self.objects["mongod_socket"], self.logger)

        # Submit
        ret = {}
        ret["error"] = []
        ret["Nanny ID"] = []
        for task in tasks:
            if "internal_error" in list(task):
                ret["error"].append(task["internal_error"])
                continue
            fw = fireworks.Firework(
                fireworks.PyTask(func="dqm_compute.run_psi4", args=[task], stored_data_varname="results"))
            fws_id = lpad.add_wf(fw)[-1]
            ret["Nanny ID"].append(self.objects["queue_nanny"].add_future(fws_id))

        # Return anything of interest
        ret["success"] = True
        self.write(json.dumps(ret))

    def get(self):

        header = self.request.headers
        _check_auth(self.objects, self.request.headers)

        self.objects["mongod_socket"].set_project(header["project"])
        queue_nanny = self.objects["queue_nanny"]
        ret = {}
        ret["queue"] = list(queue_nanny.queue)
        ret["error"] = queue_nanny.errors
        self.write(json.dumps(ret))


class Information(tornado.web.RequestHandler):
    """
    Obtains generic information about the Application Objects
    """

    def initialize(self, **objects):
        self.objects = objects

        if "logger" in list(self.objects):
            self.logger = self.objects["logger"]
        else:
            self.logger = logging.getLogger('Information')
        self.logger.info("INFO: %s" % self.request.method)

    def get(self):
        _check_auth(self.objects, self.request.headers)

        dask = self.objects["queue_socket"]
        mongod = self.objects["mongod_socket"]

        ret = {}
        ret["mongo_data"] = (mongod.url, mongod.port)
        ret["dask_data"] = dask.scheduler.address
        self.write(json.dumps(ret))


class Mongod(tornado.web.RequestHandler):
    def initialize(self, **objects):
        self.objects = objects
        if "logger" in list(self.objects):
            self.logger = self.objects["logger"]
        else:
            self.logger = logging.getLogger('Mongod')

    def post(self):

        # Decode the data
        data = json.loads(self.request.body.decode('utf-8'))
        header = self.request.headers
        _check_auth(self.objects, self.request.headers)

        # Grab objects
        mongod = self.objects["mongod_socket"]
        mongod.set_project(header["project"], username=header["username"], password=header["password"])

        self.logger.info("MONGOD: %s - %s" % (self.request.method, data["function"]))

        ret = mongod.json_query(data)
        logger = logging.getLogger(__name__)
        # logger.info("MONGOD: %s" % str(ret))
        if isinstance(ret, (pd.Series, pd.DataFrame)):
            tmp = {}
            tmp["data"] = ret.to_json()
            tmp["pandas_msgpack"] = True
            self.write(json.dumps(tmp))
        else:
            self.write(json.dumps(ret))
