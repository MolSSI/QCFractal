#!/usr/bin/env python

import json
import os
import time
import uuid
import traceback
import datetime
import logging

import mongo_qcdb as mdb

import distributed

from tornado.options import options, define
import tornado.ioloop
import tornado.web

import compute

compute_file = os.path.abspath(os.path.dirname(__file__)) + os.path.sep + 'compute.py'

define("port", default=8888, help="Run on the given port.", type=int)
define("mongod_ip", default="127.0.0.1", help="The Mongod instances IP.", type=str)
define("mongod_port", default=27017, help="The Mongod instances port.", type=int)
define("dask_ip", default="", help="The Dask instances IP. If blank starts a local cluster.", type=str)
define("dask_port", default=8786, help="The Dask instances port.", type=int)
define("logfile", default="qcdb_server.log", help="The logfile to write to.", type=str)

dask_dir_geuss = os.getcwd() + '/dask_scratch/'
define("dask_dir", default=dask_dir_geuss, help="The Dask workers working director", type=str)
dask_working_dir = options.dask_dir

tornado.options.options.parse_command_line()
tornado.options.parse_command_line()

logging.basicConfig(filename=options.logfile, level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p')


class DaskNanny(object):
    """
    This object can add to the Dask queue and watches for finished jobs. Jobs that are finished
    are automatically posted to the associated MongoDB and removed from the queue.
    """
    def __init__(self, dask_socket, mongod_socket, logger=None):


        self.dask_socket = dask_socket
        self.mongod_socket = mongod_socket
        self.dask_queue = {}
        self.errors = {}

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('DaskNanny')

    def add_future(self, future):
        uid = str(uuid.uuid4())
        self.dask_queue[uid] = future
        self.logger.info("MONGO ADD: FUTURE %s" % uid)
        return uid

    def update(self):
        del_keys = []
        for key, future in self.dask_queue.items():
            if future.done():
                try:
                    tmp_data = future.result()
                    assert tmp_data["success"] == True
                    # res = self.mongod_socket.del_page_by_data(tmp_data)
                    res = self.mongod_socket.add_page(tmp_data)
                    self.logger.info("MONGO ADD: (%s, %s) - %s" % (tmp_data["molecule_hash"],
                                                           tmp_data["modelchem"], str(res)))
                except Exception as e:
                    ename = str(type(e).__name__) + ":" + str(e)
                    msg = "".join(traceback.format_tb(e.__traceback__))
                    msg += str(type(e).__name__) + ":" + str(e)
                    self.errors[key] = msg
                    self.logger.info("MONGO ADD: ERROR\n%s" % msg)

                del_keys.append(key)

        for key in del_keys:
            del self.dask_queue[key]


class Scheduler(tornado.web.RequestHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def initialize(self, **objects):
        self.objects = objects

        if "logger" in list(self.objects):
            self.logger = self.objects["logger"]
            self.objects.pop("logger", None)
        else:
            self.logger = logging.getLogger('Scheduler')

        # Namespaced working dir
        self.working_dir = dask_working_dir

    def _verify_input(self, data, options=None):
        mongo = self.objects["mongod_socket"]

        if options is not None:
            data["options"] = options

        # Check if the minimum is present
        for req in ["molecule_hash", "modelchem", "options"]:
            if req not in list(data):
                err = "Missing required field '%s'" % req
                data["error"] = err
                self.logger.info("SCHEDULER: %s" % err)
                return data

        # Grab out molecule
        molecule = mongo.get_molecule(data["molecule_hash"])
        if molecule is None:
            err = "Molecule hash '%s' was not found." % data["molecule_hash"]
            data["error"] = err
            self.logger.info("SCHEDULER: %s" % err)
            return data

        molecule_str = mdb.Molecule(molecule, dtype="json").to_string(dtype="psi4")

        data["molecule"] = molecule_str
        data["method"] = data["modelchem"]
        data["driver"] = "energy"
        data["working_dir"] = self.working_dir

        return data

    def post(self):

        # Decode the data
        data = json.loads(self.request.body.decode('utf-8'))
        header = self.request.headers

        # Grab objects
        self.objects["mongod_socket"].set_project(header["project"])
        dask = self.objects["dask_socket"]
        dask_nanny = self.objects["dask_nanny"]

        # Parse out data
        program = "psi4"
        tasks = []
        ret = {}
        ret["error"] = []
        ret["Nanny ID"] = []

        # Multiple jobs
        if ("multi_header" in list(data)) and (data["multi_header"] == "QCDB_batch"):
            for task in data["tasks"]:
                tasks.append(self._verify_input(task, options=data["options"]))
            program = data["program"]

        # Single job
        else:
            tasks.append(self._verify_input(data))
            if "program" in list(data):
                program = data["program"]

        # Submit
        for task in tasks:
            if "internal_error" in list(task):
                ret["error"].append(task["internal_error"])
                continue
            fut = dask.submit(compute.computers[program], task)
            ret["Nanny ID"].append(self.objects["dask_nanny"].add_future(fut))

        # Return anything of interest
        ret["success"] = True
        self.write(json.dumps(ret))

    def get(self):

        header = self.request.headers
        self.objects["mongod_socket"].set_project(header["project"])
        dask_nanny = self.objects["dask_nanny"]
        ret = {}
        ret["queue"] = list(dask_nanny.dask_queue)
        ret["error"] = dask_nanny.errors
        self.write(json.dumps(ret))


class Information(tornado.web.RequestHandler):
    def initialize(self, **objects):
        logger = logging.getLogger(__name__)
        logger.info("INFO: %s" % self.request.method)
        self.objects = objects

    def get(self):

        dask = self.objects["dask_socket"]
        mongod = self.objects["mongod_socket"]

        ret = {}
        ret["mongo_data"] = (mongod.url, mongod.port)
        ret["dask_data"] = dask.scheduler.address
        self.write(json.dumps(ret))


class QCDBServer(object):
    def __init__(self):
        # Tornado configures logging.
        tornado.options.options.parse_command_line()

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        handler = logging.FileHandler(options.logfile)
        handler.setLevel(logging.INFO)

        myFormatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        handler.setFormatter(myFormatter)

        self.logger.addHandler(handler)

        self.logger.info("Logfile set to %s\n" % options.logfile)


        # Build mongo socket
        self.mongod_socket = mdb.mongo_helper.MongoSocket(options.mongod_ip, options.mongod_port)

        self.logger.info("Mongod Socket Info:")
        self.logger.info(str(self.mongod_socket) + "\n")

        # Grab the Dask Scheduler
        loop = tornado.ioloop.IOLoop.current()
        self.local_cluster = None
        if options.dask_ip == "":
            self.local_cluster = distributed.LocalCluster(nanny=None)
            self.dask_socket = distributed.Client(self.local_cluster)
        else:
            self.dask_socket = distributed.Client(options.dask_ip + ":" + str(options.dask_port))
        self.dask_socket.upload_file(compute_file)
        self.logger.info("Dask Scheduler Info:")
        self.logger.info(str(self.dask_socket) + "\n")

        # Make sure the scratch is there
        if not os.path.exists(dask_working_dir):
            os.makedirs(dask_working_dir)

        # Dask Nanny
        self.dask_nanny = DaskNanny(self.dask_socket, self.mongod_socket, logger=self.logger)

        # Start up the app
        app = tornado.web.Application(
            [
                (r"/information", Information, {
                    "mongod_socket": self.mongod_socket,
                    "dask_socket": self.dask_socket,
                    "dask_nanny": self.dask_nanny,
                    "logger": self.logger,
                }),
                (r"/scheduler", Scheduler, {
                    "mongod_socket": self.mongod_socket,
                    "dask_socket": self.dask_socket,
                    "dask_nanny": self.dask_nanny,
                    "logger": self.logger,
                }),
            ], )
        app.listen(options.port)

        # Query Dask Nanny on loop
        tornado.ioloop.PeriodicCallback(self.dask_nanny.update, 2000).start()

        # This is for testing
        #loop.add_callback(get, "{data}")
        #loop.add_callback(post, json_data)
        #loop.run_sync(lambda: post(data))

        self.loop = loop
        self.logger.info("QCDB Client successfully initialized at https://localhost:%d.\n" % options.port)

    def start(self):

        self.logger.info("QCDB Client successfully started. Starting IOLoop.\n")

        # Soft quit at the end of a loop
        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.dask_socket.shutdown()
            if self.local_cluster:
                self.local_cluster.close()
            self.loop.stop()

        self.logger.info("QCDB Client stopping gracefully. Stopped IOLoop.\n")


if __name__ == "__main__":

    server = QCDBServer()
    server.start()
