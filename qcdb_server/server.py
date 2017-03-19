#!/usr/bin/env python

import json
import os
import time
import uuid
import traceback

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
define(
    "dask_ip", default="", help="The Dask instances IP. If blank starts a local cluster.", type=str)
define("dask_port", default=8786, help="The Dask instances port.", type=int)

dask_dir_geuss = os.getcwd() + '/dask_scratch/'
define("dask_dir", default=dask_dir_geuss, help="The Dask workers working director", type=str)
dask_working_dir = options.dask_dir


class DaskNanny(object):
    def __init__(self, dask_socket, mongod_socket):

        self.dask_socket = dask_socket
        self.mongod_socket = mongod_socket
        self.dask_queue = {}
        self.errors = {}

    def add_future(self, future):
        uid = str(uuid.uuid4())
        self.dask_queue[uid] = future
        return uid

    def update(self):
        del_keys = []
        for key, future in self.dask_queue.items():
            if future.done():
                try:
                    tmp_data = future.result()
                    #print(tmp_data)
                    assert tmp_data["success"] == True
                    # res = self.mongod_socket.del_page_by_data(tmp_data)
                    res = self.mongod_socket.add_page(tmp_data)
                    print("MONGO: ADD (%s, %s) - %s" % (tmp_data["molecule_hash"], tmp_data["modelchem"], str(res)) )
                except Exception as e:
                    ename = str(type(e).__name__) + ":" + str(e)
                    msg = "".join(traceback.format_tb(e.__traceback__))
                    msg += str(type(e).__name__) + ":" + str(e)
                    self.errors[key] = msg

                del_keys.append(key)

        for key in del_keys:
            # print(self.dask_queue[key].result())
            del self.dask_queue[key]


class Scheduler(tornado.web.RequestHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """


    def initialize(self, **objects):
        print("SCHEDULER: %s (%d bytes)" % (self.request.method, len(self.request.body)))
        self.objects = objects

        # Namespaced working dir
        self.working_dir = dask_working_dir

    def _verify_input(self, data, options=None):
        mongo = self.objects["mongod_socket"]

        if options is not None:
            data["options"] = options

        # Check if the minimum is present
        for req in ["molecule_hash", "modelchem", "options"]:
            if req not in list(data):
                data["error"] = "Missing required field '%s'" % req
                return data

        # Grab out molecule
        molecule = mongo.get_molecule(data["molecule_hash"])
        if molecule is None:
            data["error"] = "Molecule hash '%s' was not found." % data["molecule_hash"]
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
        # print("INFO " + repr(self.request))
        print("INFO: " + self.request.method)
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

        # Build mongo socket
        self.mongod_socket = mdb.mongo_helper.MongoSocket(options.mongod_ip, options.mongod_port)

        print("Mongod Socket Info:")
        print(self.mongod_socket)
        print(" ")

        # Grab the Dask Scheduler
        loop = tornado.ioloop.IOLoop.current()
        if options.dask_ip == "":
            self.local_cluster = distributed.LocalCluster(nanny=False)
            self.dask_socket = distributed.Client(self.local_cluster)
        else:
            self.dask_socket = distributed.Client(options.dask_ip + ":" + str(options.dask_port))
        self.dask_socket.upload_file(compute_file)
        print("Dask Scheduler Info:")
        print(self.dask_socket)
        print(" ")

        # Make sure the scratch is there
        if not os.path.exists(dask_working_dir):
            os.makedirs(dask_working_dir)

        # Dask Nanny
        self.dask_nanny = DaskNanny(self.dask_socket, self.mongod_socket)

        # Start up the app
        app = tornado.web.Application([
            (r"/information", Information, {
                "mongod_socket": self.mongod_socket,
                "dask_socket": self.dask_socket,
                "dask_nanny": self.dask_nanny
            }),
            (r"/scheduler", Scheduler, {
                "mongod_socket": self.mongod_socket,
                "dask_socket": self.dask_socket,
                "dask_nanny": self.dask_nanny
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
        print("QCDB Client successfully initialized at https://localhost:%d.\n" % options.port)

    def start(self):

        print("QCDB Client successfully started. Starting IOLoop.\n")

        # Soft quit at the end of a loop
        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.dask_socket.shutdown()
            if options.dask_ip == "":
                self.local_cluster.close()
            self.loop.stop()

        print("\nQCDB Client stopping gracefully. Stopped IOLoop.\n")


if __name__ == "__main__":

    server = QCDBServer()
    server.start()
