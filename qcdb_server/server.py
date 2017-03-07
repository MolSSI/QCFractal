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
define("mongo_project", default="default", help="The Mongod Database instance to open.", type=str)
define("mongod_ip", default="127.0.0.1", help="The Mongod instances IP.", type=str)
define("mongod_port", default=27017, help="The Mongod instances port.", type=int)
define("dask_ip", default="127.0.0.1", help="The Dask instances IP.", type=str)
define("dask_port", default=8786, help="The Dask instances port.", type=int)


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
                    self.mongod_socket.add_page(tmp_data)
                except Exception as e:
                    ename = str( type(e).__name__) + ":" + str(e)
                    msg = "".join(traceback.format_tb(e.__traceback__))
                    msg += str(type(e).__name__) + ":" + str(e)
                    self.errors[key] = msg

                del_keys.append(key)

        for key in del_keys:
            print(self.dask_queue[key].result())
            del self.dask_queue[key]


class Scheduler(tornado.web.RequestHandler):

    def initialize(self, **objects):
        self.objects = objects

    def post(self):

        # Decode the data
        data = json.loads(self.request.body.decode('utf-8'))

        # Grab objects
        dask = self.objects["dask_socket"]
        dask_nanny = self.objects["dask_nanny"]

        # Submit
        fut = dask.submit(compute.psi_compute, data) 
        uid = self.objects["dask_nanny"].add_future(fut)

        # Return anything of interest
        ret = {}
        ret["success"] = True
        ret["Nanny ID"] = uid
        self.write(json.dumps(ret))

class Information(tornado.web.RequestHandler):

    def initialize(self, **objects):
        self.objects = objects

    def get(self):

        dask = self.objects["dask_socket"]
        mongod = self.objects["mongod_socket"]

        ret = {}
        ret["mongo_data"] = (mongod.url, mongod.port, mongod.db_name) 
        ret["dask_data"] = dask.scheduler.address
        self.write(json.dumps(ret))


if __name__ == "__main__":
    # Tornado configures logging.
    tornado.options.options.parse_command_line()

    # Build mongo socket 
    mongod_socket = mdb.mongo_helper.MongoSocket(options.mongod_ip, options.mongod_port, options.mongo_project)
    print("Mongod Socket Info:")
    print(mongod_socket)
    print(" ")

    # Grab the Dask Scheduler
    loop = tornado.ioloop.IOLoop.current() 
    dask_socket = distributed.Client(options.dask_ip + ":" + str(options.dask_port))
    dask_socket.upload_file(compute_file)
    print("Dask Scheduler Info:")
    print(dask_socket)
    print(" ")

    # Dask Nanny
    dask_nanny = DaskNanny(dask_socket, mongod_socket)

    # Start up the app
    app = tornado.web.Application([
        (r"/information", Information, {"mongod_socket": mongod_socket, "dask_socket": dask_socket, "dask_nanny": dask_nanny}),
        (r"/scheduler", Scheduler, {"mongod_socket": mongod_socket, "dask_socket": dask_socket, "dask_nanny": dask_nanny}),
        ],
    )
    app.listen(options.port)

    # Query Dask Nanny on loop
    tornado.ioloop.PeriodicCallback(dask_nanny.update, 2000).start()

    # This is for testing
    #loop.add_callback(get, "{data}")
    #loop.add_callback(post, json_data)
    #loop.run_sync(lambda: post(data))

    print("QCDB Client successfully started. Starting IOLoop.\n")

    # Soft quit at the end of a loop
    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()

    print("QCDB Client stopping gracefully. Stoped IOLoop.\n")
    
