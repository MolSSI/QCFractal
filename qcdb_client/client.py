#!/usr/bin/env python

import json
import os
import time
import uuid

import mongo_qcdb as mdb

import distributed

from tornado.options import options, define
import tornado.ioloop
import tornado.web

import compute

compute_file = os.path.abspath(os.path.dirname(__file__)) + os.path.sep + 'compute.py'

define("port", default=8888, help="Run on the given port.", type=int)
define("mongo_project", default="default", help="The Mongod Database instance to open.", type=str)



class DaskNanny(object):
    def __init__(self, dask_socket, mongod_socket):

        self.dask_socket = dask_socket
        self.mongod_socket = mongod_socket
        self.dask_queue = {}

    def add_future(self, future):
        self.dask_queue[str(uuid.uuid4())] = future

    def update(self):
        del_keys = []
        for key, future in self.dask_queue.items():
            if future.done():
                #self.mongod_socket.add_page(future.results())
                print(key, future.result())
                del_keys.append(key)

        for key in del_keys:
            del self.dask_queue[key]
        print(self.dask_queue) 


class POSTHandler(tornado.web.RequestHandler):

    def initialize(self, **data):
        self.data = data

    def post(self):
        print("\nIn post")
        #print(self.data)
        data = json.loads(self.request.body.decode('utf-8'))
        print(data)
        print("\nOut of post")

        dask = self.data["dask_socket"]
        #print(dask)
        dask_nanny = self.data["dask_nanny"]
        #print(dask_nanny)

        fut = dask.submit(compute.psi_compute, data) 

        self.data["dask_nanny"].add_future(fut)

        self.write('OK')

from tornado import gen, httpclient, ioloop
@gen.coroutine
def post(json_data):
    print(json_data)
    client = httpclient.AsyncHTTPClient()
    data = json.dumps(json_data)
    response = yield client.fetch('http://localhost:8888/post',
                                  method='POST',
                                  body=data)

    print(response)



json_data = {}
json_data["molecule"] = """He 0 0 0\n--\nHe 0 0 1"""
json_data["driver"] = "energy"
json_data["args"] = 'SCF'
#json_data["kwargs"] = {"bsse_type": "cp"}
json_data["options"] = {"BASIS": "STO-3G"}
json_data["return_output"] = True

if __name__ == "__main__":
    # Tornado configures logging.
    tornado.options.options.parse_command_line()

    # Build mongo socket 
    mongod_socket = mdb.mongo_helper.MongoSocket("127.0.0.1", 27017, options.mongo_project)
    print(mongod_socket)

    loop = tornado.ioloop.IOLoop.current() 
    dask_socket = distributed.Client("tcp://192.168.2.123:8786")
    print(dask_socket)
    dask_socket.upload_file(compute_file)

    dask_nanny = DaskNanny(dask_socket, mongod_socket)

    app = tornado.web.Application([
        #(r"/post", POSTHandler, {"mongod_socket": "hello"}),
#        (r"/post", POSTHandler, {"mongod_socket": mongod_socket, "dask_nanny": dask_nanny}),
        (r"/post", POSTHandler, {"mongod_socket": mongod_socket, "dask_socket": dask_socket, "dask_nanny": dask_nanny}),
        ],
    )
    app.listen(options.port)
    tornado.ioloop.PeriodicCallback(dask_nanny.update, 2000).start()
    loop.add_callback(post, json_data)
#    loop.add_callback(dask_socket.start)
    print("Here")

    #loop.run_sync(lambda: post(data))
    loop.start()

    time.sleep(10)
    
    print("QCDB Client successfully started")
