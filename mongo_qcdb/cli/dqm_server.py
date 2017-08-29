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
