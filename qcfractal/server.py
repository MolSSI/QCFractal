import logging

import tornado.ioloop
import tornado.web
from tornado import gen

from . import web_handlers
from . import db_sockets
from . import queue_handlers

myFormatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


class FractalServer(object):
    def __init__(
            self,

            # Server info options
            port=8888,
            io_loop=None,

            # Database options
            db_ip="127.0.0.1",
            db_port=27017,
            db_username=None,
            db_password=None,
            db_type="mongo",
            db_project_name="molssidb",

            # Queue options
            queue_socket=None,
            queue_type=None,

            # Log options
            logfile_name=None):

        # Save local options
        self.port = port

        # Setup logging.
        self.logger = logging.getLogger("FractalServer")
        self.logger.setLevel(logging.INFO)

        if logfile_name is not None:
            handler = logging.FileHandler(logfile_name.logfile)
            handler.setLevel(logging.INFO)

            handler.setFormatter(myFormatter)

            self.logger.addHandler(handler)

            self.logger.info("Logfile set to %s\n" % logfile_name)

        # Setup the database connection
        self.db = db_sockets.db_socket_factory(
            db_ip, db_port, project_name=db_project_name, username=db_username, password=db_password, db_type=db_type)

        # Pull the current loop if we need it
        if io_loop is None:
            self.loop = tornado.ioloop.IOLoop.current()
        else:
            self.loop = io_loop

        # Secure args

        # Build up the application
        self.objects = {
            "db_socket": self.db,
            "logger": self.logger,
        }

        endpoints = [
            # (r"/information", dqm.handlers.Information, self.objects),
            (r"/molecule", web_handlers.MoleculeHandler, self.objects),
            (r"/option", web_handlers.OptionHandler, self.objects),
            (r"/database", web_handlers.DatabaseHandler, self.objects),
            (r"/result", web_handlers.ResultHandler, self.objects),
        ]

        # Queue handlers
        if (queue_socket is not None) or (queue_type is not None):
            if (queue_socket is None) or (queue_type is None):
                raise KeyError("If either either queue_socket or queue_type is supplied, both must be.")

            queue_nanny, queue_scheduler = queue_handlers.build_queue(queue_type, queue_socket,
                                                                                self.objects["db_socket"])

            # Add the socket to passed args
            self.objects["queue_socket"] = queue_socket
            self.objects["queue_nanny"] = queue_nanny

            # Add the callback to check results
            # self.loop.PeriodicCallback(self.queue_nanny.update, 2000).start()

            # Add the endpoint
            endpoints.append((r"/scheduler", queue_scheduler, self.objects))

        # Build the app
        self.app = tornado.web.Application(endpoints, compress_response=True)

        self.app.listen(self.port)

        # Add in periodic callbacks
        # tornado.ioloop.PeriodicCallback(self.queue_nanny.update, 2000).start()

        self.logger.info("DQM Server successfully initialized at https://localhost:%d.\n" % self.port)

    def start(self):
        """
        Starts up all IOLoops and processes
        """

        self.logger.info("DQM Server successfully started. Starting IOLoop.\n")

        # Soft quit at the end of a loop
        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Shuts down all IOLoops
        """
        self.loop.stop()
        self.logger.info("DQM Server stopping gracefully. Stopped IOLoop.\n")


if __name__ == "__main__":

    server = FractalServer()
    server.start()
