import logging

import tornado.ioloop
import tornado.web

from . import db_sockets
from . import queue_handlers
from . import web_handlers

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

            # Log options
            logfile_name=None):

        # Save local options
        self.port = port
        self._address = "http://localhost:" + str(self.port) + "/"

        # Setup logging.
        self.logger = logging.getLogger("FractalServer")
        self.logger.setLevel(logging.INFO)

        app_logger = logging.getLogger("tornado.application")
        if logfile_name is not None:
            handler = logging.FileHandler(logfile_name.logfile)
            handler.setLevel(logging.INFO)

            handler.setFormatter(myFormatter)

            self.logger.addHandler(handler)
            app_logger.addHandler(handler)

            self.logger.info("Logfile set to %s\n" % logfile_name)
        else:
            app_logger.addHandler(logging.StreamHandler())
            self.logger.addHandler(logging.StreamHandler())
            self.logger.info("No logfile given, setting output to stdout")

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
            (r"/service", web_handlers.ServiceHandler, self.objects),
        ]

        # Queue handlers
        if queue_socket is not None:

            queue_nanny, queue_scheduler, service_scheduler = queue_handlers.build_queue(queue_socket,
                                                                                         self.objects["db_socket"])

            # Add the socket to passed args
            self.objects["queue_socket"] = queue_socket
            self.objects["queue_nanny"] = queue_nanny

            # Add the callback to check results
            # self.loop.PeriodicCallback(self.queue_nanny.update, 2000).start()

            # Add the endpoint
            endpoints.append((r"/scheduler", queue_scheduler, self.objects))
            endpoints.append((r"/service_scheduler", service_scheduler, self.objects))

        # Build the app
        app_settings = {
            "compress_response": True,
            "serve_traceback": True,
            # "debug": True,
        }
        self.app = tornado.web.Application(endpoints, **app_settings)

        self.app.listen(self.port)

        # Add in periodic callbacks

        self.logger.info("DQM Server successfully initialized at https://localhost:%d.\n" % self.port)
        self.periodic = {}

    def start(self):
        """
        Starts up all IOLoops and processes
        """

        self.logger.info("DQM Server successfully started. Starting IOLoop.\n")

        # Add canonical queue callback
        nanny = tornado.ioloop.PeriodicCallback(self.objects["queue_nanny"].update, 2000)
        nanny.start()
        self.periodic["queue_nanny_update"] = nanny

        # Add services callback
        nanny_services = tornado.ioloop.PeriodicCallback(self.objects["queue_nanny"].update_services, 2000)
        nanny_services.start()
        self.periodic["queue_nanny_services"] = nanny_services

        # Soft quit with a keyboard interupt
        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Shuts down all IOLoops
        """
        self.loop.stop()
        for cb in self.periodic.values():
            cb.stop()

        self.logger.info("DQM Server stopping gracefully. Stopped IOLoop.\n")

    def get_address(self, function=""):
        return self._address + function


if __name__ == "__main__":

    server = FractalServer()
    server.start()
