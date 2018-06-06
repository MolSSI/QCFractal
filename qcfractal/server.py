import logging

import tornado.ioloop
import tornado.web
from tornado import gen

from . import web_handlers
from . import db_sockets

myFormatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


class FractalServer(object):
    def __init__(
            self,

            # Server info options
            port=8888,
            io_loop=None,

            # Mongo options
            db_ip="127.0.0.1",
            db_port=27017,
            db_username=None,
            db_password=None,
            db_type="mongo",
            db_project_name="molssidb",

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

        # Pull the loop if we need it
        if io_loop is None:
            self.loop = tornado.ioloop.IOLoop.current()
        else:
            self.loop = io_loop

        # Secure args

        # Build up the application
        tornado_args = {
            "db_socket": self.db,
            "logger": self.logger,
        }

        self.app = tornado.web.Application([
            # (r"/information", dqm.handlers.Information, tornado_args),
            (r"/molecule", web_handlers.MoleculeHandler, tornado_args),
            (r"/option", web_handlers.OptionHandler, tornado_args),
            (r"/database", web_handlers.DatabaseHandler, tornado_args),
            # (r"/mongod", dqm.handlers.Mongod, tornado_args),
        ], compress_response=True)
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

        self.logger.info("DQM Server stopping gracefully. Stopped IOLoop.\n")

    def stop(self):
        """
        Shuts down all IOLoops
        """
        print("Shutting down")
        self.loop.stop()


if __name__ == "__main__":

    server = FractalServer()
    server.start()
