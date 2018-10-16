"""
The FractalServer class
"""

import logging
import ssl

import tornado.ioloop
import tornado.web

from . import queue
from . import services
from . import storage_sockets
from . import web_handlers

myFormatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def _build_ssl():
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    import sys
    import socket
    import datetime
    import ipaddress
    import random

    hostname = socket.gethostname()
    public_ip = ipaddress.ip_address(socket.gethostbyname(hostname))

    key = rsa.generate_private_key(public_exponent=65537, key_size=1024, backend=default_backend())

    alt_name_list = [x509.DNSName(hostname), x509.IPAddress(ipaddress.ip_address(public_ip))]
    alt_names = x509.SubjectAlternativeName(alt_name_list)

    # Basic data
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)])
    basic_contraints = x509.BasicConstraints(ca=True, path_length=0)
    now = datetime.datetime.utcnow()

    # Build cert
    cert = (x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(int(random.random() * sys.maxsize))
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=10*365))
        .add_extension(basic_contraints, False)
        .add_extension(alt_names, False)
        .sign(key, hashes.SHA256(), default_backend())) # yapf: disable

    # Build and return keys
    cert_pem = cert.public_bytes(encoding=serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ) # yapf: disable

    return (cert_pem, key_pem)


class FractalServer:
    def __init__(
            self,

            # Server info options
            port=8888,
            loop=None,
            security=None,
            ssl_options=None,

            # Database options
            storage_ip="127.0.0.1",
            storage_port=27017,
            storage_username=None,
            storage_password=None,
            storage_type="mongo",
            storage_project_name="molssistorage",

            # Queue options
            queue_socket=None,

            # Log options
            logfile_name=None,

            # Queue options
            max_active_services=10):

        # Save local options
        self.port = port
        if ssl_options is False:
            self._address = "http://localhost:" + str(self.port) + "/"
        else:
            self._address = "https://localhost:" + str(self.port) + "/"

        self.max_active_services = max_active_services

        # Setup logging.
        self.logger = logging.getLogger("FractalServer")
        self.logger.setLevel(logging.INFO)

        app_logger = logging.getLogger("tornado.application")
        if logfile_name is not None:
            handler = logging.FileHandler(logfile_name)
            handler.setLevel(logging.INFO)

            handler.setFormatter(myFormatter)

            self.logger.addHandler(handler)
            app_logger.addHandler(handler)

            self.logger.info("Logfile set to {}\n".format(logfile_name))
        else:
            app_logger.addHandler(logging.StreamHandler())
            self.logger.addHandler(logging.StreamHandler())
            self.logger.info("No logfile given, setting output to stdout\n")

        # Build security layers
        if security is None:
            storage_bypass_security = True
        elif security == "local":
            storage_bypass_security = False
        else:
            raise KeyError("Security option '{}' not recognized.".format(security))

        # Handle SSL
        ssl_ctx = None
        if ssl_options is None:
            self.logger.warning("No SSL files passed in, generating self-signed SSL certificate.")
            self.logger.warning("Clients must use `verify=False` when connecting.\n")

            cert, key = _build_ssl()

            # Add quick names
            cert_name = storage_project_name + "_ssl.crt"
            key_name = storage_project_name + "_ssl.key"

            ssl_options = {"crt": cert_name, "key": key_name}

            with open(cert_name, "wb") as handle:
                handle.write(cert)

            with open(key_name, "wb") as handle:
                handle.write(key)

            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(ssl_options["crt"], ssl_options["key"])

            # Destroy keyfiles upon close
            import atexit
            import os
            atexit.register(os.remove, cert_name)
            atexit.register(os.remove, key_name)
        elif ssl_options is False:
            ssl_ctx = None
        elif isinstance(ssl_options, dict):
            if ("crt" not in ssl_options) or ("key" not in ssl_options):
                raise KeyError("'crt' (SSL Certificate) and 'key' (SSL Key) fields are required for `ssl_options`.")

            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(ssl_options["crt"], ssl_options["key"])
        else:
            raise KeyError("ssl_options not understood")

        # Setup the database connection
        self.storage = storage_sockets.storage_socket_factory(
            storage_ip,
            storage_port,
            project_name=storage_project_name,
            username=storage_username,
            password=storage_password,
            storage_type=storage_type,
            bypass_security=storage_bypass_security)

        # Pull the current loop if we need it
        if loop is None:
            self.loop = tornado.ioloop.IOLoop.current()
        else:
            self.loop = loop

        # Build up the application
        self.objects = {
            "storage_socket": self.storage,
            "logger": self.logger,
        }

        endpoints = [

            # Generic web handlers
            (r"/molecule", web_handlers.MoleculeHandler, self.objects),
            (r"/option", web_handlers.OptionHandler, self.objects),
            (r"/collection", web_handlers.CollectionHandler, self.objects),
            (r"/result", web_handlers.ResultHandler, self.objects),
            (r"/procedure", web_handlers.ProcedureHandler, self.objects),
            (r"/locator", web_handlers.LocatorHandler, self.objects),

            # Queue Schedulers
            (r"/task_queue", queue.TaskQueueHandler, self.objects),
            (r"/service_queue", queue.ServiceQueueHandler, self.objects),
            (r"/queue_manager", queue.QueueManagerHandler, self.objects),
        ]

        # Queue manager if direct build
        if queue_socket is not None:

            queue_adapter = queue.build_queue_adapter(queue_socket, logger=self.logger)

            # Add the socket to passed args
            self.objects["queue_socket"] = queue_socket
            self.objects["queue_adapter"] = queue_adapter

        # Build the app
        app_settings = {
            "compress_response": True,
            "serve_traceback": True,
            # "debug": True,
        }
        self.app = tornado.web.Application(endpoints, **app_settings)
        self.endpoints = set([v[0].replace("/", "", 1) for v in endpoints])

        self.http_server = tornado.httpserver.HTTPServer(self.app, ssl_options=ssl_ctx)

        self.http_server.listen(self.port)

        # Add periodic callback holders
        self.periodic = {}

        self.logger.info("FractalServer successfully initialized at {}\n".format(self._address))

    def start(self):
        """
        Starts up all IOLoops and processes
        """

        self.logger.info("FractalServer successfully started. Starting IOLoop.\n")

        # If we have a queue socket start up the nanny
        if "queue_socket" in self.objects:
            # Add canonical queue callback
            nanny = tornado.ioloop.PeriodicCallback(self.update_tasks, 2000)
            nanny.start()
            self.periodic["queue_manager_update"] = nanny

        # Add services callback
        nanny_services = tornado.ioloop.PeriodicCallback(self.update_services, 2000)
        nanny_services.start()
        self.periodic["update_services"] = nanny_services

        # Soft quit with a keyboard interupt
        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Shuts down all IOLoops and periodic updates
        """
        self.loop.stop()
        for cb in self.periodic.values():
            cb.stop()

        self.logger.info("FractalServer stopping gracefully. Stopped IOLoop.\n")

    def get_address(self, endpoint=""):
        """Obtains the full URI for a given function on the FractalServer

        Parameters
        ----------
        endpoint : str, optional
            Specifies a endpoint to provide the URI to

        """

        if len(endpoint) and (endpoint not in self.endpoints):
            raise AttributeError("Endpoint '{}' not found.".format(endpoint))

        return self._address + endpoint

    def update_services(self):
        """Runs through all active services and examines their current status.
        """

        # Grab current services
        current_services = self.storage.get_services({"status": "RUNNING"})["data"]

        # Grab new services if we have open slots
        open_slots = max(0, self.max_active_services - len(current_services))
        if open_slots > 0:
            new_services = self.storage.get_services({"status": "READY"}, limit=open_slots)["data"]
            current_services.extend(new_services)

        # Loop over the services and iterate
        running_services = 0
        new_procedures = []
        complete_ids = []
        for data in current_services:
            obj = services.build(data["service"], self.storage, data)

            finished = obj.iterate()
            self.storage.update_services([(data["id"], obj.get_json())])
            # print(obj.get_json())

            if finished is not False:

                # Add results to procedures, remove complete_ids
                new_procedures.append(finished)
                complete_ids.append(data["id"])
            else:
                running_services += 1

        # Add new procedures and services
        self.storage.add_procedures(new_procedures)
        self.storage.del_services(complete_ids)

        return running_services

### Functions only available if using a local queue_adapter

    def update_tasks(self):
        """Pulls tasks from the queue_adapter, inserts them into the database,
        and fills the queue_adapter with new tasks.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """
        if "queue_adapter" not in self.objects:
            raise AttributeError("update_tasks is only available if the server was initalized with a queue manager.")

        results = self.objects["queue_adapter"].aquire_complete()

        # Call the QueueAPI static method
        queue.QueueManagerHandler.insert_complete_tasks(self.objects["storage_socket"], results, self.logger)

        # Add new tasks to queue
        new_tasks = self.objects["storage_socket"].queue_get_next(n=1000)
        self.objects["queue_adapter"].submit_tasks(new_tasks)

        return True

    def await_results(self):
        """A synchronous method for testing or small launches
        that awaits task completion before adding all queued results
        to the database and returning.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """

        if "queue_adapter" not in self.objects:
            raise AttributeError("await_results is only available if the server was initalized with a queue manager.")

        self.update_tasks()
        self.objects["queue_adapter"].await_results()
        self.update_tasks()
        return True

    def await_services(self, max_iter=10):
        """A synchronous method that awaits the completion of all services
        before returning.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """
        if "queue_adapter" not in self.objects:
            raise AttributeError("await_results is only available if the server was initalized with a queue manager.")

        self.await_results()
        for x in range(1, max_iter + 1):
            self.logger.info("\nAwait services: Iteration {}\n".format(x))
            running_services = self.update_services()
            self.await_results()
            if running_services == 0:
                break

        return True

    def list_current_tasks(self):
        """Provides a list of tasks currently in the queue along
        with the associated keys

        Returns
        -------
        ret : list of tuples
            All tasks currently still in the database
        """
        if "queue_adapter" not in self.objects:
            raise AttributeError(
                "list_current_tasks is only available if the server was initalized with a queue manager.")

        return self.objects["queue_adapter"].list_tasks()

if __name__ == "__main__":

    server = FractalServer()
    server.start()
