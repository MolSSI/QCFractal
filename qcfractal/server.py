"""
The FractalServer class
"""

import asyncio
import datetime
import logging
import ssl
import threading
import traceback

import tornado.ioloop
import tornado.log
import tornado.options
import tornado.web

from . import interface
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

    return cert_pem, key_pem


class FractalServer:
    def __init__(
            self,

            # Server info options
            name="QCFractal Server",
            port=8888,
            loop=None,
            security=None,
            ssl_options=None,

            # Database options
            storage_uri="mongodb://localhost",
            storage_project_name="molssistorage",

            # Log options
            logfile_prefix=None,

            # Queue options
            queue_socket=None,
            max_active_services=10,
            heartbeat_frequency=300):

        # Save local options
        self.name = name
        self.port = port
        if ssl_options is False:
            self._address = "http://localhost:" + str(self.port) + "/"
        else:
            self._address = "https://localhost:" + str(self.port) + "/"

        self.max_active_services = max_active_services
        self.heartbeat_frequency = heartbeat_frequency

        # Setup logging.
        if logfile_prefix is not None:
            tornado.options.options['log_file_prefix'] = logfile_prefix

        tornado.log.enable_pretty_logging()
        self.logger = logging.getLogger("tornado.application")

        # Build security layers
        if security is None:
            storage_bypass_security = True
        elif security == "local":
            storage_bypass_security = False
        else:
            raise KeyError("Security option '{}' not recognized.".format(security))

        # Handle SSL
        ssl_ctx = None
        self.client_verify = True
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
            self.client_verify = False
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
            storage_uri, project_name=storage_project_name, bypass_security=storage_bypass_security)
        self.logger.info("Connected to '{}'' with database name '{}'\n.".format(storage_uri, storage_project_name))

        # Pull the current loop if we need it
        self.loop = loop or tornado.ioloop.IOLoop.current()

        # Build up the application
        self.objects = {
            "storage_socket": self.storage,
            "logger": self.logger,
        }

        # Public information
        self.objects["public_information"] = {"name": self.name, "heartbeat_frequency": self.heartbeat_frequency}

        endpoints = [

            # Generic web handlers
            (r"/information", web_handlers.InformationHandler, self.objects),
            (r"/molecule", web_handlers.MoleculeHandler, self.objects),
            (r"/option", web_handlers.OptionHandler, self.objects),
            (r"/collection", web_handlers.CollectionHandler, self.objects),
            (r"/result", web_handlers.ResultHandler, self.objects),
            (r"/procedure", web_handlers.ProcedureHandler, self.objects),

            # Queue Schedulers
            (r"/task_queue", queue.TaskQueueHandler, self.objects),
            (r"/service_queue", queue.ServiceQueueHandler, self.objects),
            (r"/queue_manager", queue.QueueManagerHandler, self.objects),
        ]

        # Queue manager if direct build
        if queue_socket is not None:

            if security == "local":
                raise ValueError("Cannot yet use local security with a internal QueueManager")

            # Add the socket to passed args
            client = interface.FractalClient(self._address, verify=self.client_verify)
            self.objects["queue_manager"] = queue.QueueManager(
                client, queue_socket, loop=loop, logger=self.logger, cluster="FractalServer")

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

        # Exit callbacks
        self.exit_callbacks = []

        self.logger.info("FractalServer successfully initialized at {}".format(self._address))
        self.loop_active = False

    def start(self):
        """
        Starts up all IOLoops and processes
        """

        self.logger.info("FractalServer successfully started. Starting IOLoop.\n")

        # If we have a queue socket start up the nanny
        if "queue_manager" in self.objects:
            # Add canonical queue callback
            manager = tornado.ioloop.PeriodicCallback(self.update_tasks, 2000)
            manager.start()
            self.periodic["queue_manager_update"] = manager

        # Add services callback
        nanny_services = tornado.ioloop.PeriodicCallback(self.update_services, 2000)
        nanny_services.start()
        self.periodic["update_services"] = nanny_services

        # Add Manager heartbeats
        heartbeats = tornado.ioloop.PeriodicCallback(self.manager_heartbeats, self.heartbeat_frequency * 1000)
        heartbeats.start()
        self.periodic["heartbeats"] = heartbeats

        # Soft quit with a keyboard interrupt
        try:
            self.loop_active = True
            if not asyncio.get_event_loop().is_running():  # Only works on Py3
                self.loop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Shuts down all IOLoops and periodic updates
        """

        # Shut down queue manager
        if "queue_manager" in self.objects:
            if self.loop_active:
                # Drop this in a thread so that we are not blocking eachother
                thread = threading.Thread(target=self.objects["queue_manager"].shutdown, name="QueueManager Shutdown")
                thread.daemon = True
                thread.start()
                self.loop.call_later(5, thread.join)
            else:
                self.objects["queue_manager"].shutdown()

        # Close down periodics
        for cb in self.periodic.values():
            cb.stop()

        # Call exit callbacks
        for func, args, kwargs in self.exit_callbacks:
            func(*args, **kwargs)

        # Shutdown IOLoop if needed
        if asyncio.get_event_loop().is_running():
            self.loop.stop()
        self.loop_active = False

        # Final shutdown
        self.loop.close(all_fds=True)
        self.logger.info("FractalServer stopping gracefully. Stopped IOLoop.\n")

    def add_exit_callback(self, callback, *args, **kwargs):
        """Adds additional callbacks to perform when closing down the server

        Parameters
        ----------
        callback : callable
            The function to call at exit
        *args
            Arguments to call with the function.
        **kwargs
            Kwargs to call with the function.

        """
        self.exit_callbacks.append((callback, args, kwargs))

    def get_address(self, endpoint=None):
        """Obtains the full URI for a given function on the FractalServer

        Parameters
        ----------
        endpoint : str, optional
            Specifies a endpoint to provide the URI to

        """

        if endpoint and (endpoint not in self.endpoints):
            raise AttributeError("Endpoint '{}' not found.".format(endpoint))

        if endpoint:
            return self._address + endpoint
        else:
            return self._address

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

            # Attempt to iteration and get message
            try:
                obj = services.build(data["service"], self.storage, data)
                finished = obj.iterate()
                data = obj.get_json()
            except Exception as e:
                print(traceback.format_exc())
                data["status"] = "ERROR"
                data["error_message"] = "FractalServer Service Build and Iterate Error:\n" + traceback.format_exc()
                finished = False

            self.storage.update_services([(data["id"], data)])

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

    def manager_heartbeats(self):

        dt = datetime.datetime.utcnow() - datetime.timedelta(seconds=self.heartbeat_frequency)
        print()
        print(dt)

        print(self.storage.get_managers({})["data"])
        ret = self.storage.get_managers({"modifed_on": {"$lt": dt}, "status": "ACTIVE"}, projection={"name": True})
        print(ret["data"])

    def list_managers(self, status=None, name=None):
        """
        Provides a list of managers associated with the server both active and inactive
        """
        query = {}
        if status:
            query["status"] = status.upper()
        if name:
            query["name"] = name

        return self.storage.get_managers(query)["data"]

### Functions only available if using a local queue_adapter

    def _check_manager(self, func_name):
        if "queue_manager" not in self.objects:
            raise AttributeError(
                "{} is only available if the server was initialized with a queue manager.".format(func_name))

    def update_tasks(self):
        """Pulls tasks from the queue_adapter, inserts them into the database,
        and fills the queue_adapter with new tasks.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """
        self._check_manager("update_tasks")

        if self.loop_active:
            # Drop this in a thread so that we are not blocking each other
            thread = threading.Thread(target=self.objects["queue_manager"].update, name="QueueManager Update")
            thread.daemon = True
            thread.start()
            self.loop.call_later(5, thread.join)
        else:
            self.objects["queue_manager"].update()

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
        self._check_manager("await_results")

        self.logger.info("Updating tasks")
        return self.objects["queue_manager"].await_results()

    def await_services(self, max_iter=10):
        """A synchronous method that awaits the completion of all services
        before returning.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """
        self._check_manager("await_services")

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
        self._check_manager("list_current_tasks")

        return self.objects["queue_manager"].list_current_tasks()
