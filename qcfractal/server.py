"""
The FractalServer class
"""

import asyncio
import datetime
import logging
import ssl
import traceback

import tornado.ioloop
import tornado.log
import tornado.options
import tornado.web

from .extras import get_information
from .interface import FractalClient
from .queue import QueueManager, QueueManagerHandler, ServiceQueueHandler, TaskQueueHandler
from .services import construct_service
from .storage_sockets import storage_socket_factory
from .web_handlers import (CollectionHandler, InformationHandler, MoleculeHandler, OptionHandler, ProcedureHandler,
                           ResultHandler)

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
        self.storage = storage_socket_factory(
            storage_uri, project_name=storage_project_name, bypass_security=storage_bypass_security)

        # Pull the current loop if we need it
        self.loop = loop or tornado.ioloop.IOLoop.current()

        # Build up the application
        self.objects = {
            "storage_socket": self.storage,
            "logger": self.logger,
        }

        # Public information
        self.objects["public_information"] = {
            "name": self.name,
            "heartbeat_frequency": self.heartbeat_frequency,
            "version": get_information("version")
        }

        endpoints = [

            # Generic web handlers
            (r"/information", InformationHandler, self.objects),
            (r"/molecule", MoleculeHandler, self.objects),
            (r"/keyword", OptionHandler, self.objects),
            (r"/collection", CollectionHandler, self.objects),
            (r"/result", ResultHandler, self.objects),
            (r"/procedure", ProcedureHandler, self.objects),

            # Queue Schedulers
            (r"/task_queue", TaskQueueHandler, self.objects),
            (r"/service_queue", ServiceQueueHandler, self.objects),
            (r"/queue_manager", QueueManagerHandler, self.objects),
        ]

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

        self.logger.info("FractalServer:")
        self.logger.info("    Version:       {}".format(get_information("version")))
        self.logger.info("    Address:       {}".format(self._address))
        self.logger.info("    Database URI:  {}".format(storage_uri))
        self.logger.info("    Database Name: {}\n".format(storage_project_name))
        self.loop_active = False

        # Queue manager if direct build
        self.queue_socket = queue_socket
        self.executor = None
        if (self.queue_socket is not None):
            if security == "local":
                raise ValueError("Cannot yet use local security with a internal QueueManager")

            # Create the executor
            from concurrent.futures import ThreadPoolExecutor
            self.executor = ThreadPoolExecutor(max_workers=2)

            # Build the queue manager, will not run until loop starts
            self.objects["queue_manager_future"] = self._run_in_thread(self._build_manager)

    def _run_in_thread(self, func, timeout=5):
        """
        Runs a function in a background thread
        """
        if self.executor is None:
            raise AttributeError("No Executor was created, but run_in_thread was called.")

        fut = self.executor.submit(func)
        return fut

    def _build_manager(self):
        """
        Async build the manager so it can talk to itself
        """
        # Add the socket to passed args
        client = FractalClient(self._address, verify=self.client_verify)
        self.objects["queue_manager"] = QueueManager(
            client, self.queue_socket, loop=self.loop, logger=self.logger, cluster="FractalServer", verbose=False)

    def start(self):
        """
        Starts up all IOLoops and processes
        """

        # If we have a queue socket start up the nanny
        if self.queue_socket is not None:

            # Add canonical queue callback
            manager = tornado.ioloop.PeriodicCallback(self.update_tasks, 2000)
            manager.start()
            self.periodic["queue_manager_update"] = manager

        # Add services callback
        nanny_services = tornado.ioloop.PeriodicCallback(self.update_services, 2000)
        nanny_services.start()
        self.periodic["update_services"] = nanny_services

        # Add Manager heartbeats
        heartbeats = tornado.ioloop.PeriodicCallback(self.check_manager_heartbeats, self.heartbeat_frequency * 1000)
        heartbeats.start()
        self.periodic["heartbeats"] = heartbeats

        # Soft quit with a keyboard interrupt
        self.logger.info("FractalServer successfully started.\n")
        self.loop_active = True
        self.loop.start()

    def stop(self):
        """
        Shuts down all IOLoops and periodic updates
        """

        # Shut down queue manager
        if self.queue_socket is not None:
            if self.loop_active:
                # This currently doesn't work, we need to rethink
                # how the background thread works
                pass
                # self._run_in_thread(self.objects["queue_manager"].shutdown)
            else:
                self.objects["queue_manager"].shutdown()
                self.objects["queue_manager"].close_adapter()

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
        current_services = self.storage.get_services(status="RUNNING")["data"]

        # Grab new services if we have open slots
        open_slots = max(0, self.max_active_services - len(current_services))
        if open_slots > 0:
            new_services = self.storage.get_services(status="READY", limit=open_slots)["data"]
            current_services.extend(new_services)

        # Loop over the services and iterate
        running_services = 0
        completed_procedures = []
        for data in current_services:

            # Attempt to iteration and get message
            try:
                obj = construct_service(self.storage, data)
                finished = obj.iterate()
                data = obj.json_dict()
            except Exception as e:
                print(traceback.format_exc())
                data["status"] = "ERROR"
                data["error_message"] = "FractalServer Service Build and Iterate Error:\n" + traceback.format_exc()
                finished = False

            self.storage.update_services(data["id"], data)

            if finished is not False:

                # Add results to procedures, remove complete_ids
                completed_procedures.append((data["id"], finished.json_dict()))
            else:
                running_services += 1

        # Add new procedures and services
        # self.storage.add_procedures(new_procedures)
        self.storage.services_completed(completed_procedures)

        return running_services

    def check_manager_heartbeats(self):
        """
        Checks the heartbeats and kills off managers that have not been heard from
        """

        dt = datetime.datetime.utcnow() - datetime.timedelta(seconds=self.heartbeat_frequency)
        ret = self.storage.get_managers(status="ACTIVE", modified_before=dt)

        for blob in ret["data"]:
            nshutdown = self.storage.queue_reset_status(blob["name"])
            self.storage.manager_update(blob["name"], returned=nshutdown, status="INACTIVE")

            self.logger.info("Hearbeat missing from {}. Shutting down, recycling {} incomplete tasks.".format(
                blob["name"], nshutdown))

    def list_managers(self, status=None, name=None):
        """
        Provides a list of managers associated with the server both active and inactive
        """

        return self.storage.get_managers(status=status, name=name)["data"]

### Functions only available if using a local queue_adapter

    def _check_manager(self, func_name):
        if self.queue_socket is None:
            raise AttributeError(
                "{} is only available if the server was initialized with a queue manager.".format(func_name))

        # Pull the manager and delete
        if "queue_manager_future" in self.objects:
            self.logger.info("Waiting on queue_manager to build.")
            self.objects["queue_manager_future"].result()
            del self.objects["queue_manager_future"]

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
            self._run_in_thread(self.objects["queue_manager"].update)
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
