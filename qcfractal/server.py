"""
The FractalServer class
"""

import asyncio
import datetime
import logging
import ssl
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Union

import tornado.ioloop
import tornado.log
import tornado.options
import tornado.web

from .extras import get_information
from .interface import FractalClient
from .queue import QueueManager, QueueManagerHandler, ServiceQueueHandler, TaskQueueHandler, ComputeManagerHandler
from .services import construct_service
from .storage_sockets import ViewHandler, storage_socket_factory
from .storage_sockets.api_logger import API_AccessLogger
from .web_handlers import (
    CollectionHandler,
    InformationHandler,
    KeywordHandler,
    KVStoreHandler,
    MoleculeHandler,
    OptimizationHandler,
    ProcedureHandler,
    ResultHandler,
    WavefunctionStoreHandler,
)


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
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(int(random.random() * sys.maxsize))
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=10 * 365))
        .add_extension(basic_contraints, False)
        .add_extension(alt_names, False)
        .sign(key, hashes.SHA256(), default_backend())
    )  # yapf: disable

    # Build and return keys
    cert_pem = cert.public_bytes(encoding=serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )  # yapf: disable

    return cert_pem, key_pem


class FractalServer:
    def __init__(
        self,
        # Server info options
        name: str = "QCFractal Server",
        port: int = 7777,
        loop: "IOLoop" = None,
        compress_response: bool = True,
        # Security
        security: Optional[str] = None,
        allow_read: bool = False,
        ssl_options: Union[bool, Dict[str, str]] = True,
        # Database options
        storage_uri: str = "postgresql://localhost:5432",
        storage_project_name: str = "qcfractal_default",
        query_limit: int = 1000,
        # View options
        view_enabled: bool = False,
        view_path: Optional[str] = None,
        # Log options
        logfile_prefix: str = None,
        loglevel: str = "info",
        log_apis: bool = False,
        geo_file_path: str = None,
        # Queue options
        queue_socket: "BaseAdapter" = None,
        heartbeat_frequency: float = 1800,
        # Service options
        max_active_services: int = 20,
        service_frequency: float = 60,
        # Testing functions
        skip_storage_version_check=True,
    ):
        """QCFractal initialization

        Parameters
        ----------
        name : str, optional
            The name of the server itself, provided when users query information
        port : int, optional
            The port the server will listen on.
        loop : IOLoop, optional
            Provide an IOLoop to use for the server
        compress_response : bool, optional
            Automatic compression of responses, turn on unless behind a proxy that
            provides this capability.
        security : Optional[str], optional
            The security options for the server {None, "local"}. The local security
            option uses the database to cache users.
        allow_read : bool, optional
            Allow unregistered to perform GET operations on Molecule/KeywordSets/KVStore/Results/Procedures
        ssl_options : Optional[Dict[str, str]], optional
            True, automatically creates self-signed SSL certificates. False, turns off SSL entirely. A user can also supply a dictionary of valid certificates.
        storage_uri : str, optional
            The database URI that the underlying storage socket will connect to.
        storage_project_name : str, optional
            The project name to use on the database.
        query_limit : int, optional
            The maximum number of entries a query will return.
        logfile_prefix : str, optional
            The logfile to use for logging.
        loglevel : str, optional
            The level of logging to output
        queue_socket : BaseAdapter, optional
            An optional Adapter to provide for server to have limited local compute.
            Should only be used for testing and interactive sessions.
        heartbeat_frequency : float, optional
            The time (in seconds) of the heartbeat manager frequency.
        max_active_services : int, optional
            The maximum number of active Services that can be running at any given time.
        service_frequency : float, optional
            The time (in seconds) before checking and updating services.
        """

        # Save local options
        self.name = name
        self.port = port
        if ssl_options is False:
            self._address = "http://localhost:" + str(self.port) + "/"
        else:
            self._address = "https://localhost:" + str(self.port) + "/"

        self.max_active_services = max_active_services
        self.service_frequency = service_frequency
        self.heartbeat_frequency = heartbeat_frequency

        # Setup logging.
        if logfile_prefix is not None:
            tornado.options.options["log_file_prefix"] = logfile_prefix

        tornado.log.enable_pretty_logging()
        self.logger = logging.getLogger("tornado.application")
        self.logger.setLevel(loglevel.upper())

        # Create API Access logger class if enables
        if log_apis:
            self.api_logger = API_AccessLogger(geo_file_path=geo_file_path)
        else:
            self.api_logger = None

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
        if ssl_options is True:
            self.logger.warning("No SSL files passed in, generating self-signed SSL certificate.")
            self.logger.warning("Clients must use `verify=False` when connecting.\n")

            cert, key = _build_ssl()

            # Add quick names
            ssl_name = name.lower().replace(" ", "_")
            cert_name = ssl_name + "_ssl.crt"
            key_name = ssl_name + "_ssl.key"

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
        self.storage_database = storage_project_name
        self.storage_uri = storage_uri
        self.storage = storage_socket_factory(
            storage_uri,
            project_name=storage_project_name,
            bypass_security=storage_bypass_security,
            allow_read=allow_read,
            max_limit=query_limit,
            skip_version_check=skip_storage_version_check,
        )

        if view_enabled:
            self.view_handler = ViewHandler(view_path)
        else:
            self.view_handler = None

        # Pull the current loop if we need it
        self.loop = loop or tornado.ioloop.IOLoop.current()

        # Build up the application
        self.objects = {
            "storage_socket": self.storage,
            "logger": self.logger,
            "api_logger": self.api_logger,
            "view_handler": self.view_handler,
        }

        # Public information
        self.objects["public_information"] = {
            "name": self.name,
            "heartbeat_frequency": self.heartbeat_frequency,
            "version": get_information("version"),
            "query_limit": self.storage.get_limit(1.0e9),
            "client_lower_version_limit": "0.14.0",  # Must be XX.YY.ZZ
            "client_upper_version_limit": "0.14.99",  # Must be XX.YY.ZZ
        }
        self.update_public_information()

        endpoints = [
            # Generic web handlers
            (r"/information", InformationHandler, self.objects),
            (r"/kvstore", KVStoreHandler, self.objects),
            (r"/molecule", MoleculeHandler, self.objects),
            (r"/keyword", KeywordHandler, self.objects),
            (r"/collection(?:/([0-9]+)(?:/(value|entry|list|molecule))?)?", CollectionHandler, self.objects),
            (r"/result", ResultHandler, self.objects),
            (r"/wavefunctionstore", WavefunctionStoreHandler, self.objects),
            (r"/procedure/?", ProcedureHandler, self.objects),
            (r"/optimization/(.*)/?", OptimizationHandler, self.objects),
            # Queue Schedulers
            (r"/task_queue", TaskQueueHandler, self.objects),
            (r"/service_queue", ServiceQueueHandler, self.objects),
            (r"/queue_manager", QueueManagerHandler, self.objects),
            (r"/manager", ComputeManagerHandler, self.objects),
        ]

        # Build the app
        app_settings = {"compress_response": compress_response}
        self.app = tornado.web.Application(endpoints, **app_settings)
        self.endpoints = set([v[0].replace("/", "", 1) for v in endpoints])

        self.http_server = tornado.httpserver.HTTPServer(self.app, ssl_options=ssl_ctx)

        self.http_server.listen(self.port)

        # Add periodic callback holders
        self.periodic = {}

        # Exit callbacks
        self.exit_callbacks = []

        self.logger.info("FractalServer:")
        self.logger.info("    Name:          {}".format(self.name))
        self.logger.info("    Version:       {}".format(get_information("version")))
        self.logger.info("    Address:       {}".format(self._address))
        self.logger.info("    Database URI:  {}".format(storage_uri))
        self.logger.info("    Database Name: {}".format(storage_project_name))
        self.logger.info("    Query Limit:   {}\n".format(self.storage.get_limit(1.0e9)))
        self.loop_active = False

        # Create a executor for background processes
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.futures = {}

        # Queue manager if direct build
        self.queue_socket = queue_socket
        if self.queue_socket is not None:
            if security == "local":
                raise ValueError("Cannot yet use local security with a internal QueueManager")

            def _build_manager():
                client = FractalClient(self, username="qcfractal_server")
                self.objects["queue_manager"] = QueueManager(
                    client,
                    self.queue_socket,
                    logger=self.logger,
                    manager_name="FractalServer",
                    cores_per_task=1,
                    memory_per_task=1,
                    verbose=False,
                )

            # Build the queue manager, will not run until loop starts
            self.futures["queue_manager_future"] = self._run_in_thread(_build_manager)

    def __repr__(self):

        return f"FractalServer(name='{self.name}' uri='{self._address}')"

    def _run_in_thread(self, func, timeout=5):
        """
        Runs a function in a background thread
        """
        if self.executor is None:
            raise AttributeError("No Executor was created, but run_in_thread was called.")

        fut = self.loop.run_in_executor(self.executor, func)
        return fut

    ## Start/stop functionality

    def start(self, start_loop: bool = True, start_periodics: bool = True) -> None:
        """
        Starts up the IOLoop and periodic calls.

        Parameters
        ----------
        start_loop : bool, optional
            If False, does not start the IOLoop
        start_periodics : bool, optional
            If False, does not start the server periodic updates such as
            Service iterations and Manager heartbeat checking.
        """
        if "queue_manager_future" in self.futures:

            def start_manager():
                self._check_manager("manager_build")
                self.objects["queue_manager"].start()

            # Call this after the loop has started
            self._run_in_thread(start_manager)

        # Add services callback
        if start_periodics:
            nanny_services = tornado.ioloop.PeriodicCallback(self.update_services, self.service_frequency * 1000)
            nanny_services.start()
            self.periodic["update_services"] = nanny_services

            # Check Manager heartbeats, 5x heartbeat frequency
            heartbeats = tornado.ioloop.PeriodicCallback(
                self.check_manager_heartbeats, self.heartbeat_frequency * 1000 * 0.2
            )
            heartbeats.start()
            self.periodic["heartbeats"] = heartbeats

            # Log can take some time, update in thread
            def run_log_update_in_thread():
                self._run_in_thread(self.update_server_log)

            server_log = tornado.ioloop.PeriodicCallback(run_log_update_in_thread, self.heartbeat_frequency * 1000)

            server_log.start()
            self.periodic["server_log"] = server_log

        # Build callbacks which are always required
        public_info = tornado.ioloop.PeriodicCallback(self.update_public_information, self.heartbeat_frequency * 1000)
        public_info.start()
        self.periodic["public_info"] = public_info

        # Soft quit with a keyboard interrupt
        self.logger.info("FractalServer successfully started.\n")
        if start_loop:
            self.loop_active = True
            self.loop.start()

    def stop(self, stop_loop: bool = True) -> None:
        """
        Shuts down the IOLoop and periodic updates.

        Parameters
        ----------
        stop_loop : bool, optional
            If False, does not shut down the IOLoop. Useful if the IOLoop is externally managed.
        """

        # Shut down queue manager
        if "queue_manager" in self.objects:
            self._run_in_thread(self.objects["queue_manager"].stop)

        # Close down periodics
        for cb in self.periodic.values():
            cb.stop()

        # Call exit callbacks
        for func, args, kwargs in self.exit_callbacks:
            func(*args, **kwargs)

        # Shutdown executor and futures
        for k, v in self.futures.items():
            v.cancel()

        if self.executor is not None:
            self.executor.shutdown()

        # Shutdown IOLoop if needed
        if (asyncio.get_event_loop().is_running()) and stop_loop:
            self.loop.stop()
        self.loop_active = False

        # Final shutdown
        if stop_loop:
            self.loop.close(all_fds=True)
        self.logger.info("FractalServer stopping gracefully. Stopped IOLoop.\n")

    def add_exit_callback(self, callback, *args, **kwargs):
        """Adds additional callbacks to perform when closing down the server.

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

    ## Helpers

    def get_address(self, endpoint: Optional[str] = None) -> str:
        """Obtains the full URI for a given function on the FractalServer.

        Parameters
        ----------
        endpoint : Optional[str], optional
            Specifies a endpoint to provide the URI for. If None returns the server address.

        Returns
        -------
        str
            The endpoint URI

        """

        if endpoint and (endpoint not in self.endpoints):
            raise AttributeError("Endpoint '{}' not found.".format(endpoint))

        if endpoint:
            return self._address + endpoint
        else:
            return self._address

    ## Updates

    def update_services(self) -> int:
        """Runs through all active services and examines their current status."""

        # Grab current services
        current_services = self.storage.get_services(status="RUNNING")["data"]

        # Grab new services if we have open slots
        open_slots = max(0, self.max_active_services - len(current_services))
        if open_slots > 0:
            new_services = self.storage.get_services(status="WAITING", limit=open_slots)["data"]
            current_services.extend(new_services)
            if len(new_services):
                self.logger.info(f"Starting {len(new_services)} new services.")

        self.logger.debug(f"Updating {len(current_services)} services.")

        # Loop over the services and iterate
        running_services = 0
        completed_services = []
        for data in current_services:

            # Attempt to iteration and get message
            try:
                service = construct_service(self.storage, self.logger, data)
                finished = service.iterate()
            except Exception:
                error_message = "FractalServer Service Build and Iterate Error:\n{}".format(traceback.format_exc())
                self.logger.error(error_message)
                service.status = "ERROR"
                service.error = {"error_type": "iteration_error", "error_message": error_message}
                finished = False

            self.storage.update_services([service])

            # Mark procedure and service as error
            if service.status == "ERROR":
                self.storage.update_service_status("ERROR", id=service.id)

            if finished is not False:
                # Add results to procedures, remove complete_ids
                completed_services.append(service)
            else:
                running_services += 1

        if len(completed_services):
            self.logger.info(f"Completed {len(completed_services)} services.")

        # Add new procedures and services
        self.storage.services_completed(completed_services)

        return running_services

    def update_server_log(self) -> Dict[str, Any]:
        """
        Updates the servers internal log
        """

        return self.storage.log_server_stats()

    def update_public_information(self) -> None:
        """
        Updates the public information data
        """
        data = self.storage.get_server_stats_log(limit=1)["data"]

        counts = {"collection": 0, "molecule": 0, "result": 0, "kvstore": 0}
        if len(data):
            counts["collection"] = data[0].get("collection_count", 0)
            counts["molecule"] = data[0].get("molecule_count", 0)
            counts["result"] = data[0].get("result_count", 0)
            counts["kvstore"] = data[0].get("kvstore_count", 0)

        update = {"counts": counts}
        self.objects["public_information"].update(update)

    def check_manager_heartbeats(self) -> None:
        """
        Checks the heartbeats and kills off managers that have not been heard from.
        """

        dt = datetime.datetime.utcnow() - datetime.timedelta(seconds=self.heartbeat_frequency)
        ret = self.storage.get_managers(status="ACTIVE", modified_before=dt)

        for blob in ret["data"]:
            nshutdown = self.storage.queue_reset_status(manager=blob["name"], reset_running=True)
            self.storage.manager_update(blob["name"], returned=nshutdown, status="INACTIVE")

            self.logger.info(
                "Hearbeat missing from {}. Shutting down, recycling {} incomplete tasks.".format(
                    blob["name"], nshutdown
                )
            )

    def list_managers(self, status: Optional[str] = None, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Provides a list of managers associated with the server both active and inactive.

        Parameters
        ----------
        status : Optional[str], optional
            Filters managers by status.
        name : Optional[str], optional
            Filters managers by name

        Returns
        -------
        List[Dict[str, Any]]
            The requested Manager data.
        """

        return self.storage.get_managers(status=status, name=name)["data"]

    def client(self):
        """
        Builds a client from this server.
        """

        return FractalClient(self)

    ### Functions only available if using a local queue_adapter

    def _check_manager(self, func_name: str) -> None:
        if self.queue_socket is None:
            raise AttributeError(
                "{} is only available if the server was initialized with a queue manager.".format(func_name)
            )

        # Wait up to one second for the queue manager to build
        if "queue_manager" not in self.objects:
            self.logger.info("Waiting on queue_manager to build.")
            for x in range(20):
                time.sleep(0.1)
                if "queue_manager" in self.objects:
                    break

            if "queue_manager" not in self.objects:
                raise AttributeError("QueueManager never constructed.")

    def update_tasks(self) -> bool:
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

    def await_results(self) -> bool:
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

    def await_services(self, max_iter: int = 10) -> bool:
        """A synchronous method that awaits the completion of all services
        before returning.

        Parameters
        ----------
        max_iter : int, optional
            The maximum number of service iterations the server will run through. Will
            terminate early if all services have completed.

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

    def list_current_tasks(self) -> List[Any]:
        """Provides a list of tasks currently in the queue along
        with the associated keys.

        Returns
        -------
        ret : list of tuples
            All tasks currently still in the database
        """
        self._check_manager("list_current_tasks")

        return self.objects["queue_manager"].list_current_tasks()
