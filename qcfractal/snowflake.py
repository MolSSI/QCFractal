import asyncio
import atexit
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Optional, Union

from tornado.ioloop import IOLoop

from .interface import FractalClient
from .postgres_harness import TemporaryPostgres
from .server import FractalServer
from .storage_sockets import storage_socket_factory
from .port_util import find_port, is_port_open


def _background_process(args, **kwargs):

    if sys.platform.startswith("win"):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.PIPE
    proc = subprocess.Popen(args, **kwargs)

    return proc


def _terminate_process(proc, timeout: int = 5):
    """
    SIGKILL the process, no shutdown
    """
    if proc.poll() is None:
        proc.send_signal(signal.SIGKILL)
        start = time.time()
        while (proc.poll() is None) and (time.time() < (start + timeout)):
            time.sleep(0.02)

        if proc.poll() is None:
            raise AssertionError(f"Could not kill process {proc.pid}!")


class FractalSnowflake(FractalServer):
    def __init__(
        self,
        max_workers: Optional[int] = 2,
        storage_uri: Optional[str] = None,
        storage_project_name: str = "temporary_snowflake",
        max_active_services: int = 20,
        logging: Union[bool, str] = False,
        start_server: bool = True,
        reset_database: bool = False,
    ):
        """A temporary FractalServer that can be used to run complex workflows or try new computations.

        ! Warning ! All data is lost when the server is shutdown.

        Parameters
        ----------
        max_workers : Optional[int], optional
            The maximum number of ProcessPoolExecutor to spin up.
        storage_uri : Optional[str], optional
            A database URI to connect to, otherwise builds a default instance in a
            temporary directory
        storage_project_name : str, optional
            The database name
        max_active_services : int, optional
            The maximum number of active services
        logging : Union[bool, str], optional
            If True, prints logging information to stdout. If False, hides all logging output. If a filename string is provided the logging will be
            written to this file.
        start_server : bool, optional
            Starts the background asyncio loop or not.
        reset_database : bool, optional
            Resets the database or not if a storage_uri is provided

        """

        # Startup a MongoDB in background thread and in custom folder.
        if storage_uri is None:
            self._storage = TemporaryPostgres(database_name=storage_project_name)
            self._storage_uri = self._storage.database_uri(safe=False, database="")
        else:
            self._storage = None
            self._storage_uri = storage_uri

            if reset_database:
                socket = storage_socket_factory(
                    self._storage_uri, project_name=storage_project_name, skip_version_check=True
                )
                socket._clear_db(socket._project_name)
                del socket

        # Boot workers if needed
        self.queue_socket = None
        if max_workers:
            self.queue_socket = ProcessPoolExecutor(max_workers=max_workers)

        # Add the loop to a background thread and init the server
        self.aioloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.aioloop)
        IOLoop.clear_instance()
        IOLoop.clear_current()
        loop = IOLoop()
        self.loop = loop
        self.loop_thread = ThreadPoolExecutor(max_workers=2)

        if logging is False:
            self.logfile = tempfile.NamedTemporaryFile()
            log_prefix = self.logfile.name
        elif logging is True:
            self.logfile = None
            log_prefix = None
        elif isinstance(logging, str):
            self.logfile = logging
            log_prefix = self.logfile
        else:
            raise KeyError(f"Logfile type not recognized {type(logging)}.")

        self._view_tempdir = tempfile.TemporaryDirectory()

        super().__init__(
            name="QCFractal Snowflake Instance",
            port=find_port(),
            loop=self.loop,
            storage_uri=self._storage_uri,
            storage_project_name=storage_project_name,
            ssl_options=False,
            max_active_services=max_active_services,
            queue_socket=self.queue_socket,
            logfile_prefix=log_prefix,
            service_frequency=2,
            query_limit=int(1.0e6),
            view_enabled=True,
            view_path=self._view_tempdir.name,
        )

        if self._storage:
            self.logger.warning(
                "Warning! This is a temporary instance, data will be lost upon shutdown. "
                "For information about how to set up a permanent QCFractal instance, see "
                "http://docs.qcarchive.molssi.org/projects/qcfractal/en/latest/setup_quickstart.html"
            )

        if start_server:
            self.start(start_loop=False)

        self.loop_future = self.loop_thread.submit(self.loop.start)

        self._active = True

        # We need to call before threadings cleanup
        atexit.register(self.stop)

    def __del__(self):
        """
        Cleans up the Snowflake instance on delete.
        """

        self.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    def stop(self) -> None:
        """
        Shuts down the Snowflake instance. This instance is not recoverable after a stop call.
        """

        if not self._active:
            return

        super().stop(stop_loop=False)
        self.loop.add_callback(self.loop.stop)
        self.loop_future.result()

        self.loop_thread.shutdown()

        if self._storage is not None:
            self._storage.stop()
            self._storage = None

        if self.queue_socket is not None:
            self.queue_socket.shutdown(wait=False)
            self.queue_socket = None

        # Closed down
        self._active = False
        atexit.unregister(self.stop)

    def client(self):
        """
        Builds a client from this server.
        """

        return FractalClient(self)


class FractalSnowflakeHandler:
    def __init__(self, ncores: int = 2):

        # Set variables
        self._running = False
        self._qcfractal_proc = None
        self._storage = TemporaryPostgres()
        self._storage_uri = self._storage.database_uri(safe=False)
        self._qcfdir = None
        self._dbname = None
        self._server_port = find_port()
        self._address = f"https://localhost:{self._server_port}"
        self._ncores = ncores

        # Set items for the Client
        self.client_verify = False

        self.start()

        # We need to call before threadings cleanup
        atexit.register(self.stop)

    ### Dunder functions

    def __repr__(self) -> str:

        return f"FractalSnowflakeHandler(name='{self._dbname}' uri='{self._address}')"

    def _repr_html_(self) -> str:

        return f"""
<h3>FractalSnowflakeHandler</h3>
<ul>
  <li><b>Server:   &nbsp; </b>{self._dbname}</li>
  <li><b>Address:  &nbsp; </b>{self._address}</li>
</ul>
"""

    def __del__(self) -> None:
        """
        Cleans up the Snowflake instance on delete.
        """

        self.stop()
        atexit.unregister(self.stop)

    def __enter__(self) -> "FractalSnowflakeHandler":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.stop()

    ### Utility funcitons

    @property
    def logfilename(self) -> str:
        return os.path.join(self._qcfdir.name, self._dbname)

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

        if endpoint:
            return self._address + endpoint
        else:
            return self._address

    def start(self, timeout: int = 5) -> None:
        """
        Stop the current FractalSnowflake instance and destroys all data.
        """
        if self._running:
            return

        if self._storage is None:
            raise ValueError("This object has been stopped. Please build a new object to continue.")

        if shutil.which("qcfractal-server") is None:
            raise ValueError(
                "qcfractal-server is not installed. This is likely a development environment, please `pip install -e` from the development folder."
            )

        # Generate a new database name and temporary directory
        self._qcfdir = tempfile.TemporaryDirectory()
        self._dbname = "db_" + str(uuid.uuid4()).replace("-", "_")

        # Init
        proc = subprocess.run(
            [
                shutil.which("qcfractal-server"),
                "init",
                f"--base-folder={self._qcfdir.name}",
                f"--port={self._server_port}",
                "--db-own=False",
                f"--db-database-name={self._dbname}",
                f"--db-port={self._storage.config.database.port}",
                "--query-limit=100000",
                "--service-frequency=2",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = proc.stdout.decode()
        if "Success!" not in stdout:
            raise ValueError(
                f"Could not initialize temporary server.\n\nStdout:\n{stdout}\n\nStderr:\n{proc.stderr.decode()}"
            )

        self._qcfractal_proc = _background_process(
            [
                shutil.which("qcfractal-server"),
                "start",
                f"--logfile={self._dbname}",
                f"--base-folder={self._qcfdir.name}",
                f"--server-name={self._dbname}",
                f"--port={self._server_port}",
                f"--local-manager={self._ncores}",
                f"--server-name=FractalSnowFlake_{self._dbname[:8]}",
            ],
            cwd=self._qcfdir.name,
        )  # yapf: disable

        for x in range(timeout * 10):

            try:
                # Client will attempt to connect to the server
                FractalClient(self)
                break
            except ConnectionRefusedError:
                pass

            time.sleep(0.1)
        else:
            self._running = True
            self.stop()
            out, err = self._qcfractal_proc.communicate()
            raise ConnectionRefusedError(
                "Snowflake instance did not boot properly, try increasing the timeout.\n\n"
                f"stdout:\n{out.decode()}\n\n",
                f"stderr:\n{err.decode()}",
            )

        self._running = True

    def stop(self, keep_storage: bool = False) -> None:
        """
        Stop the current FractalSnowflake instance and destroys all data.

        Parameters
        ----------
        keep_storage : bool, optional
            Does not delete the storage object if True.
        """
        if self._running is False:
            return

        if (self._storage is not None) and (keep_storage is False):
            self._storage.stop()
            self._storage = None

        _terminate_process(self._qcfractal_proc, timeout=1)
        self._running = False

    def restart(self, timeout: int = 5) -> None:
        """
        Restarts the current FractalSnowflake instances and destroys all data in the process.
        """
        self.stop(keep_storage=True)

        # Make sure we really shut down
        for x in range(timeout * 10):
            if is_port_open("localhost", self._server_port):
                time.sleep(0.1)
            else:
                break
        else:
            raise ConnectionRefusedError(
                f"Could not start. The current port {self._server_port} is being used by another process. Please construct a new FractalSnowflakeHandler, this error is likely encountered due a bad shutdown of a previous instance."
            )
        self.start()

    def show_log(self, nlines: int = 20, clean: bool = True, show: bool = True):
        """Displays the FractalSnowflakes log data.

        Parameters
        ----------
        nlines : int, optional
            The the last n lines of the log.
        clean : bool, optional
            If True, cleans the log of manager operations where nothing happens.
        show : bool, optional
            If True prints to the log, otherwise returns the result text.

        Returns
        -------
        TYPE
            Description
        """

        with open(self.logfilename, "r") as handle:
            log = handle.read().splitlines()

        _skiplines = [
            "Pushed 0 complete tasks to the server",
            "QueueManager: Served 0 tasks",
            "Acquired 0 new tasks.",
            "Heartbeat was successful.",
            "QueueManager: Heartbeat of manager",
            "GET /queue_manager",
            "PUT /queue_manager",
            "200 GET",
            "200 PUT",
            "200 POST",
            "200 UPDATE",
        ]  # yapf: disable

        ret = []
        if clean:
            for line in log:
                skip = False
                for skips in _skiplines:
                    if skips in line:
                        skip = True
                        break

                if skip:
                    continue
                else:
                    ret.append(line)
        else:
            ret = log

        ret = "\n".join(ret[-nlines:])

        if show:
            print(ret)
        else:
            return ret

    def client(self) -> "FractalClient":
        """
        Builds a client from this server.

        Returns
        -------
        FractalClient
            An active client connected to the server.
        """

        return FractalClient(self)
