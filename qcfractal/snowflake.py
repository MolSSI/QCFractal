import asyncio
import atexit
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Optional, Union

from tornado.ioloop import IOLoop

from .interface import FractalClient
from .server import FractalServer
from .storage_sockets import storage_socket_factory


def _find_port() -> int:
    sock = socket.socket()
    sock.bind(('', 0))
    host, port = sock.getsockname()
    return port


def _port_open(ip: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect((ip, int(port)))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except ConnectionRefusedError:
        return False
    finally:
        s.close()


def _background_process(args, **kwargs):

    if sys.platform.startswith('win'):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP

    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
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
    def __init__(self,
                 max_workers: Optional[int] = 2,
                 storage_uri: Optional[str] = None,
                 storage_project_name: str = "temporary_snowflake",
                 max_active_services: int = 20,
                 logging: Union[bool, str] = False,
                 start_server: bool = True,
                 reset_database: bool = False):
        """A temporary FractalServer that can be used to run complex workflows or try new computations.

        ! Warning ! All data is lost when the server is shutdown.

        Parameters
        ----------
        max_workers : Optional[int], optional
            The maximum number of ProcessPoolExecutor to spin up.
        storage_uri : Optional[str], optional
            A database URI to connect to, otherwise builds a default instance in a
            tempory directory
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

        Raises
        ------
        KeyError
            Description

        """

        # Startup a MongoDB in background thread and in custom folder.
        if storage_uri is None:
            mongod_port = _find_port()
            self._mongod_tmpdir = tempfile.TemporaryDirectory()

            mongod_path = shutil.which("mongod")
            if mongod_path is None:
                raise OSError("Could not find `mongod` in PATH, please `conda install mongodb`.")

            self._mongod_proc = _background_process(
                [mongod_path, f"--port={mongod_port}", f"--dbpath={self._mongod_tmpdir.name}"])
            storage_uri = f"mongodb://localhost:{mongod_port}"
        else:
            self._mongod_tmpdir = None
            self._mongod_proc = None

            if reset_database:
                socket = storage_socket_factory(storage_uri, project_name=storage_project_name)
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
            raise KeyError(f"Logfile type not recognized {type(logfile)}.")

        super().__init__(name="QCFractal Snowflake Instance",
                         port=_find_port(),
                         loop=self.loop,
                         storage_uri=storage_uri,
                         storage_project_name=storage_project_name,
                         ssl_options=False,
                         max_active_services=max_active_services,
                         queue_socket=self.queue_socket,
                         logfile_prefix=log_prefix,
                         query_limit=int(1.e6))

        if self._mongod_proc:
            self.logger.warning("Warning! This is a temporary instance, data will be lost upon shutdown.")

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

        if self._mongod_proc is not None:
            self._mongod_proc.kill()
            self._mongod_proc = None

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
        self._dbdir = tempfile.TemporaryDirectory()
        self._logdir = tempfile.TemporaryDirectory()
        self._dbname = str(uuid.uuid4())
        self._server_port = _find_port()
        self._address = f"https://localhost:{self._server_port}"
        self._ncores = ncores

        # Start up a mongo instance, controlled by the temp file so that it properly dies on shutdown
        mongod_port = _find_port()
        self._mongod_proc = _background_process(
            [shutil.which("mongod"), f"--port={mongod_port}", f"--dbpath={self._dbdir.name}"])
        self._storage_uri = f"mongodb://localhost:{mongod_port}"

        # Set items for the Client
        self.client_verify = False

        self.start()

        # We need to call before threadings cleanup
        atexit.register(self.stop)
        atexit.register(self._kill_mongod)

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
        self._kill_mongod()
        atexit.unregister(self._kill_mongod)

    def __enter__(self) -> 'FractalSnowflakeHandler':
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.stop()
        atexit.unregister(self.stop)
        self._kill_mongod()
        atexit.unregister(self._kill_mongod)
        return False

    def _kill_mongod(self) -> None:
        if self._mongod_proc:
            _terminate_process(self._mongod_proc, timeout=1)
            self._mongod_proc = None


### Utility funcitons

    @property
    def logfilename(self) -> str:
        return os.path.join(self._logdir.name, self._dbname)

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

        # Generate a new database name
        self._dbname = str(uuid.uuid4())


        self._qcfractal_proc = _background_process([
            shutil.which("qcfractal-server"),
            self._dbname,
            f"--server-name={self._dbname}",
            f"--database-uri={self._storage_uri}",
            f"--log-prefix={self.logfilename}",
            f"--port={self._server_port}",
            f"--local-manager={self._ncores}",
            "--query-limit=100000",
        ], cwd=self._logdir.name) # yapf: disable

        client = None
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
            raise ConnectionRefusedError("Snowflake instance did not boot properly, try increasing the timeout.")

        self._running = True

    def stop(self) -> None:
        """
        Stop the current FractalSnowflake instance and destroys all data.
        """
        if self._running is False:
            return

        _terminate_process(self._qcfractal_proc, timeout=1)
        self._running = False

    def restart(self, timeout: int = 5) -> None:
        """
        Restarts the current FractalSnowflake instances and destroys all data in the process.
        """
        self.stop()

        # Make sure we really shut down
        for x in range(timeout * 10):
            if _port_open("localhost", self._server_port):
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
        ] # yapf: disable

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

    def client(self) -> 'FractalClient':
        """
        Builds a client from this server.

        Returns
        -------
        FractalClient
            An active client connected to the server.
        """

        return FractalClient(self)
