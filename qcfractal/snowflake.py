import asyncio
import atexit
import shutil
import socket
import sys
import os
import subprocess
import tempfile
import uuid

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tornado.ioloop import IOLoop

from typing import Optional, Union

from .server import FractalServer
from .storage_sockets import storage_socket_factory


def _find_port() -> int:
    sock = socket.socket()
    sock.bind(('', 0))
    host, port = sock.getsockname()
    return port


def _background_process(args, **kwargs):

    if sys.platform.startswith('win'):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP

    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
    proc = subprocess.Popen(args, **kwargs)

    return proc


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
            self._mongod_proc = _background_process(
                [shutil.which("mongod"), f"--port={mongod_port}", f"--dbpath={self._mongod_tmpdir.name}"])
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

    def __del__(self):
        """
        Cleans up the Snowflake instance on delete.
        """

        self.stop()
        atexit.unregister(self.stop)
        self._kill_mongod()
        atexit.unregister(self._kill_mongod)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    def _kill_mongod(self):
        if self._mongod_proc:
            self._mongod_proc.kill()
            self._mongod_proc = None


### Utility funcitons

    @property
    def logfilename(self):
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

    def start(self):
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
            f"--database-uri={self._storage_uri}",
            f"--log-prefix={self.logfilename}",
            f"--port={self._server_port}",
            f"--local-manager={self._ncores}"
        ]) # yapf: disable

        self._running = True

    def stop(self):
        """
        Stop the current FractalSnowflake instance and destroys all data.
        """
        if self._running is False:
            return

        self._running = False

    def restart(self):
        """
        Restarts the current FractalSnowflake instances and destroys all data in the process.
        """
        self.stop()
        self.start()

    def show_log(self, nlines: int = 20, stream: bool = False, clean: bool = True):
        pass
