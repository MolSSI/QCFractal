import asyncio
import atexit
import shutil
import socket
import sys
import subprocess
import tempfile

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tornado.ioloop import IOLoop

from typing import Optional, Union

from .server import FractalServer


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
                 max_workers: Optional[int]=2,
                 storage_uri: Optional[str]=None,
                 storage_project_name: str="temporary_snowflake",
                 max_active_services: int=20,
                 logging: Union[bool, str]=False,
                 start_server: bool=True):
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
        elif logging is True:
            self.logfile = None
        elif isinstance(logging, str):
            self.logfile = logging
        else:
            raise KeyError(f"Logfile type not recognized {type(logfile)}.")

        super().__init__(
            name="QCFractal Snowflake Instance",
            port=_find_port(),
            loop=self.loop,
            storage_uri=storage_uri,
            storage_project_name=storage_project_name,
            ssl_options=False,
            max_active_services=max_active_services,
            queue_socket=self.queue_socket,
            # logfile_prefix=self.logfile.name,
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
