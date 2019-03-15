import atexit
import shutil
import socket
import sys
import subprocess
import tempfile

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tornado.ioloop import IOLoop

from typing import Optional

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
    def __init__(self, max_workers: Optional[int]=1, max_active_services: int=20):
        """A temporary FractalServer that can be used to run complex workflows or try


        ! Warning ! All data is lost when the server is shutdown

        Parameters
        ----------
        max_workers : Optional[int], optional
            The maximum number of ProcessPoolExecutor to spin up.
        max_active_services : int, optional
            Description
        """
        # Startup a MongoDB in background thread and in custom folder.
        mongod_port = _find_port()
        self._mongod_tmpdir = tempfile.TemporaryDirectory()
        self._mongod_proc = _background_process(
            [shutil.which("mongod"), f"--port={mongod_port}", f"--dbpath={self._mongod_tmpdir.name}"])

        queue_socket = None
        if max_workers:
            queue_socket = ProcessPoolExecutor(max_workers=max_workers)

        self.loop = IOLoop()
        self.loop_thread = ThreadPoolExecutor(max_workers=1)
        self.loop_future = self.loop_thread.submit(self.loop.start)

        super().__init__(
            name="QCFractal Snowflake Instance",
            port=_find_port(),
            loop=self.loop,
            storage_uri=f"mongodb://localhost:{mongod_port}",
            storage_project_name="temporary_snowflake",
            max_active_services=max_active_services,
            queue_socket=queue_socket,
            query_limit=int(1.e6))

        self.start(start_loop=False)

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
        self.loop_thread.shutdown()

        if self._mongod_proc is not None:
            self._mongod_proc.kill()
            self._mongod_proc = None

        # Closed down
        self._active = False
        atexit.unregister(self.stop)

    def __del__(self):
        """
        Cleans up the Snowflake instance on delete.
        """

        self.stop()
