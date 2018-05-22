"""
Contains testing infrastructure for QCFractal
"""

import pytest
import threading
from contextlib import contextmanager
from tornado.ioloop import IOLoop
from .server import FractalServer

_server_port = 8888
test_server_address = "http://localhost:" + str(_server_port) + "/"


@contextmanager
def pristine_loop():
    """
    Builds a clean IOLoop for using as a background request.
    Courtesy of Dask Distributed
    """
    IOLoop.clear_instance()
    IOLoop.clear_current()
    loop = IOLoop()
    loop.make_current()
    assert IOLoop.current() is loop

    try:
        yield loop
    finally:
        try:
            loop.close(all_fds=True)
        except ValueError:
            pass
        IOLoop.clear_instance()
        IOLoop.clear_current()

    db_name = "dqm_local_values_test"


@pytest.fixture(scope="module")
def test_server(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    db_name = "dqm_local_values_test"

    with pristine_loop() as loop:
        # Clean and re-init the databse, manually handle IOLoop (no start/stop needed)
        server = FractalServer(port=_server_port, db_project_name=db_name, io_loop=loop)
        server.db.client.drop_database(server.db._project_name)
        server.db.init_database()

        # Add the IOloop to a thread daemon
        thread = threading.Thread(target=loop.start, name="test IOLoop")
        thread.daemon = True
        thread.start()
        loop_started = threading.Event()
        loop.add_callback(loop_started.set)
        loop_started.wait()

        # Yield the server instance
        yield server

        # Cleanup
        loop.add_callback(loop.stop)
        thread.join(timeout=5)
