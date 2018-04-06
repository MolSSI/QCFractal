"""
Tests the DQM Server class
"""

import pytest
import dqm_server as ds
import pymongo
import threading
from contextlib import contextmanager
from tornado.ioloop import IOLoop

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


@pytest.fixture(scope="module")
def server(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    db_name = "dqm_local_values_test"

    with pristine_loop() as loop:
        # Clean and re-init the databse
        server = ds.DQMServer(db_project_name=db_name, io_loop=loop)
        server.db.client.drop_database(server.db._project_name)
        server.db.init_database()

        thread = threading.Thread(target=loop.start,
                                  name="test IOLoop")
        thread.daemon = True
        thread.start()
        loop_started = threading.Event()
        loop.add_callback(loop_started.set)
        loop_started.wait()

        yield server

        # yield loop
        loop.add_callback(loop.stop)
        thread.join(timeout=5)

def test_molecule(server):

    #print(dir(server))
    #server.start()
    assert 5 == 5
    #server.stop()

