"""
Contains testing infrastructure for QCFractal
"""

import os
import logging
import pkgutil
import socket
import threading
import pymongo
from contextlib import contextmanager

import pytest
from tornado.ioloop import IOLoop

from .storage_sockets import storage_socket_factory
from .server import FractalServer

### Addon testing capabilities


def _plugin_import(plug):
    plug_spec = pkgutil.find_loader(plug)
    if plug_spec is None:
        return False
    else:
        return True


_import_message = "Not detecting module {}. Install package if necessary and add to envvar PYTHONPATH"

# Figure out what is imported
_programs = {
    "fireworks": _plugin_import("fireworks"),
    "rdkit": _plugin_import("rdkit"),
    "psi4": _plugin_import("psi4"),
    "dask": _plugin_import("dask"),
    "geometric": _plugin_import("geometric"),
    "torsiondrive": _plugin_import("torsiondrive"),
}
if _programs["dask"]:
    _programs["dask.distributed"] = _plugin_import("dask.distributed")
else:
    _programs["dask.distributed"] = False


def has_module(name):
    return _programs[name]

# Add a number of module testing options
using_fireworks = pytest.mark.skipif(has_module('fireworks') is False, reason=_import_message.format('fireworks'))
using_dask = pytest.mark.skipif(
    has_module('dask.distributed') is False, reason=_import_message.format('dask.distributed'))
using_psi4 = pytest.mark.skipif(has_module('psi4') is False, reason=_import_message.format('psi4'))
using_rdkit = pytest.mark.skipif(has_module('rdkit') is False, reason=_import_message.format('rdkit'))
using_geometric = pytest.mark.skipif(has_module('geometric') is False, reason=_import_message.format('geometric'))
using_torsiondrive = pytest.mark.skipif(has_module('torsiondrive') is False, reason=_import_message.format('torsiondrive'))
using_unix = pytest.mark.skipif(os.name.lower() != 'posix', reason='Not on Unix operating system, '
                                                                   'assuming Bash is not present')

# Check for MongoDB connection
def check_active_mongo_server():

    client = pymongo.MongoClient("localhost:27017", serverSelectionTimeoutMS=100)
    try:
        client.server_info()
    except:
        pytest.skip("Could not find an activate mongo test instance at 'localhost:27017'.")

### Server testing mechanics


def find_open_port():
    """
    Use socket's built in ability to find an open port.
    """
    sock = socket.socket()
    sock.bind(('', 0))

    host, port = sock.getsockname()

    return port


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
        except (ValueError, KeyError):
            pass
        IOLoop.clear_instance()
        IOLoop.clear_current()


@contextmanager
def active_loop(loop):
    # Add the IOloop to a thread daemon
    thread = threading.Thread(target=loop.start, name="test IOLoop")
    thread.daemon = True
    thread.start()
    loop_started = threading.Event()
    loop.add_callback(loop_started.set)
    loop_started.wait()

    try:
        yield loop
    finally:
        try:
            loop.add_callback(loop.stop)
            thread.join(timeout=5)
        except:
            pass

@pytest.fixture(scope="module")
def test_server(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    storage_name = "qcf_local_server_test"

    with pristine_loop() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = FractalServer(port=find_open_port(),
                               storage_project_name=storage_name,
                               io_loop=loop,
                               ssl_options=False)

        # Clean and re-init the database
        server.storage.client.drop_database(server.storage._project_name)
        server.storage.init_database()

        with active_loop(loop) as act:
            yield server


@using_dask
@pytest.fixture(scope="module")
def dask_server_fixture(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    dd = pytest.importorskip("dask.distributed")

    storage_name = "qcf_dask_server_test"

    with pristine_loop() as loop:

        # LocalCluster will start the loop in a background thread.
        with dd.LocalCluster(n_workers=1, threads_per_worker=1, loop=loop) as cluster:

            # Build a Dask Client
            client = dd.Client(cluster)

            # Build server, manually handle IOLoop (no start/stop needed)
            server = FractalServer(
                port=find_open_port(),
                storage_project_name=storage_name,
                io_loop=cluster.loop,
                queue_socket=client,
                ssl_options=False)

            # Clean and re-init the databse
            server.storage.client.drop_database(server.storage._project_name)
            server.storage.init_database()

            # Yield the server instance
            yield server

            client.close()


@using_fireworks
@pytest.fixture(scope="module")
def fireworks_server_fixture(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    fireworks = pytest.importorskip("fireworks")
    logging.basicConfig(level=logging.CRITICAL, filename="/tmp/fireworks_logfile.txt")

    lpad = fireworks.LaunchPad(name="fw_testing_server", logdir="/tmp/", strm_lvl="CRITICAL")
    lpad.reset(None, require_password=False)

    storage_name = "qcf_fireworks_server_test"

    with pristine_loop() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = FractalServer(
            port=find_open_port(), storage_project_name=storage_name,
            io_loop=loop, queue_socket=lpad, ssl_options=False)

        # Clean and re-init the databse
        server.storage.client.drop_database(server.storage._project_name)
        server.storage.init_database()

        # Yield the server instance
        with active_loop(loop) as act:
            yield server

    lpad.reset(None, require_password=False)
    logging.basicConfig(level=None, filename=None)


# @pytest.fixture(scope="module", params=["dask"])
# @pytest.fixture(scope="module", params=["fireworks"])
@pytest.fixture(scope="module", params=["dask", "fireworks"])
def fractal_compute_server(request):
    if request.param == "dask":
        yield from dask_server_fixture(request)
    elif request.param == "fireworks":
        yield from fireworks_server_fixture(request)
    else:
        raise TypeError("fractal_compute_server: internal parametrize error")


@pytest.fixture(scope="module", params=["mongo"])
def storage_socket_fixture(request):
    print("")

    # Check mongo
    check_active_mongo_server()
    storage_name = "qcf_local_values_test"

    # IP/port/drop table is specific to build
    if request.param == "mongo":
        storage = storage_socket_factory("127.0.0.1", 27017, storage_name, storage_type=request.param)

        # Clean and re-init the database
        storage.client.drop_database(storage._project_name)
        storage.init_database()
    else:
        raise KeyError("Storage type {} not understood".format(request.param))

    yield storage

    if request.param == "mongo":
        storage.client.drop_database(storage_name)
    else:
        raise KeyError("Storage type {} not understood".format(request.param))
