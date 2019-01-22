"""
Contains testing infrastructure for QCFractal
"""

import logging
import os
import pkgutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Mapping
from contextlib import contextmanager

import pymongo
import pytest
from tornado.ioloop import IOLoop

from .server import FractalServer
from .storage_sockets import storage_socket_factory

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
    "parsl": _plugin_import("parsl"),
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
using_parsl = pytest.mark.skipif(has_module('parsl') is False, reason=_import_message.format('parsl'))
using_rdkit = pytest.mark.skipif(has_module('rdkit') is False, reason=_import_message.format('rdkit'))
using_geometric = pytest.mark.skipif(has_module('geometric') is False, reason=_import_message.format('geometric'))
using_torsiondrive = pytest.mark.skipif(
    has_module('torsiondrive') is False, reason=_import_message.format('torsiondrive'))
using_unix = pytest.mark.skipif(
    os.name.lower() != 'posix', reason='Not on Unix operating system, '
    'assuming Bash is not present')

### Generic helpers


def mark_slow(func):
    try:
        if not pytest.config.getoption("--runslow"):
            func = pytest.mark.skip("need --runslow option to run")(func)
    except (AttributeError, ValueError):
        # AttributeError: module 'pytest' has no attribute 'config'
        pass

    return func


def mark_example(func):
    try:
        if not pytest.config.getoption("--runexamples"):
            func = pytest.mark.skip("need --runexample option to run")(func)
    except AttributeError:
        # AttributeError: module 'pytest' has no attribute 'config'
        pass

    return func


def recursive_dict_merge(base_dict, dict_to_merge_in):
    """Recursive merge for more complex than a simple top-level merge {**x, **y} which does not handle nested dict"""
    for k, v in dict_to_merge_in.items():
        if (k in base_dict and isinstance(base_dict[k], dict) and isinstance(dict_to_merge_in[k], Mapping)):
            recursive_dict_merge(base_dict[k], dict_to_merge_in[k])
        else:
            base_dict[k] = dict_to_merge_in[k]


def check_active_mongo_server():
    """Checks for a active mongo server, skips the test if not found.
    """

    client = pymongo.MongoClient("mongodb://localhost", serverSelectionTimeoutMS=100)
    try:
        client.server_info()
    except:
        pytest.skip("Could not find an activate mongo test instance at 'localhost:27017'.")


def find_open_port():
    """
    Use socket's built in ability to find an open port.
    """
    sock = socket.socket()
    sock.bind(('', 0))

    host, port = sock.getsockname()

    return port


@contextmanager
def preserve_cwd():
    """Always returns to CWD on exit
    """
    cwd = os.getcwd()
    try:
        yield cwd
    finally:
        os.chdir(cwd)


### Background thread loops


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
        except (ValueError, KeyError, RuntimeError):
            pass
        IOLoop.clear_instance()
        IOLoop.clear_current()


@contextmanager
def loop_in_thread():
    with pristine_loop() as loop:
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


@contextmanager
def fireworks_quiet_lpad(name=None):
    """
    Returns a (relatively) quiet launchpad instance
    """

    if name is None:
        name = "fw_testing_quiet"

    # Build Fireworks test server and manager
    fireworks = pytest.importorskip("fireworks")
    logging.basicConfig(level=logging.CRITICAL, filename="/tmp/fireworks_logfile.txt")

    name = "fw_testing_quiet"
    lpad = fireworks.LaunchPad(name=name, logdir="/tmp/", strm_lvl="CRITICAL")
    lpad.reset(None, require_password=False)

    try:
        yield lpad

    finally:
        # Cleanup and reset
        lpad.reset(None, require_password=False)
        logging.basicConfig(level=None, filename=None)


def terminate_process(proc):
    if proc.poll() is None:

        # Sigint (keyboard interupt)
        if sys.platform.startswith('win'):
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        try:
            start = time.time()
            while (proc.poll() is None) and (time.time() < (start + 15)):
                time.sleep(0.02)
        # Flat kill
        finally:
            proc.kill()


@contextmanager
def popen(args, **kwargs):
    """
    Opens a background task

    Code and idea from dask.distributed's testing suite
    https://github.com/dask/distributed
    """
    args = list(args)

    # Bin prefix
    if sys.platform.startswith('win'):
        bin_prefix = os.path.join(sys.prefix, 'Scripts')
    else:
        bin_prefix = os.path.join(sys.prefix, 'bin')

    # Do we prefix with Python?
    if kwargs.pop("append_prefix", True):
        args[0] = os.path.join(bin_prefix, args[0])

    # Add coverage testing
    if kwargs.pop("coverage", False):
        coverage_dir = os.path.join(bin_prefix, "coverage")
        if not os.path.exists(coverage_dir):
            print("Could not find Python coverage, skipping cov.")

        else:
            src_dir = os.path.dirname(os.path.abspath(__file__))
            coverage_flags = [coverage_dir, "run", "--append", "--source=" + src_dir]

            # If python script, skip the python bin
            if args[0].endswith("python"):
                args.pop(0)
            args = coverage_flags + args

    # Do we optionally dumpstdout?
    dump_stdout = kwargs.pop("dump_stdout", False)

    if sys.platform.startswith('win'):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP

    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
    proc = subprocess.Popen(args, **kwargs)
    try:
        yield proc
    except Exception:
        dump_stdout = True
        raise

    finally:
        try:
            terminate_process(proc)
        finally:
            output, error = proc.communicate()
            if dump_stdout:
                print('\n' + '-' * 30)
                print("\n|| Process command: {}".format(" ".join(args)))
                print('\n|| Process stderr: \n{}'.format(error.decode()))
                print('-' * 30)
                print('\n|| Process stdout: \n{}'.format(output.decode()))
                print('-' * 30)


def run_process(args, **kwargs):
    """
    Runs a process in the background until complete.

    Returns True if exit code zero
    """

    timeout = kwargs.pop("timeout", 30)
    terminate_after = kwargs.pop("interupt_after", None)
    with popen(args, **kwargs) as proc:
        if terminate_after is None:
            proc.wait(timeout=timeout)
        else:
            time.sleep(terminate_after)
            terminate_process(proc)

        retcode = proc.poll()

    return retcode == 0


### Server testing mechanics


def reset_server_database(server):
    """Resets the server database for testing.
    """
    server.storage._clear_db(server.storage._project_name)
    # server.storage.init_database()


@pytest.fixture(scope="module")
def test_server(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    storage_name = "qcf_local_server_test"

    with loop_in_thread() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = FractalServer(port=find_open_port(), storage_project_name=storage_name, loop=loop, ssl_options=False)

        # Clean and re-init the database
        reset_server_database(server)

        yield server


def _dask_server_fixture(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    dd = pytest.importorskip("dask.distributed")

    storage_name = "qcf_dask_server_test"

    with pristine_loop() as loop:

        # Client auto builds and shuts down a LocalCluster
        # LocalCluster will start the loop in a background thread for us
        with dd.Client(n_workers=1, threads_per_worker=1, loop=loop) as client:

            # Build server, manually handle IOLoop (no start/stop needed)
            server = FractalServer(
                port=find_open_port(),
                storage_project_name=storage_name,
                loop=client.loop,
                queue_socket=client,
                ssl_options=False)

            # Clean and re-init the databse
            reset_server_database(server)

            # Yield the server instance
            yield server

            # LocalCluster cleans itself up in loop


@pytest.fixture(scope="module")
def dask_server_fixture(request):
    yield from _dask_server_fixture(request)


def _fireworks_server_fixture(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    fireworks = pytest.importorskip("fireworks")
    logging.basicConfig(level=logging.CRITICAL, filename="/tmp/fireworks_logfile.txt")

    storage_name = "qcf_fireworks_server_test"
    fireworks_name = storage_name + "_fireworks_queue"
    lpad = fireworks.LaunchPad(name=fireworks_name, logdir="/tmp/", strm_lvl="CRITICAL")

    with loop_in_thread() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = FractalServer(
            port=find_open_port(), storage_project_name=storage_name, loop=loop, queue_socket=lpad, ssl_options=False)

        # Clean and re-init the databse
        reset_server_database(server)

        yield server

        # Close down and clean the lpad
        server.objects["queue_manager"].close_adapter()

    logging.basicConfig(level=None, filename=None)


@pytest.fixture(scope="module")
def fireworks_server_fixture(request):
    yield from _fireworks_server_fixture(request)


def _parsl_server_fixture(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    parsl = pytest.importorskip("parsl")
    from parsl.configs.local_ipp import config

    storage_name = "qcf_parsl_server_test"

    with loop_in_thread() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = FractalServer(
            port=find_open_port(),
            storage_project_name=storage_name,
            loop=loop,
            queue_socket=config,
            ssl_options=False)

        # Parsl takes a bit to boot
        time.sleep(2)

        # Clean and re-init the databse
        reset_server_database(server)

        # Yield the server instance
        yield server

        # Close down and clean the parsl adapter
        server.objects["queue_manager"].close_adapter()


@pytest.fixture(scope="module")
def parsl_server_fixture(request):
    yield from _dask_server_fixture(request)


@pytest.fixture(scope="module", params=["dask", "fireworks", "parsl"])
def fractal_compute_server(request):
    if request.param == "dask":
        yield from _dask_server_fixture(request)
    elif request.param == "fireworks":
        yield from _fireworks_server_fixture(request)
    elif request.param == "parsl":
        yield from _parsl_server_fixture(request)
    else:
        raise TypeError("fractal_compute_server: internal parametrize error")


@pytest.fixture(scope="module", params=["mongoengine"])
def storage_socket_fixture(request):
    print("")

    # Check mongo
    check_active_mongo_server()
    storage_name = "qcf_test_me"

    # IP/port/drop table is specific to build
    if request.param in ["pymongo", "mongoengine"]:
        storage = storage_socket_factory("mongodb://localhost", storage_name, db_type=request.param)

        # Clean and re-init the database
        storage._clear_db(storage_name)
    else:
        raise KeyError("Storage type {} not understood".format(request.param))

    yield storage

    if request.param in ["pymongo", "mongoengine"]:
        storage.client.drop_database(storage_name)
    else:
        raise KeyError("Storage type {} not understood".format(request.param))


@pytest.fixture(scope="module")
def mongoengine_socket_fixture(request):

    # Check mongo
    check_active_mongo_server()
    storage_name = "qcf_local_values_test_me"

    storage = storage_socket_factory("mongodb://localhost", storage_name, db_type="mongoengine")

    # Clean and re-init the database
    storage._clear_db(storage_name)

    yield storage

    storage.client.drop_database(storage_name)
