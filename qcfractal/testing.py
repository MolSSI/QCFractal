"""
Contains testing infrastructure for QCFractal.
"""

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
import qcengine as qcng
from tornado.ioloop import IOLoop

from .queue import build_queue_adapter
from .server import FractalServer
from .snowflake import FractalSnowflake
from .storage_sockets import storage_socket_factory

### Globals
POSTGRES_TESTING_URI = "postgresql+psycopg2://qcarchive:mypass@localhost:5432/test_qcarchivedb"

### Addon testing capabilities


def _plugin_import(plug):
    plug_spec = pkgutil.find_loader(plug)
    if plug_spec is None:
        return False
    else:
        return True


_import_message = "Not detecting module {}. Install package if necessary and add to envvar PYTHONPATH"

_adapter_testing = ["pool", "dask", "fireworks", "parsl"]

# Figure out what is imported
_programs = {
    "fireworks": _plugin_import("fireworks"),
    "rdkit": _plugin_import("rdkit"),
    "psi4": _plugin_import("psi4"),
    "parsl": _plugin_import("parsl"),
    "dask": _plugin_import("dask"),
    "geometric": _plugin_import("geometric"),
    "torsiondrive": _plugin_import("torsiondrive"),
    "torchani": _plugin_import("torchani"),
}
if _programs["dask"]:
    _programs["dask.distributed"] = _plugin_import("dask.distributed")
else:
    _programs["dask.distributed"] = False

_programs["dftd3"] = "dftd3" in qcng.list_available_programs()


def has_module(name):
    return _programs[name]


def _build_pytest_skip(program):
    import_message = "Not detecting module {}. Install package if necessary to enable tests."
    return pytest.mark.skipif(has_module(program) is False, reason=import_message.format(program))


# Add a number of module testing options
using_dask = _build_pytest_skip('dask.distributed')
using_dftd3 = _build_pytest_skip('dftd3')
using_fireworks = _build_pytest_skip('fireworks')
using_geometric = _build_pytest_skip('geometric')
using_parsl = _build_pytest_skip('parsl')
using_psi4 = _build_pytest_skip('psi4')
using_rdkit = _build_pytest_skip('rdkit')
using_torsiondrive = _build_pytest_skip('torsiondrive')
using_unix = pytest.mark.skipif(os.name.lower() != 'posix',
                                reason='Not on Unix operating system, '
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
    """Recursive merge for more complex than a simple top-level merge {**x, **y} which does not handle nested dict."""
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


def check_active_postgres_server():
    """Checks for a active mongo server, skips the test if not found.
    """

    try:
        from sqlalchemy import create_engine
        engine = create_engine(POSTGRES_TESTING_URI)
        engine.table_names()
    except:
        pytest.skip(
            f"Could not find an activate mongo test instance at '{POSTGRES_TESTING_URI}'."
        )


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
    Opens a background task.

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

    Returns True if exit code zero.
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
    # server.storage._clear_db(server.storage._project_name)
    server.storage._delete_DB_data(server.storage._project_name)


@pytest.fixture(scope="module")
def test_server(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    check_active_mongo_server()

    storage_name = "qcf_local_server_test"

    storage_uri = POSTGRES_TESTING_URI
    # storage_uri="mongodb://localhost:27017"

    # with loop_in_thread() as loop:
    with FractalSnowflake(max_workers=0,
                          storage_project_name=storage_name,
                          storage_uri=storage_uri,
                          start_server=False,
                          reset_database=True) as server:

        # Clean and re-init the database
        yield server


def build_adapter_clients(mtype, storage_name="qcf_compute_server_test"):
    # Check mongo
    check_active_mongo_server()

    # Basic boot and loop information
    if mtype == "pool":
        from concurrent.futures import ProcessPoolExecutor

        adapter_client = ProcessPoolExecutor(max_workers=2)

    elif mtype == "dask":
        dd = pytest.importorskip("dask.distributed")
        adapter_client = dd.Client(n_workers=2, threads_per_worker=1, resources={"process": 1})

        # Not super happy about this line, but shuts up dangling reference errors
        adapter_client._should_close_loop = False

    elif mtype == "fireworks":
        fireworks = pytest.importorskip("fireworks")

        fireworks_name = storage_name + "_fireworks_queue"
        adapter_client = fireworks.LaunchPad(name=fireworks_name, logdir="/tmp/", strm_lvl="CRITICAL")

    elif mtype == "parsl":
        parsl = pytest.importorskip("parsl")

        # Must only be a single thread as we run thread unsafe applications.
        adapter_client = parsl.config.Config(executors=[parsl.executors.threads.ThreadPoolExecutor(max_threads=1)])

    else:
        raise TypeError("fractal_compute_server: internal parametrize error")

    return adapter_client


def build_managed_compute_server(mtype):

    storage_name = "qcf_compute_server_test"
    adapter_client = build_adapter_clients(mtype, storage_name=storage_name)

    # Build a server with the thread in a outer context loop
    # Not all adapters play well with internal loops
    with loop_in_thread() as loop:
        server = FractalServer(port=find_open_port(),
                               storage_project_name=storage_name,
                               loop=loop,
                               queue_socket=adapter_client,
                               ssl_options=False)

        # Clean and re-init the database
        reset_server_database(server)

        # Build Client and Manager
        from qcfractal.interface import FractalClient
        client = FractalClient(server)

        from qcfractal.queue import QueueManager
        manager = QueueManager(client, adapter_client)

        yield client, server, manager

        # Close down and clean the adapter
        manager.close_adapter()
        manager.stop()


@pytest.fixture(scope="module", params=_adapter_testing)
def adapter_client_fixture(request):
    adapter_client = build_adapter_clients(request.param)
    yield adapter_client

    # Do a final close with existing tech
    build_queue_adapter(adapter_client).close()


@pytest.fixture(scope="module", params=_adapter_testing)
def managed_compute_server(request):
    """
    A FractalServer with compute associated parametrize for all managers.
    """

    yield from build_managed_compute_server(request.param)


@pytest.fixture(scope="module")
def fractal_compute_server(request):
    """
    A FractalServer with a local Pool manager.
    """

    # Check mongo
    check_active_mongo_server()

    # Storage name
    storage_name = "qcf_compute_server_test"
    # storage_uri = "mongodb://localhost:27017"
    storage_uri = POSTGRES_TESTING_URI
    # storage_uri = "sqlite:///:memory:"
    with FractalSnowflake(max_workers=2,
                          storage_project_name=storage_name,
                          storage_uri=storage_uri,
                          reset_database=True,
                          start_server=False) as server:
        # reset_server_database(server)
        yield server


def build_socket_fixture(stype):
    print("")

    # Check mongo
    storage_name = "qcf_local_values_test_" + stype

    # IP/port/drop table is specific to build
    if stype in ["pymongo", "mongoengine"]:
        check_active_mongo_server()
        storage = storage_socket_factory("mongodb://localhost", storage_name, db_type=stype)

        # Clean and re-init the database
        storage._clear_db(storage_name)

    elif stype == 'sqlalchemy':
        check_active_postgres_server()
        storage = storage_socket_factory(POSTGRES_TESTING_URI,
                                         storage_name,
                                         db_type=stype,
                                         sql_echo=False)
        # storage = storage_socket_factory('sqlite:///:memory:', storage_name, db_type=stype)
        # storage = storage_socket_factory('sqlite:///path_to_db', storage_name, db_type=stype)

        # Clean and re-init the database
        storage._clear_db(storage_name)
    else:
        raise KeyError("Storage type {} not understood".format(stype))

    yield storage

    if stype in ["pymongo", "mongoengine"]:
        storage.client.drop_database(storage_name)
    elif stype == "sqlalchemy":
        # todo: drop db
        # storage._clear_db(storage_name)
        pass
    else:
        raise KeyError("Storage type {} not understood".format(stype))


@pytest.fixture(scope="module", params=["mongoengine", "sqlalchemy"])
def socket_fixture(request):

    yield from build_socket_fixture(request.param)


@pytest.fixture(scope="module")
def mongoengine_socket_fixture(request):

    yield from build_socket_fixture("mongoengine")


@pytest.fixture(scope="module")
def sqlalchemy_socket_fixture(request):

    yield from build_socket_fixture("sqlalchemy")
