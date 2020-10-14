"""
Contains testing infrastructure for QCFractal.
"""

import os
import pkgutil
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Mapping
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
import qcengine as qcng
import requests
from qcelemental.models import Molecule
from tornado.ioloop import IOLoop

from .interface import FractalClient
from .postgres_harness import PostgresHarness, TemporaryPostgres
from .queue import build_queue_adapter
from .server import FractalServer
from .snowflake import FractalSnowflake
from .storage_sockets import storage_socket_factory

### Addon testing capabilities


def pytest_addoption(parser):
    """
    Additional PyTest CLI flags to add

    See `pytest_collection_modifyitems` for handling and `pytest_configure` for adding known in-line marks.
    """
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--runexamples", action="store_true", default=False, help="run example tests")


def pytest_collection_modifyitems(config, items):
    """
    Handle test triggers based on the CLI flags

    Use decorators:
    @pytest.mark.slow
    @pyrest.mark.example
    """
    runslow = config.getoption("--runslow")
    runexamples = config.getoption("--runexamples")
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    skip_example = pytest.mark.skip(reason="need --runexamples option to run")
    for item in items:
        if "slow" in item.keywords and not runslow:
            item.add_marker(skip_slow)
        if "example" in item.keywords and not runexamples:
            item.add_marker(skip_example)


def pytest_configure(config):
    import sys

    sys._called_from_test = True
    config.addinivalue_line("markers", "example: Mark a given test as an example which can be run")
    config.addinivalue_line(
        "markers", "slow: Mark a given test as slower than most other tests, needing a special " "flag to run."
    )


def pytest_unconfigure(config):
    import sys

    del sys._called_from_test


def _plugin_import(plug):
    plug_spec = pkgutil.find_loader(plug)
    if plug_spec is None:
        return False
    else:
        return True


_adapter_testing = ["pool", "dask", "fireworks", "parsl"]

# Figure out what is imported
_programs = {
    "fireworks": _plugin_import("fireworks"),
    "rdkit": _plugin_import("rdkit"),
    "psi4": _plugin_import("psi4"),
    "parsl": _plugin_import("parsl"),
    "dask": _plugin_import("dask"),
    "dask_jobqueue": _plugin_import("dask_jobqueue"),
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


def check_has_module(program):
    import_message = "Not detecting module {}. Install package if necessary to enable tests."
    if has_module(program) is False:
        pytest.skip(import_message.format(program))


def _build_pytest_skip(program):
    import_message = "Not detecting module {}. Install package if necessary to enable tests."
    return pytest.mark.skipif(has_module(program) is False, reason=import_message.format(program))


# Add a number of module testing options
using_dask = _build_pytest_skip("dask.distributed")
using_dask_jobqueue = _build_pytest_skip("dask_jobqueue")
using_dftd3 = _build_pytest_skip("dftd3")
using_fireworks = _build_pytest_skip("fireworks")
using_geometric = _build_pytest_skip("geometric")
using_parsl = _build_pytest_skip("parsl")
using_psi4 = _build_pytest_skip("psi4")
using_rdkit = _build_pytest_skip("rdkit")
using_torsiondrive = _build_pytest_skip("torsiondrive")
using_unix = pytest.mark.skipif(
    os.name.lower() != "posix", reason="Not on Unix operating system, " "assuming Bash is not present"
)

### Generic helpers


def recursive_dict_merge(base_dict, dict_to_merge_in):
    """Recursive merge for more complex than a simple top-level merge {**x, **y} which does not handle nested dict."""
    for k, v in dict_to_merge_in.items():
        if k in base_dict and isinstance(base_dict[k], dict) and isinstance(dict_to_merge_in[k], Mapping):
            recursive_dict_merge(base_dict[k], dict_to_merge_in[k])
        else:
            base_dict[k] = dict_to_merge_in[k]


def find_open_port():
    """
    Use socket's built in ability to find an open port.
    """
    sock = socket.socket()
    sock.bind(("", 0))

    host, port = sock.getsockname()

    return port


@contextmanager
def preserve_cwd():
    """Always returns to CWD on exit"""
    cwd = os.getcwd()
    try:
        yield cwd
    finally:
        os.chdir(cwd)


def await_true(wait_time, func, *args, **kwargs):

    wait_period = kwargs.pop("period", 4)
    periods = max(int(wait_time / wait_period), 1)
    for period in range(periods):
        ret = func(*args, **kwargs)
        if ret:
            return True
        time.sleep(wait_period)

    return False


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
        if sys.platform.startswith("win"):
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
    if sys.platform.startswith("win"):
        bin_prefix = os.path.join(sys.prefix, "Scripts")
    else:
        bin_prefix = os.path.join(sys.prefix, "bin")

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
            coverage_flags = [coverage_dir, "run", "--parallel-mode", "--source=" + src_dir]

            # If python script, skip the python bin
            if args[0].endswith("python"):
                args.pop(0)
            args = coverage_flags + args

    # Do we optionally dumpstdout?
    dump_stdout = kwargs.pop("dump_stdout", False)

    if sys.platform.startswith("win"):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.PIPE
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
                print("\n" + "-" * 30)
                print("\n|| Process command: {}".format(" ".join(args)))
                print("\n|| Process stderr: \n{}".format(error.decode()))
                print("-" * 30)
                print("\n|| Process stdout: \n{}".format(output.decode()))
                print("-" * 30)


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


@pytest.fixture(scope="session")
def postgres_server():

    if shutil.which("psql") is None:
        pytest.skip("Postgres is not installed on this server and no active postgres could be found.")

    storage = None
    psql = PostgresHarness({"database": {"port": 5432}})
    # psql = PostgresHarness({"database": {"port": 5432, "username": "qcarchive", "password": "mypass"}})
    if not psql.is_alive():
        print()
        print(
            f"Could not connect to a Postgres server at {psql.config.database_uri()}, this will increase time per test session by ~3 seconds."
        )
        print()
        storage = TemporaryPostgres()
        psql = storage.psql
        print("Using Database: ", psql.config.database_uri())

    yield psql

    if storage:
        storage.stop()


def reset_server_database(server):
    """Resets the server database for testing."""
    if "QCFRACTAL_RESET_TESTING_DB" in os.environ:
        server.storage._clear_db(server.storage._project_name)

    server.storage._delete_DB_data(server.storage._project_name)

    # Force a heartbeat after database clean if a manager is present.
    if server.queue_socket:
        server.await_results()


@pytest.fixture(scope="module")
def test_server(request, postgres_server):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Storage name
    storage_name = "test_qcfractal_server"
    postgres_server.create_database(storage_name)

    with FractalSnowflake(
        max_workers=0,
        storage_project_name="test_qcfractal_server",
        storage_uri=postgres_server.database_uri(),
        start_server=False,
        reset_database=True,
    ) as server:

        # Clean and re-init the database
        yield server


def build_adapter_clients(mtype, storage_name="test_qcfractal_compute_server"):

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


@pytest.fixture(scope="module", params=_adapter_testing)
def adapter_client_fixture(request):
    adapter_client = build_adapter_clients(request.param)
    yield adapter_client

    # Do a final close with existing tech
    build_queue_adapter(adapter_client).close()


@pytest.fixture(scope="module", params=_adapter_testing)
def managed_compute_server(request, postgres_server):
    """
    A FractalServer with compute associated parametrize for all managers.
    """

    storage_name = "test_qcfractal_compute_server"
    postgres_server.create_database(storage_name)

    adapter_client = build_adapter_clients(request.param, storage_name=storage_name)

    # Build a server with the thread in a outer context loop
    # Not all adapters play well with internal loops
    with loop_in_thread() as loop:
        server = FractalServer(
            port=find_open_port(),
            storage_project_name=storage_name,
            storage_uri=postgres_server.database_uri(),
            loop=loop,
            queue_socket=adapter_client,
            ssl_options=False,
            skip_storage_version_check=True,
        )

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


@pytest.fixture(scope="module")
def fractal_compute_server(postgres_server):
    """
    A FractalServer with a local Pool manager.
    """

    # Storage name
    storage_name = "test_qcfractal_compute_snowflake"
    postgres_server.create_database(storage_name)

    with FractalSnowflake(
        max_workers=2,
        storage_project_name=storage_name,
        storage_uri=postgres_server.database_uri(),
        reset_database=True,
        start_server=False,
    ) as server:
        reset_server_database(server)
        yield server


def build_socket_fixture(stype, server=None):
    print("")

    # Check mongo
    storage_name = "test_qcfractal_storage" + stype

    # IP/port/drop table is specific to build
    if stype == "sqlalchemy":

        server.create_database(storage_name)
        storage = storage_socket_factory(server.database_uri(), storage_name, db_type=stype, sql_echo=False)

        # Clean and re-init the database
        storage._clear_db(storage_name)
    else:
        raise KeyError("Storage type {} not understood".format(stype))

    yield storage

    if stype == "sqlalchemy":
        # todo: drop db
        # storage._clear_db(storage_name)
        pass
    else:
        raise KeyError("Storage type {} not understood".format(stype))


@pytest.fixture(scope="module", params=["sqlalchemy"])
def socket_fixture(request):

    yield from build_socket_fixture(request.param)


@pytest.fixture(scope="module")
def sqlalchemy_socket_fixture(request, postgres_server):

    yield from build_socket_fixture("sqlalchemy", postgres_server)


def live_fractal_or_skip():
    """
    Ensure Fractal live connection can be made
    First looks for a local staging server, then tries QCArchive.
    """
    try:
        return FractalClient("localhost:7777", verify=False)
    except (requests.exceptions.ConnectionError, ConnectionRefusedError):
        print("Failed to connect to localhost, trying MolSSI QCArchive.")
        try:
            requests.get("https://api.qcarchive.molssi.org:443", json={}, timeout=5)
            return FractalClient()
        except (requests.exceptions.ConnectionError, ConnectionRefusedError):
            return pytest.skip("Could not make a connection to central Fractal server")


def df_compare(df1, df2, sort=False):
    """ checks equality even when columns contain numpy arrays, which .equals and == struggle with """
    if sort:
        if isinstance(df1, pd.DataFrame):
            df1 = df1.reindex(sorted(df1.columns), axis=1)
        elif isinstance(df1, pd.Series):
            df1 = df1.sort_index()
        if isinstance(df2, pd.DataFrame):
            df2 = df2.reindex(sorted(df2.columns), axis=1)
        elif isinstance(df2, pd.Series):
            df2 = df2.sort_index()

    def element_equal(e1, e2):
        if isinstance(e1, np.ndarray):
            if not np.array_equal(e1, e2):
                return False
        elif isinstance(e1, Molecule):
            if not e1.get_hash() == e2.get_hash():
                return False
        # Because nan != nan
        elif isinstance(e1, float) and np.isnan(e1):
            if not np.isnan(e2):
                return False
        else:
            if not e1 == e2:
                return False
        return True

    if isinstance(df1, pd.Series):
        if not isinstance(df2, pd.Series):
            return False
        if len(df1) != len(df2):
            return False
        for i in range(len(df1)):
            if not element_equal(df1[i], df2[i]):
                return False
        return True

    for column in df1.columns:
        if column.startswith("_"):
            df1.drop(column, axis=1, inplace=True)
    for column in df2.columns:
        if column.startswith("_"):
            df2.drop(column, axis=1, inplace=True)
    if not all(df1.columns == df2.columns):
        return False
    if not all(df1.index.values == df2.index.values):
        return False
    for i in range(df1.shape[0]):
        for j in range(df1.shape[1]):
            if not element_equal(df1.iloc[i, j], df2.iloc[i, j]):
                return False

    return True
