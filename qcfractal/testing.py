"""
Contains testing infrastructure for QCFractal.
"""

import os
import pkgutil
import logging
import subprocess
import sys
import time
import gc
import copy
from collections import Mapping
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
import qcengine as qcng
import requests
from qcelemental.models import Molecule
from .port_util import find_port
from .config import FractalConfig

import qcfractal.interface as ptl
from qcfractal.interface.models import TorsionDriveInput
from .interface import FractalClient
from .interface.models import TorsionDriveInput
from .postgres_harness import TemporaryPostgres
from .qc_queue import build_queue_adapter
from .snowflake import FractalSnowflake
from .periodics import FractalPeriodics
from .storage_sockets.sqlalchemy_socket import SQLAlchemySocket

## For mock flask responses
#from requests_mock_flask import add_flask_app_to_mock
#import requests_mock
#import responses

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
    config.addinivalue_line("markers", "example: Mark a given test as an example which can be run")
    config.addinivalue_line(
        "markers", "slow: Mark a given test as slower than most other tests, needing a special " "flag to run."
    )


def pytest_unconfigure(config):
    pass


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


@contextmanager
def preserve_cwd():
    """Always returns to CWD on exit"""
    cwd = os.getcwd()
    try:
        yield cwd
    finally:
        os.chdir(cwd)



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


#######################################
# Database and storage socket fixtures
#######################################
@pytest.fixture(scope="session")
def postgres_server(tmp_path_factory):
    '''
    A postgres server instance, not including any databases

    This is built only once per session, and automatically deleted after the session. It uses
    a pytest-proveded session-scoped temporary directory
    '''

    logger = logging.getLogger(__name__)

    db_path = str(tmp_path_factory.mktemp('db'))
    tmp_pg = TemporaryPostgres(data_dir=db_path)
    pg_harness = tmp_pg.harness
    logger.debug(f"Using database located at {db_path} with uri {pg_harness.database_uri()}")
    assert pg_harness.is_alive(False) and not pg_harness.is_alive(True)
    yield pg_harness

    if tmp_pg:
        tmp_pg.stop()


@pytest.fixture(scope="function")
def temporary_database(postgres_server):
    """
    A temporary database that only lasts for one function

    It is part of the postgres instance given by the postgres_server fixture
    """

    # Make sure that the server is up, but that the database doesn't exist
    assert postgres_server.is_alive(False) and not postgres_server.is_alive(True)

    # Now create it (including creating tables)
    postgres_server.create_database()

    try:
        yield postgres_server
    finally:
        # Force a garbage collection. Sometimes there's session objects or something
        # hanging around, which will prevent the database from being deleted
        gc.collect()

        postgres_server.delete_database()


@pytest.fixture(scope="function")
def storage_socket(temporary_database):
    '''
    A fixture for temporary database and storage socket
    '''

    # Create a configuration. Since this is mostly just for a storage socket,
    # We can use defaults for almost all, since a flask server, etc, won't be instantiated
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    cfg_dict = {}
    cfg_dict["base_directory"] = temporary_database.config.base_directory
    cfg_dict["loglevel"] = "DEBUG"
    cfg_dict["database"] = temporary_database.config.dict()
    cfg_dict["database"]["pool_size"] = 0
    qcf_config = FractalConfig(**cfg_dict)

    socket = SQLAlchemySocket()
    socket.init(qcf_config)
    yield socket


@pytest.fixture(scope="function")
def test_server(temporary_database):
    """
    A QCFractal server with no compute attached, and with security disabled
    """

    # Tighten the service frequency for tests
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    extra_config = {}
    extra_config['service_frequency'] = 5
    extra_config['heartbeat_frequency'] = 3
    extra_config['heartbeat_max_missed'] = 2
    extra_config['database'] = {'pool_size': 0}

    with FractalSnowflake(
        start=True,
        max_workers=0,
        enable_watching=True,
        database_config=temporary_database.config,
        flask_config="testing",
        extra_config=extra_config
    ) as server:
        yield server


@pytest.fixture(scope="function")
def fractal_compute_server(temporary_database):
    """
    A Fractal snowflake with local compute manager

    All subprocesses are started automatically
    """

    # Tighten the service frequency for tests
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    extra_config = {}
    extra_config['service_frequency'] = 5
    extra_config['heartbeat_frequency'] = 3
    extra_config['heartbeat_max_missed'] = 2
    extra_config['database'] = {'pool_size': 0}

    with FractalSnowflake(
            start=True,
            max_workers=2,
            enable_watching=True,
            database_config=temporary_database.config,
            flask_config="testing",
            extra_config=extra_config
    ) as server:
        yield server


@pytest.fixture(scope="function")
def fractal_compute_server_manualperiodics(temporary_database):
    """
    A Fractal snowflake with local compute manager, but with periodics disabled and manually controllable
    """

    # Tighten the service frequency for tests
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    extra_config = {}
    extra_config['service_frequency'] = 5
    extra_config['heartbeat_frequency'] = 3
    extra_config['heartbeat_max_missed'] = 2
    extra_config['database'] = {'pool_size': 0}

    with FractalSnowflake(
            start=False,
            max_workers=2,
            enable_watching=True,
            database_config=temporary_database.config,
            flask_config="testing",
            extra_config=extra_config
    ) as server:
        qcf_cfg = server._qcf_config
        server._flask_proc.start()
        server._compute_proc.start()

        periodics = FractalPeriodics(qcf_cfg)
        yield server, periodics


####################################
# Torsiondrive fixtures & functions
####################################
def run_services(server: FractalSnowflake, periodics: FractalPeriodics, max_iter: int = 10) -> bool:
    """
    Run up to max_iter iterations on a service
    """

    logger = logging.getLogger(__name__)
    # Wait for everything currently running to finish
    server.await_results()

    for i in range(1, max_iter+1):
        logger.debug(f"Iteration {i}")
        running_services = periodics._update_services()
        logger.debug(f"Number of running services: {running_services}")
        if running_services == 0:
            return True

        server.await_results()

    return False


@pytest.fixture(scope="function")
def torsiondrive_fixture(fractal_compute_server_manualperiodics):

    # Cannot use this fixture without these services. Also cannot use `mark` and `fixture` decorators
    pytest.importorskip("torsiondrive")
    pytest.importorskip("geometric")
    pytest.importorskip("rdkit")

    server, periodics = fractal_compute_server_manualperiodics
    client = server.client()

    # Add a HOOH
    hooh = ptl.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    # Geometric options
    torsiondrive_options = {
        "initial_molecule": mol_ret[0],
        "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [90]},
        "optimization_spec": {
            "program": "geometric",
            "keywords": {"coordsys": "tric"},
            "protocols": {"trajectory": "initial_and_final"},
        },
        "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
    }

    def spin_up_test(**keyword_augments):
        run_service = keyword_augments.pop("run_service", True)

        instance_options = copy.deepcopy(torsiondrive_options)
        recursive_dict_merge(instance_options, keyword_augments)

        inp = TorsionDriveInput(**instance_options)
        ret = client.add_service([inp], full_return=True)

        if ret.meta.n_inserted:  # In case test already submitted
            compute_key = ret.data.ids[0]
            service = client.query_services(procedure_id=compute_key)[0]
            assert "WAITING" in service["status"]

        if run_service:
            finished = run_services(server, periodics)
            assert finished

        return ret.data

    yield spin_up_test, server, periodics



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
            port=find_port(),
            storage_project_name=storage_name,
            storage_uri=postgres_server.database_uri(),
            loop=loop,
            queue_socket=adapter_client,
            ssl_options=False,
            skip_storage_version_check=True,
            flask_config="testing"
        )

        # Clean and re-init the database
        reset_server_database(server)

        # Build Client and Manager
        from qcfractal.interface import FractalClient

        client = FractalClient(server)

        from qcfractal.qc_queue import QueueManager

        manager = QueueManager(client, adapter_client)

        yield client, server, manager

        # Close down and clean the adapter
        manager.close_adapter()
        manager.stop()


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
