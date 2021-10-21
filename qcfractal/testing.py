"""
Contains testing infrastructure for QCFractal.
"""

import os
import pkgutil
import json
import signal
import logging
import subprocess
import sys
import time
import gc
import copy
from contextlib import contextmanager

import numpy as np
import pandas as pd
import pytest
import qcengine as qcng
import requests
from qcelemental.models import Molecule
from qcelemental.models.results import WavefunctionProperties
from .config import FractalConfig, update_nested_dict

import qcfractal.interface as ptl
from .interface import FractalClient
from .interface.models import TorsionDriveInput, RecordStatusEnum
from .postgres_harness import TemporaryPostgres
from .qc_queue import build_queue_adapter, QueueManager
from .snowflake import FractalSnowflake, attempt_client_connect
from .periodics import FractalPeriodics
from .db_socket.socket import SQLAlchemySocket
from .portal.components.permissions import UserInfo
from .portal import PortalClient

# Valid client encodings
valid_encodings = ["application/json", "application/msgpack"]


_test_users = {
    "admin_user": {
        "pw": "something123",
        "info": {
            "role": "admin",
            "fullname": "Mrs. Admin User",
            "organization": "QCF Testing",
            "email": "admin@example.com",
        },
    },
    "read_user": {
        "pw": "something123",
        "info": {
            "role": "read",
            "fullname": "Mr. Read User",
            "organization": "QCF Testing",
            "email": "read@example.com",
        },
    },
    "monitor_user": {
        "pw": "something123",
        "info": {
            "role": "monitor",
            "fullname": "Mr. Monitor User",
            "organization": "QCF Testing",
            "email": "monitor@example.com",
        },
    },
    "compute_user": {
        "pw": "something123",
        "info": {
            "role": "compute",
            "fullname": "Mr. Compute User",
            "organization": "QCF Testing",
            "email": "compute@example.com",
        },
    },
    "submit_user": {
        "pw": "something123",
        "info": {
            "role": "submit",
            "fullname": "Mrs. Submit User",
            "organization": "QCF Testing",
            "email": "submit@example.com",
        },
    },
}


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


def load_procedure_data(name: str):
    """
    Loads pre-computed/dummy procedure data from the test directory

    Parameters
    ----------
    name
        The name of the file to load (without the json extension)

    Returns
    -------
    :
        A tuple of input data, molecule, and output data

    """

    my_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(my_path, "tests", "procedure_data")
    file_path = os.path.join(data_path, name + ".json")
    with open(file_path, "r") as f:
        data = json.load(f)

    procedure = data["input"]["procedure"]
    if procedure == "single":
        input_type = ptl.models.SingleProcedureSpecification
        result_type = ptl.models.AtomicResult
    elif procedure == "optimization":
        input_type = ptl.models.OptimizationProcedureSpecification
        result_type = ptl.models.OptimizationResult
    else:
        raise RuntimeError(f"Unknown procedure '{procedure}' in test!")

    if data["result"]["success"] is not True:
        result_type = ptl.models.FailedOperation

    molecule = Molecule(**data["molecule"])

    return input_type(**data["input"]), molecule, result_type(**data["result"])


def load_molecule_data(name: str) -> Molecule:
    """
    Loads a molecule object for use in testing
    """

    my_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(my_path, "tests", "molecule_data")
    file_path = os.path.join(data_path, name + ".json")
    return Molecule.from_file(file_path)


def load_wavefunction_data(name: str) -> WavefunctionProperties:
    """
    Loads a wavefunction object for use in testing
    """

    my_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(my_path, "tests", "wavefunction_data")
    file_path = os.path.join(data_path, name + ".json")

    with open(file_path, "r") as f:
        data = json.load(f)
    return WavefunctionProperties(**data)


@contextmanager
def caplog_handler_at_level(caplog_fixture, level, logger=None):
    """
    Helper function to set the caplog fixture's handler to a certain level as well, otherwise it wont be captured

    e.g. if caplog.set_level(logging.INFO) but caplog.handler is at logging.CRITICAL, anything below CRITICAL wont be
    captured.
    """
    starting_handler_level = caplog_fixture.handler.level
    caplog_fixture.handler.setLevel(level)
    with caplog_fixture.at_level(level, logger=logger):
        yield
    caplog_fixture.handler.setLevel(starting_handler_level)


@contextmanager
def preserve_cwd():
    """Always returns to CWD on exit"""
    cwd = os.getcwd()
    try:
        yield cwd
    finally:
        os.chdir(cwd)


def terminate_process(proc):
    if proc.poll() is None:

        # Interrupt (SIGINT)
        if sys.platform.startswith("win"):
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        try:
            start = time.time()
            while (proc.poll() is None) and (time.time() < (start + 15)):
                time.sleep(0.02)

        # Kill (SIGKILL)
        finally:
            proc.kill()


@contextmanager
def popen(args):
    """
    Opens a background task.
    """
    args = list(args)

    # Bin prefix
    if sys.platform.startswith("win"):
        bin_prefix = os.path.join(sys.prefix, "Scripts")
    else:
        bin_prefix = os.path.join(sys.prefix, "bin")

    # First argument is the executable name
    # We are testing executable scripts found in the bin directory
    args[0] = os.path.join(bin_prefix, args[0])

    # Add coverage testing
    coverage_dir = os.path.join(bin_prefix, "coverage")
    if not os.path.exists(coverage_dir):
        print("Could not find Python coverage, skipping cov.")
    else:
        src_dir = os.path.dirname(os.path.abspath(__file__))
        # --source is the path to the QCFractal source
        # --parallel-mode means every process gets its own file (useful because we do multiple processes)
        coverage_flags = [coverage_dir, "run", "--parallel-mode", "--source=" + src_dir]
        args = coverage_flags + args

    kwargs = {}
    if sys.platform.startswith("win"):
        # Allow using CTRL_C_EVENT / CTRL_BREAK_EVENT
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.PIPE
    proc = subprocess.Popen(args, **kwargs)
    try:
        yield proc
    except Exception:
        raise
    finally:
        try:
            terminate_process(proc)
        finally:
            output, error = proc.communicate()
            print("-" * 80)
            print("|| Process command: {}".format(" ".join(args)))
            print("|| Process stdout: \n{}".format(output.decode()))
            print("-" * 80)
            print()
            if error:
                print("\n|| Process stderr: \n{}".format(error.decode()))
                print("-" * 80)


def run_process(args, interrupt_after=15):
    """
    Runs a process in the background until complete.

    Returns True if exit code zero.
    """

    with popen(args) as proc:
        try:
            proc.wait(timeout=interrupt_after)
        except subprocess.TimeoutExpired:
            pass
        finally:
            terminate_process(proc)

        retcode = proc.poll()

    return retcode == 0


#######################################
# Database and storage socket fixtures
#######################################
@pytest.fixture(scope="session")
def postgres_server(tmp_path_factory):
    """
    A postgres server instance, not including any databases

    This is built only once per session, and automatically deleted after the session. It uses
    a pytest-proveded session-scoped temporary directory
    """

    logger = logging.getLogger(__name__)

    db_path = str(tmp_path_factory.mktemp("db"))
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
    """
    A fixture for temporary database and storage socket

    This should not be used with other fixtures, but used for unit testing
    the storage socket.
    """

    # Create a configuration. Since this is mostly just for a storage socket,
    # We can use defaults for almost all, since a flask server, etc, won't be instantiated
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    cfg_dict = {}
    cfg_dict["base_folder"] = temporary_database.config.base_folder
    cfg_dict["loglevel"] = "DEBUG"
    cfg_dict["database"] = temporary_database.config.dict()
    cfg_dict["database"]["pool_size"] = 0
    qcf_config = FractalConfig(**cfg_dict)

    socket = SQLAlchemySocket(qcf_config)
    yield socket


class TestingSnowflake(FractalSnowflake):
    """
    A snowflake class used for testing

    This mostly behaves like FractalSnowflake, but
    allows for some extra features such as manual handling of periodics
    and creating storage sockets from an instance.

    By default, the periodics and worker subprocesses are not started.
    """

    def __init__(
        self,
        database_config,
        encoding: str,
        start_flask=True,
        create_users=False,
        enable_security=False,
        allow_unauthenticated_read=False,
    ):

        self._encoding = encoding

        # Tighten the service frequency for tests
        # Also disable connection pooling in the storage socket
        # (which can leave db connections open, causing problems when we go to delete
        # the database)
        # Have a short token expiration (in case enable_security is True)
        extra_config = {}

        flask_config = {"jwt_access_token_expires": 1}  # expire tokens in 1 second

        extra_config["enable_security"] = enable_security
        extra_config["allow_unauthenticated_read"] = allow_unauthenticated_read
        extra_config["flask"] = flask_config
        extra_config["service_frequency"] = 5
        extra_config["loglevel"] = "DEBUG"
        extra_config["heartbeat_frequency"] = 3
        extra_config["heartbeat_max_missed"] = 2
        extra_config["database"] = {"pool_size": 0}

        FractalSnowflake.__init__(
            self,
            start=False,
            compute_workers=1,
            enable_watching=True,
            database_config=database_config,
            flask_config="testing",
            extra_config=extra_config,
        )

        if create_users:
            self.create_users()

        # Start the flask process if requested
        if start_flask:
            self.start_flask()

    def create_users(self):
        # Get a storage socket and add the roles/users/passwords
        storage = self.get_storage_socket()
        for k, v in _test_users.items():
            uinfo = UserInfo(username=k, enabled=True, **v["info"])
            storage.users.add(uinfo, password=v["pw"])

    def get_storage_socket(self) -> SQLAlchemySocket:
        """
        Obtain a new SQLAlchemy socket

        This function will create a new socket instance every time it is called
        """

        return SQLAlchemySocket(self._qcf_config)

    def get_periodics(self) -> FractalPeriodics:
        """
        Obtain a new FractalPeriodics object

        This function will create a new FractalPeriodics object every time it is called
        """

        return FractalPeriodics(self._qcf_config)

    def get_compute_manager(self, name: str) -> QueueManager:
        """
        Obtain a new QueueManager attached to this instance

        This function will create a new QueueManager object every time it is called
        """

        client = self.client()
        adapter_client = build_adapter_clients("pool")
        return QueueManager(client, adapter_client, manager_name=name)

    def start_flask(self) -> None:
        """
        Starts the flask subprocess
        """
        if not self._flask_proc.is_alive():
            self._flask_proc.start()

    def stop_flask(self) -> None:
        """
        Stops the flask subprocess
        """
        if self._flask_proc.is_alive():
            self._flask_proc.stop()

    def start_periodics(self) -> None:
        """
        Starts the periodics subprocess
        """
        if not self._periodics_proc.is_alive():
            self._periodics_proc.start()

    def stop_periodics(self) -> None:
        """
        Stops the periodics subprocess
        """
        if self._periodics_proc.is_alive():
            self._periodics_proc.stop()

    def start_compute_worker(self) -> None:
        """
        Starts the compute worker subprocess
        """
        if not self._compute_proc.is_alive():
            self._compute_proc.start()

    def stop_compute_worker(self) -> None:
        """
        Stops the compute worker subprocess
        """
        if self._compute_proc.is_alive():
            self._compute_proc.stop()

    def client(self, username=None, password=None) -> PortalClient:
        """
        Obtain a client connected to this snowflake

        Parameters
        ----------
        username
            The username to connect as
        password
            The password to use

        Returns
        -------
        :
            A PortalClient that is connected to this snowflake
        """
        client = attempt_client_connect(
            self.get_uri(),
            username=username,
            password=password,
        )
        client.encoding = self._encoding
        return client


@pytest.fixture(scope="function", params=valid_encodings)
def stopped_snowflake(temporary_database, request):
    """
    A QCFractal snowflake server used for testing, but with nothing started by default
    """

    db_config = temporary_database.config
    with TestingSnowflake(db_config, encoding=request.param, start_flask=False) as server:
        yield server


@pytest.fixture(scope="function")
def snowflake(stopped_snowflake):
    """
    A QCFractal snowflake server used for testing
    """

    stopped_snowflake.start_flask()
    yield stopped_snowflake


@pytest.fixture(scope="function", params=valid_encodings)
def secure_snowflake(temporary_database, request):
    """
    A QCFractal snowflake server with authorization/authentication enabled
    """

    db_config = temporary_database.config
    with TestingSnowflake(
        db_config,
        encoding=request.param,
        start_flask=True,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
    ) as server:
        yield server


@pytest.fixture(scope="function", params=valid_encodings)
def secure_snowflake_allow_read(temporary_database, request):
    """
    A QCFractal snowflake server with authorization/authentication enabled, but allowing
    unauthenticated read
    """

    db_config = temporary_database.config
    with TestingSnowflake(
        db_config,
        encoding=request.param,
        start_flask=True,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=True,
    ) as server:
        yield server


@pytest.fixture(scope="function")
def snowflake_client(snowflake):
    """
    A client connected to a testing snowflake

    This is for a simple snowflake (no security, no compute) because a lot
    of tests will use this. Other tests will need to use a different fixture
    and manually call client() there
    """

    yield snowflake.client()


@pytest.fixture(scope="function")
def old_test_server(temporary_database):
    """
    A QCFractal server with no compute attached, and with security disabled
    """

    # Tighten the service frequency for tests
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    extra_config = {}
    extra_config["service_frequency"] = 5
    extra_config["heartbeat_frequency"] = 3
    extra_config["heartbeat_max_missed"] = 2
    extra_config["database"] = {"pool_size": 0}

    with FractalSnowflake(
        start=True,
        compute_workers=0,
        enable_watching=True,
        database_config=temporary_database.config,
        flask_config="testing",
        extra_config=extra_config,
    ) as server:
        yield server


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

    for i in range(1, max_iter + 1):
        logger.debug(f"Iteration {i}")
        running_services = periodics._update_services()
        logger.debug(f"Number of running services: {running_services}")
        if running_services == 0:
            return True

        server.await_results()

    return False


@pytest.fixture(scope="function")
def torsiondrive_fixture(fractal_test_server):

    # Cannot use this fixture without these services. Also cannot use `mark` and `fixture` decorators
    pytest.importorskip("torsiondrive")
    pytest.importorskip("geometric")
    pytest.importorskip("rdkit")

    client = fractal_test_server.client()
    periodics = fractal_test_server.get_periodics()
    fractal_test_server.start_compute_worker()

    # Add a HOOH
    hooh = ptl.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    # Geometric options
    torsiondrive_options = {
        "initial_molecule": [mol_ret[0]],
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
        update_nested_dict(instance_options, keyword_augments)

        inp = TorsionDriveInput(**instance_options)
        ret = client.add_service([inp], full_return=True)

        if ret.meta.n_inserted:  # In case test already submitted
            compute_key = ret.data.ids[0]
            service = client.query_procedures(compute_key)[0]
            assert service.status == RecordStatusEnum.waiting

        if run_service:
            finished = run_services(fractal_test_server, periodics)
            assert finished

        return ret.data

    yield spin_up_test, fractal_test_server, periodics


def build_adapter_clients(mtype):

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

        fireworks_name = "qcfractal_fireworks_queue"
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

    # Do a final close with existing adapter
    build_queue_adapter(adapter_client).close()


@pytest.fixture(scope="function", params=_adapter_testing)
def fractal_test_server_adapter(request, test_server):
    """
    A Fractal snowflake server with an external compute worker
    """

    adapter_client = build_adapter_clients(request.param)

    client = test_server.client()
    manager = QueueManager(client, adapter_client)
    yield client, test_server, manager
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
        return pytest.skip("Failed to connect to localhost, skipping")
        # print("Failed to connect to localhost, trying MolSSI QCArchive.")
        # try:
        #    requests.get("https://api.qcarchive.molssi.org:443", json={}, timeout=5)
        #    return FractalClient()
        # except (requests.exceptions.ConnectionError, ConnectionRefusedError):
        #    return pytest.skip("Could not make a connection to central Fractal server")


def df_compare(df1, df2, sort=False):
    """checks equality even when columns contain numpy arrays, which .equals and == struggle with"""
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
