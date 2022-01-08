"""
Contains testing infrastructure for QCFractal.
"""

import copy
import gc
import json
import logging
import lzma
import os
import pkgutil
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from typing import Dict, List, Union, Any, Tuple

import numpy as np
import pandas as pd
import pydantic
import pytest
import requests
from qcelemental.models import Molecule, FailedOperation, OptimizationResult, AtomicResult
from qcelemental.models.results import WavefunctionProperties

from qcportal.managers import ManagerName
from qcportal.permissions import UserInfo
from qcportal.records.gridoptimization import GridoptimizationInputSpecification
from qcportal.records.optimization import OptimizationInputSpecification
from qcportal.records.singlepoint import SinglepointInputSpecification
from qcportal.records.torsiondrive import TorsiondriveInputSpecification
from .config import FractalConfig, update_nested_dict
from .db_socket.socket import SQLAlchemySocket
from .interface import FractalClient
from .interface.models import TorsionDriveInput
from .periodics import FractalPeriodics
from qcportal import PortalClient, ManagerClient
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.utils import recursive_normalizer
from .postgres_harness import TemporaryPostgres
from qcfractalcompute import build_queue_adapter, QueueManager
from .snowflake import FractalSnowflake, attempt_client_connect
from concurrent.futures import ProcessPoolExecutor

adapter_client = ProcessPoolExecutor(max_workers=2)

# Path to this file (directory only)
_my_path = os.path.dirname(os.path.abspath(__file__))

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
    parser.addoption(
        "--runfull",
        action="store",
        help="Run full end-to-end tests only, given the adapter type",
        choices=["snowflake"],
    )


def pytest_collection_modifyitems(config, items):
    """
    Handle test triggers based on the CLI flags

    Use decorators:
    @pytest.mark.slow
    """
    runslow = config.getoption("--runslow")
    runfull = config.getoption("--runfull")

    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    skip_full = pytest.mark.skip(reason="need --runfull option to run")
    for item in items:
        if "slow" in item.keywords and not runslow:
            item.add_marker(skip_slow)
        if "fulltest" in item.keywords and not runfull:
            item.add_marker(skip_full)


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: Mark a given test as slower than most other tests")
    config.addinivalue_line("markers", "full: Mark a given test as a full end-to-end test")


def pytest_unconfigure(config):
    pass


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

    data_path = os.path.join(_my_path, "tests", "procedure_data")
    file_path = os.path.join(data_path, name + ".json.xz")
    is_xz = True

    if not os.path.exists(file_path):
        file_path = os.path.join(data_path, name + ".json")
        is_xz = False

    if not os.path.exists(file_path):
        raise RuntimeError(f"Procedure data file {file_path} not found!")

    if is_xz:
        with lzma.open(file_path, "rt") as f:
            data = json.load(f)
    else:
        with open(file_path, "r") as f:
            data = json.load(f)

    record_type = data["record_type"]
    if record_type == "singlepoint":
        input_type = SinglepointInputSpecification
        result_type = Union[AtomicResult, FailedOperation]
        molecule_type = Molecule
    elif record_type == "optimization":
        input_type = OptimizationInputSpecification
        result_type = Union[OptimizationResult, FailedOperation]
        molecule_type = Molecule
    elif record_type == "torsiondrive":
        input_type = TorsiondriveInputSpecification
        result_type = Dict[str, Union[OptimizationResult, FailedOperation]]
        molecule_type = List[Molecule]
    elif record_type == "gridoptimization":
        input_type = GridoptimizationInputSpecification
        result_type = Dict[str, Union[OptimizationResult, FailedOperation]]
        molecule_type = Molecule
    else:
        raise RuntimeError(f"Unknown procedure '{record_type}' in test!")

    molecule = pydantic.parse_obj_as(molecule_type, data["molecule"])

    return (
        pydantic.parse_obj_as(input_type, data["specification"]),
        molecule,
        pydantic.parse_obj_as(result_type, data["result"]),
    )


def load_molecule_data(name: str) -> Molecule:
    """
    Loads a molecule object for use in testing
    """

    data_path = os.path.join(_my_path, "tests", "molecule_data")
    file_path = os.path.join(data_path, name + ".json")
    return Molecule.from_file(file_path)


def load_wavefunction_data(name: str) -> WavefunctionProperties:
    """
    Loads a wavefunction object for use in testing
    """

    data_path = os.path.join(_my_path, "tests", "wavefunction_data")
    file_path = os.path.join(data_path, name + ".json")

    with open(file_path, "r") as f:
        data = json.load(f)
    return WavefunctionProperties(**data)


def load_ip_test_data():
    """
    Loads data for testing IP logging
    """

    file_path = os.path.join(_my_path, "tests", "MaxMind-DB", "source-data", "GeoIP2-City-Test.json")

    with open(file_path, "r") as f:
        d = json.load(f)

    # Stored as a list containing a dictionary with one key. Convert to a regular dict
    ret = {}
    for x in d:
        ret.update(x)

    return ret


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
    A postgres server instance

    This does not contain the target database, but does contain an empty template database
    that can be used by snowflake, etc.

    This is built only once per session, and automatically deleted after the session. It uses
    a pytest-provided session-scoped temporary directory
    """

    logger = logging.getLogger(__name__)

    db_path = str(tmp_path_factory.mktemp("db"))
    tmp_pg = TemporaryPostgres(data_dir=db_path)
    pg_harness = tmp_pg.harness
    logger.debug(f"Using database located at {db_path} with uri {pg_harness.database_uri()}")
    assert pg_harness.is_alive(False) and not pg_harness.is_alive(True)

    # Create the database, and we will use that as a template
    # We connect to the "postgres" database, so as not to be using the database we want to copy
    pg_harness.create_database()
    pg_harness.sql_command(
        f"CREATE DATABASE template_db TEMPLATE {tmp_pg.config.database_name};", database_name="postgres", returns=False
    )

    # Delete the database - we will create it from the template later
    pg_harness.delete_database()

    yield pg_harness

    if tmp_pg:
        tmp_pg.stop()


@pytest.fixture(scope="function")
def temporary_database(postgres_server):
    """
    A temporary database that only lasts for one function

    It is part of the postgres instance given by the postgres_server fixture
    """

    # Make sure that the database process is up
    assert postgres_server.is_alive(False)

    # Create the database from the template
    db_name = postgres_server.config.database_name
    postgres_server.sql_command(
        f"CREATE DATABASE {db_name} TEMPLATE template_db;", database_name="postgres", returns=False
    )

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
    cfg_dict["log_access"] = True
    cfg_dict["geo_file_path"] = os.path.join(_my_path, "tests", "MaxMind-DB", "test-data", "GeoIP2-City-Test.mmdb")
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
        log_access=True,
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
        extra_config["hide_internal_errors"] = False
        extra_config["heartbeat_frequency"] = 3
        extra_config["heartbeat_max_missed"] = 2
        extra_config["database"] = {"pool_size": 0}
        extra_config["log_access"] = log_access
        extra_config["geo_file_path"] = os.path.join(
            _my_path, "tests", "MaxMind-DB", "test-data", "GeoIP2-City-Test.mmdb"
        )

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

    def get_compute_manager(self, name: str) -> QueueManager:
        """
        Obtain a new QueueManager attached to this instance

        This function will create a new QueueManager object every time it is called
        """

        adapter_client = ProcessPoolExecutor(max_workers=2)
        return QueueManager(adapter_client, manager_name=name)

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

    def manager_client(self, name_data: ManagerName, username=None, password=None) -> ManagerClient:
        """
        Obtain a manager client connected to this snowflake

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

        attempt_client_connect(
            self.get_uri(),
            username=username,
            password=password,
        )

        # Now that we know it's up, create a manager client
        client = ManagerClient(name_data, self.get_uri(), username=username, password=password)
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
def fulltest_client(temporary_database, pytestconfig):
    """
    A portal client used for full end-to-end tests

    This may be from a snowflake, or from something else
    """

    db_config = temporary_database.config
    client_type = pytestconfig.getoption("--runfull")

    if client_type == "snowflake":
        with TestingSnowflake(db_config, encoding="application/json") as server:
            server.start_compute_worker()
            server.start_periodics()
            server.start_flask()
            yield server.client()
    else:
        raise RuntimeError("Unknown client type for tests! This is a developer error")


mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="2234-5678-1234-5678")


def populate_db(storage_socket: SQLAlchemySocket):
    """
    Populates the db with tasks in all statuses
    """

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0", "rdkit": None, "geometric": None},
        tags=["tag1", "tag2", "tag3", "tag6"],
    )

    input_spec_0, molecule_0, result_data_0 = load_procedure_data("psi4_methane_opt_sometraj")
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_water_gradient")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_water_hessian")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("rdkit_water_energy")
    input_spec_5, molecule_5, result_data_5 = load_procedure_data("psi4_benzene_energy_2")
    input_spec_6, molecule_6, result_data_6 = load_procedure_data("psi4_water_energy")

    meta, id_0 = storage_socket.records.optimization.add(input_spec_0, [molecule_0], "tag0", PriorityEnum.normal)
    meta, id_1 = storage_socket.records.singlepoint.add(input_spec_1, [molecule_1], "tag1", PriorityEnum.high)
    meta, id_2 = storage_socket.records.singlepoint.add(input_spec_2, [molecule_2], "tag2", PriorityEnum.high)
    meta, id_3 = storage_socket.records.singlepoint.add(input_spec_3, [molecule_3], "tag3", PriorityEnum.high)
    meta, id_4 = storage_socket.records.singlepoint.add(input_spec_4, [molecule_4], "tag4", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add(input_spec_5, [molecule_5], "tag5", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add(input_spec_6, [molecule_6], "tag6", PriorityEnum.normal)
    all_id = id_0 + id_1 + id_2 + id_3 + id_4 + id_5 + id_6

    # 0 = waiting   1 = complete   2 = running
    # 3 = error     4 = cancelled  5 = deleted
    # 6 = invalid

    # claim only the ones we want to be complete, running, or error (1, 2, 3, 6)
    # 6 needs to be complete to be invalidated
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=4)
    assert len(tasks) == 4

    # we don't send back the one we want to be 'running' still (#2)
    storage_socket.tasks.update_finished(
        mname1.fullname,
        {
            # tasks[1] is left running (corresponds to record 2)
            tasks[0]["id"]: result_data_1,
            tasks[2]["id"]: result_data_3,
            tasks[3]["id"]: result_data_6,
        },
    )

    # Add some more entries to the history of #3 (failing)
    for i in range(4):
        meta = storage_socket.records.reset(id_3)
        assert meta.success
        tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=1)
        assert len(tasks) == 1
        assert tasks[0]["tag"] == "tag3"

        storage_socket.tasks.update_finished(
            mname1.fullname,
            {
                tasks[0]["id"]: result_data_3,
            },
        )

    meta = storage_socket.records.cancel(id_4)
    assert meta.n_updated == 1
    meta = storage_socket.records.delete(id_5)
    assert meta.n_deleted == 1
    meta = storage_socket.records.invalidate(id_6)
    assert meta.n_updated == 1

    rec = storage_socket.records.get(all_id, include=["status"])
    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.running
    assert rec[3]["status"] == RecordStatusEnum.error
    assert rec[4]["status"] == RecordStatusEnum.cancelled
    assert rec[5]["status"] == RecordStatusEnum.deleted
    assert rec[6]["status"] == RecordStatusEnum.invalid

    return all_id


def run_service_constropt(
    record_id: int, result_data: Dict[str, Any], storage_socket: SQLAlchemySocket, max_iterations: int = 20
) -> Tuple[bool, int]:
    """
    Runs a service that is based on constrained optimizations
    """

    # A manager for completing the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={
            "geometric": None,
            "psi4": None,
        },
        tags=["*"],
    )
    rec = storage_socket.records.get([record_id])
    assert rec[0]["status"] == RecordStatusEnum.waiting

    r = storage_socket.services.iterate_services()

    n_optimizations = 0
    n_iterations = 1

    while r > 0 and n_iterations <= max_iterations:
        rec = storage_socket.records.get(
            [record_id], include=["*", "service.*", "service.dependencies.*", "service.dependencies.record"]
        )
        assert rec[0]["status"] == RecordStatusEnum.running

        manager_tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=10)

        # Sometimes a task may be duplicated in the service dependencies.
        # The C8H6 test has this "feature"
        opt_ids = set(x["record_id"] for x in manager_tasks)
        opt_recs = storage_socket.records.optimization.get(opt_ids, include=["*", "initial_molecule", "task"])
        assert all(x["task"]["priority"] == PriorityEnum.low for x in opt_recs)
        assert all(x["task"]["tag"] == "test_tag" for x in opt_recs)

        manager_ret = {}
        for opt in opt_recs:
            # Find out info about what tasks the service spawned
            mol_hash = opt["initial_molecule"]["identifiers"]["molecule_hash"]
            constraints = opt["specification"]["keywords"].get("constraints", None)

            # Lookups may depend on floating point values
            constraints = recursive_normalizer(constraints)

            # This is the key in the dictionary of optimization results
            constraints_str = json.dumps(constraints, sort_keys=True)

            optresult_key = mol_hash + "|" + constraints_str

            opt_data = result_data[optresult_key]
            manager_ret[opt["task"]["id"]] = opt_data

        rmeta = storage_socket.tasks.update_finished(mname1.fullname, manager_ret)
        assert rmeta.n_accepted == len(manager_tasks)
        n_optimizations += len(manager_ret)

        # may or may not iterate - depends on if all tasks done
        r = storage_socket.services.iterate_services()
        n_iterations += 1

    return r == 0, n_optimizations
