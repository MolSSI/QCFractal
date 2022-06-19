from __future__ import annotations

import json
from typing import Dict, Any, Tuple

from qcelemental.models import Molecule

from qcfractal import FractalSnowflake
from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import geoip_path, test_users
from qcportal import PortalClient, ManagerClient
from qcportal.managers import ManagerName
from qcportal.permissions import UserInfo
from qcportal.records import RecordStatusEnum
from qcportal.utils import recursive_normalizer

mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="2234-5678-1234-5678")


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

        api_config = {"jwt_access_token_expires": 1}  # expire tokens in 1 second

        # Smaller api limits (so we can test chunking)
        api_limits = {
            "manager_tasks_claim": 5,
            "manager_tasks_return": 2,
            "get_records": 10,
            "get_dataset_entries": 5,
            "get_molecules": 10,
            "get_managers": 10,
            "get_server_stats": 10,
            "get_error_logs": 10,
            "get_access_logs": 10,
        }

        extra_config["api"] = api_config
        extra_config["api_limits"] = api_limits

        extra_config["enable_security"] = enable_security
        extra_config["allow_unauthenticated_read"] = allow_unauthenticated_read
        extra_config["service_frequency"] = 5
        extra_config["loglevel"] = "DEBUG"
        extra_config["heartbeat_frequency"] = 3
        extra_config["heartbeat_max_missed"] = 2
        extra_config["statistics_frequency"] = 3

        extra_config["database"] = {"pool_size": 0}
        extra_config["log_access"] = log_access
        extra_config["geo_file_path"] = geoip_path

        FractalSnowflake.__init__(
            self,
            start=False,
            compute_workers=0,
            enable_watching=True,
            database_config=database_config,
            flask_config="testing",
            extra_config=extra_config,
        )

        if create_users:
            self.create_users()

        # Start the flask api process if requested
        if start_flask:
            self.start_flask()

    def create_users(self):
        # Get a storage socket and add the roles/users/passwords
        storage = self.get_storage_socket()
        for k, v in test_users.items():
            uinfo = UserInfo(username=k, enabled=True, **v["info"])
            storage.users.add(uinfo, password=v["pw"])

    def get_storage_socket(self) -> SQLAlchemySocket:
        """
        Obtain a new SQLAlchemy socket

        This function will create a new socket instance every time it is called
        """

        return SQLAlchemySocket(self._qcf_config)

    def start_flask(self) -> None:
        """
        Starts the flask subprocess
        """
        if not self._flask_proc.is_alive():
            self._flask_proc.start()
            self.wait_for_flask()

    def stop_flask(self) -> None:
        """
        Stops the flask subprocess
        """
        if self._flask_proc.is_alive():
            self._flask_proc.stop()
            self._flask_started.clear()

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
        client = PortalClient(
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

        # Now that we know it's up, create a manager client
        client = ManagerClient(name_data, self.get_uri(), username=username, password=password)
        client.encoding = self._encoding
        return client


def run_service_constropt(
    storage_socket: SQLAlchemySocket,
    manager_name: ManagerName,
    record_id: int,
    result_data: Dict[str, Any],
    max_iterations: int = 20,
) -> Tuple[bool, int]:
    """
    Runs a service that is based on constrained optimizations
    """

    rec = storage_socket.records.get([record_id], include=["*", "service"])
    assert rec[0]["status"] in [RecordStatusEnum.waiting, RecordStatusEnum.running]

    tag = rec[0]["service"]["tag"]
    priority = rec[0]["service"]["priority"]

    n_optimizations = 0
    n_iterations = 0
    r = 1

    while n_iterations < max_iterations:
        r = storage_socket.services.iterate_services()

        if r == 0:
            break

        n_iterations += 1

        rec = storage_socket.records.get(
            [record_id], include=["*", "service.*", "service.dependencies.*", "service.dependencies.record"]
        )
        assert rec[0]["status"] == RecordStatusEnum.running

        # only do 5 tasks at a time. Tests iteration when stuff is not completed
        manager_tasks = storage_socket.tasks.claim_tasks(manager_name.fullname, limit=5)

        # Sometimes a task may be duplicated in the service dependencies.
        # The C8H6 test has this "feature"
        opt_ids = set(x["record_id"] for x in manager_tasks)
        opt_recs = storage_socket.records.optimization.get(opt_ids, include=["*", "initial_molecule", "task"])
        assert all(x["task"]["priority"] == priority for x in opt_recs)
        assert all(x["task"]["tag"] == tag for x in opt_recs)

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

        rmeta = storage_socket.tasks.update_finished(manager_name.fullname, manager_ret)
        assert rmeta.n_accepted == len(manager_tasks)
        n_optimizations += len(manager_ret)

    return r == 0, n_optimizations


def run_service_simple(
    storage_socket: SQLAlchemySocket,
    manager_name: ManagerName,
    record_id: int,
    result_data: Dict[str, Any],
    max_iterations: int = 20,
) -> Tuple[bool, int]:
    """
    Runs a service that is based on singlepoint calculations
    """

    rec = storage_socket.records.get([record_id], include=["*", "service"])
    assert rec[0]["status"] in [RecordStatusEnum.waiting, RecordStatusEnum.running]

    tag = rec[0]["service"]["tag"]
    priority = rec[0]["service"]["priority"]

    n_records = 0
    n_iterations = 0
    r = 1

    while n_iterations < max_iterations:
        r = storage_socket.services.iterate_services()

        if r == 0:
            break

        n_iterations += 1

        rec = storage_socket.records.get(
            [record_id], include=["*", "service.*", "service.dependencies.*", "service.dependencies.record"]
        )
        assert rec[0]["status"] == RecordStatusEnum.running

        # only do 5 tasks at a time. Tests iteration when stuff is not completed
        manager_tasks = storage_socket.tasks.claim_tasks(manager_name.fullname, limit=5)

        # Sometimes a task may be duplicated in the service dependencies.
        # The C8H6 test has this "feature"
        ids = set(x["record_id"] for x in manager_tasks)
        recs = storage_socket.records.get(ids, include=["id", "record_type", "task"])
        assert all(x["task"]["priority"] == priority for x in recs)
        assert all(x["task"]["tag"] == tag for x in recs)

        manager_ret = {}
        for r in recs:
            # Find out info about what tasks the service spawned
            sock = storage_socket.records.get_socket(r["record_type"])

            # Include any molecules in the record. Unknown ones are ignored
            real_r = sock.get([r["id"]], include=["*", "molecule", "initial_molecule"])[0]

            # Optimizations have initial molecule
            if "initial_molecule" in real_r:
                mol_hash = real_r["initial_molecule"]["identifiers"]["molecule_hash"]
            else:
                mol_hash = real_r["molecule"]["identifiers"]["molecule_hash"]

            key = r["record_type"] + "|" + mol_hash

            task_result = result_data[key]
            manager_ret[r["task"]["id"]] = task_result

        rmeta = storage_socket.tasks.update_finished(manager_name.fullname, manager_ret)
        assert rmeta.n_accepted == len(manager_tasks)
        n_records += len(manager_ret)

    return r == 0, n_records


def compare_validate_molecule(m1: Molecule, m2: Molecule) -> bool:
    """
    Validates, and then compares molecules

    Molecules get validated when added to the server, so if we are comparing
    molecules, we often need to validate the input as well.
    """

    m1_v = Molecule(**m1.dict(), validate=True)
    m2_v = Molecule(**m2.dict(), validate=True)
    return m1_v == m2_v
