from __future__ import annotations

from typing import Dict, Any, Tuple, Callable

from qcelemental.models import Molecule
from sqlalchemy import select

from qcarchivetesting import geoip_path, test_users, test_groups
from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.snowflake import FractalSnowflake
from qcportal import PortalClient, ManagerClient
from qcportal.auth import UserInfo, GroupInfo
from qcportal.managers import ManagerName
from qcportal.record_models import RecordStatusEnum

mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="2234-5678-1234-5678")


class DummyJobStatus:
    """
    Functor for updating progress and cancelling internal jobs

    This is a dummy version used for testing
    """

    def __init__(self):
        self._runner_uuid = "1234-5678-9101-1213"
        pass

    def update_progress(self, progress: int):
        pass

    def cancelled(self) -> bool:
        return False

    def deleted(self) -> bool:
        return False


class QCATestingSnowflake(FractalSnowflake):
    """
    A snowflake class used for testing

    This mostly behaves like FractalSnowflake, but
    allows for some extra features such as manual handling of internal jobs
    and creating storage sockets from an instance.

    By default, the job runner and worker subprocesses are not started.
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

        # expire tokens in 5 seconds
        # Too short and this interferes with some other tests
        api_config = {"jwt_access_token_expires": 5}

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

        for g in test_groups:
            storage.groups.add(GroupInfo(groupname=g))

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

    def start_job_runner(self) -> None:
        """
        Starts the job runner subprocess
        """
        if not self._job_runner_proc.is_alive():
            self._job_runner_proc.start()

    def stop_job_runner(self) -> None:
        """
        Stops the job_runner subprocess
        """
        if self._job_runner_proc.is_alive():
            self._job_runner_proc.stop()

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


def run_service(
    storage_socket: SQLAlchemySocket,
    manager_name: ManagerName,
    record_id: int,
    task_key_generator: Callable,
    result_data: Dict[str, Any],
    max_iterations: int = 20,
) -> Tuple[bool, int]:
    """
    Runs a service
    """

    rec = storage_socket.records.get([record_id], include=["*", "service"])
    assert rec[0]["status"] in [RecordStatusEnum.waiting, RecordStatusEnum.running]

    owner_user = rec[0]["owner_user"]
    owner_group = rec[0]["owner_group"]

    tag = rec[0]["service"]["tag"]
    priority = rec[0]["service"]["priority"]
    service_id = rec[0]["service"]["id"]

    n_records = 0
    n_iterations = 0
    finished = False

    while n_iterations < max_iterations:
        with storage_socket.session_scope() as session:
            n_services = storage_socket.services.iterate_services(session, DummyJobStatus())

            # iterate_services will handle errors
            if n_services == 0:
                finished = True
                break

            # Kinda hacky...
            # Run any internal jobs that iterate_services added
            jobname = f"iterate_service_{service_id}"
            stmt = select(InternalJobORM).where(InternalJobORM.unique_name == jobname)
            job_orm = session.execute(stmt).scalar_one_or_none()

            if job_orm is not None:
                storage_socket.internal_jobs._run_single(session, job_orm, DummyJobStatus())
                # The function that iterates a service returns True if it is finished
                if job_orm.result is True:
                    rec = storage_socket.records.get([record_id], include=["status", "service.*"])
                    assert rec[0]["status"] == RecordStatusEnum.complete
                    assert rec[0]["service"] is None
                    finished = True
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
        recs = storage_socket.records.get(ids, include=["id", "owner_user", "owner_group", "record_type", "task"])
        assert all(x["owner_user"] == owner_user for x in recs)
        assert all(x["owner_group"] == owner_group for x in recs)
        assert all(x["task"]["priority"] == priority for x in recs)
        assert all(x["task"]["tag"] == tag for x in recs)

        manager_ret = {}
        for r in recs:
            # Find out info about what tasks the service spawned
            sock = storage_socket.records.get_socket(r["record_type"])

            # Include any molecules in the record. Unknown ones are ignored
            real_r = sock.get([r["id"]], include=["*", "molecule", "initial_molecule"])[0]

            # The results dict has keys that are generated by a function
            # That same function is passed into this function
            task_key = task_key_generator(real_r)

            task_result = result_data.get(task_key, None)
            if task_result is None:
                raise RuntimeError(f"Cannot find task results! key = {task_key}")

            manager_ret[r["task"]["id"]] = task_result

        rmeta = storage_socket.tasks.update_finished(manager_name.fullname, manager_ret)
        assert rmeta.n_accepted == len(manager_tasks)
        n_records += len(manager_ret)

    return finished, n_records


def compare_validate_molecule(m1: Molecule, m2: Molecule) -> bool:
    """
    Validates, and then compares molecules

    Molecules get validated when added to the server, so if we are comparing
    molecules, we often need to validate the input as well.
    """

    m1_v = Molecule(**m1.dict(), validate=True)
    m2_v = Molecule(**m2.dict(), validate=True)
    return m1_v == m2_v
