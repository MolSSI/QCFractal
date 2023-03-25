from typing import Optional, List, Dict

from .all_results import AllResultTypes
from .client_base import PortalClientBase
from .managers import (
    ManagerName,
    ManagerActivationBody,
    ManagerUpdateBody,
    ManagerStatusEnum,
)
from .metadata_models import TaskReturnMetadata
from .tasks import TaskClaimBody, TaskReturnBody, TaskInformation


class ManagerClient(PortalClientBase):
    def __init__(
        self,
        name_data: ManagerName,
        address: str = "https://api.qcarchive.molssi.org",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
        show_motd: bool = False,
    ) -> None:
        """Initializes a ManagerClient

        Parameters
        ----------
        name_data
            Information about this manager's name
        address
            The IP and port of the FractalServer instance ("192.168.1.1:8888")
        username
            The username to authenticate with.
        password
            The password to authenticate with.
        verify
            Verifies the SSL connection with a third party server. This may be False if a
            FractalServer was not provided a SSL certificate and defaults back to self-signed
            SSL keys.
        show_motd
            If a Message-of-the-Day is available, display it
        """

        PortalClientBase.__init__(
            self, address=address, username=username, password=password, verify=verify, show_motd=show_motd
        )

        self.manager_name_data = name_data

    def _update_on_server(self, manager_update: ManagerUpdateBody) -> None:
        return self.make_request("patch", f"v1/managers/{self.manager_name_data.fullname}", None, body=manager_update)

    def activate(
        self,
        manager_version: str,
        programs: Dict[str, List[str]],
        tags: List[str],
    ) -> None:
        """Registers/Activates a manager for use on the server

        If an error occurs, an exception is raised.
        """

        manager_info = ManagerActivationBody(
            name_data=self.manager_name_data,
            manager_version=manager_version,
            username=self.username,
            programs=programs,
            tags=tags,
        )

        return self.make_request(
            "post",
            "v1/managers",
            None,
            body=manager_info,
        )

    def deactivate(
        self,
        total_worker_walltime: float,
        total_task_walltime: float,
        active_tasks: int,
        active_cores: int,
        active_memory: float,
    ) -> None:
        manager_update = ManagerUpdateBody(
            status=ManagerStatusEnum.inactive,
            total_worker_walltime=total_worker_walltime,
            total_task_walltime=total_task_walltime,
            active_tasks=active_tasks,
            active_cores=active_cores,
            active_memory=active_memory,
        )

        return self._update_on_server(manager_update)

    def heartbeat(
        self,
        total_worker_walltime: float,
        total_task_walltime: float,
        active_tasks: int,
        active_cores: int,
        active_memory: float,
    ) -> None:

        manager_update = ManagerUpdateBody(
            status=ManagerStatusEnum.active,
            total_worker_walltime=total_worker_walltime,
            total_task_walltime=total_task_walltime,
            active_tasks=active_tasks,
            active_cores=active_cores,
            active_memory=active_memory,
        )

        return self._update_on_server(manager_update)

    def claim(self, tags: List[str], limit: int) -> List[TaskInformation]:

        body = TaskClaimBody(name_data=self.manager_name_data, tags=tags, limit=limit)

        return self.make_request("post", "v1/tasks/claim", List[TaskInformation], body=body)

    def return_finished(self, results: Dict[int, AllResultTypes]) -> TaskReturnMetadata:

        # Chunk based on the server limit
        results_flat = list(results.items())
        n_results = len(results_flat)
        limit = self.server_info["api_limits"]["manager_tasks_return"]

        task_return_meta = TaskReturnMetadata()
        for chunk in range(0, n_results, limit):
            body = TaskReturnBody(
                name_data=self.manager_name_data, results={k: v for k, v in results_flat[chunk : chunk + limit]}
            )

            meta = self.make_request("post", "v1/tasks/return", TaskReturnMetadata, body=body)

            task_return_meta.error_description = meta.error_description
            task_return_meta.rejected_info.extend(meta.rejected_info)
            task_return_meta.accepted_ids.extend(meta.accepted_ids)

            if not meta.success:
                return task_return_meta

        return task_return_meta
