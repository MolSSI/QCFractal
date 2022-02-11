from typing import Optional, List, Dict, Any

from .client_base import PortalClientBase
from .managers import (
    ManagerName,
    ManagerActivationBody,
    ManagerUpdateBody,
    ManagerStatusEnum,
)
from .metadata_models import TaskReturnMetadata
from .records import AllResultTypes
from .tasks import TaskClaimBody, TaskReturnBody


class ManagerClient(PortalClientBase):
    def __init__(
        self,
        name_data: ManagerName,
        address: str = "api.qcarchive.molssi.org:443",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
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
        """

        PortalClientBase.__init__(self, address=address, username=username, password=password, verify=verify)

        self.manager_name_data = name_data

    def _update_on_server(self, manager_update: ManagerUpdateBody) -> None:
        return self._auto_request(
            "patch",
            f"v1/managers/{self.manager_name_data.fullname}",
            ManagerUpdateBody,
            None,
            None,
            manager_update,
            None,
        )

    def activate(
        self,
        manager_version: str,
        qcengine_version: str,
        programs: Dict[str, Any],
        tags: List[str],
    ) -> None:
        """Registers/Activates a manager for use on the server

        If an error occurs, an exception is raised.
        """

        manager_info = {
            "name_data": self.manager_name_data,
            "manager_version": manager_version,
            "qcengine_version": qcengine_version,
            "username": self.username,
            "programs": programs,
            "tags": tags,
        }

        return self._auto_request(
            "post",
            "v1/managers",
            ManagerActivationBody,
            None,
            None,
            manager_info,
            None,
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

    def claim(self, limit: int) -> List[Dict[str, Any]]:

        claim_data = {"name_data": self.manager_name_data, "limit": limit}

        return self._auto_request(
            "post",
            "v1/tasks/claim",
            TaskClaimBody,
            None,
            List[Dict[str, Any]],
            claim_data,
            None,
        )

    def return_finished(self, results: Dict[int, AllResultTypes]) -> TaskReturnMetadata:

        # Chunk based on the server limit
        results_flat = list(results.items())
        n_results = len(results_flat)
        limit = self.server_info["api_limits"]["manager_tasks_return"]

        task_return_meta = TaskReturnMetadata()
        for chunk in range(0, n_results, limit):
            return_data = {
                "name_data": self.manager_name_data,
                "results": {k: v for k, v in results_flat[chunk : chunk + limit]},
            }

            meta = self._auto_request(
                "post",
                "v1/tasks/return",
                TaskReturnBody,
                None,
                TaskReturnMetadata,
                return_data,
                None,
            )

            task_return_meta.error_description = meta.error_description
            task_return_meta.rejected_info.extend([(x + chunk, y) for x, y in meta.rejected_info])
            task_return_meta.accepted_ids.extend(meta.accepted_ids)

            if not meta.success:
                return task_return_meta

        return task_return_meta
