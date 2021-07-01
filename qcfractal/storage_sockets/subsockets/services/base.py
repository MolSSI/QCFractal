"""
Base class for computation procedure handlers
"""

from __future__ import annotations

import abc
from ....interface.models import AllServiceSpecifications
from ...models import ServiceQueueTasks

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...sqlalchemy_socket import SQLAlchemySocket
    from ...models import Base, ServiceQueueORM
    from ....interface.models import InsertMetadata, ObjectId, AllProcedureSpecifications, Molecule, PriorityEnum
    from typing import Tuple, List, Sequence, Dict, Any, Optional, TypeVar

    _ORM_T = TypeVar("_ORM_T", bound=Base)


class BaseServiceHandler(abc.ABC):
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket

    @abc.abstractmethod
    def verify_input(self, data):
        pass

    @abc.abstractmethod
    def create_records(
        self, session: Session, service_input: AllServiceSpecifications
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        pass

    @abc.abstractmethod
    def create_tasks(
        self, session: Session, service_orm: Sequence[_ORM_T], tag: Optional[str], priority: PriorityEnum
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        pass

    @abc.abstractmethod
    def iterate(self, session: Session, service_orm: ServiceQueueORM) -> bool:
        """
        Run the next iteration of a service

        Returns
        -------
        :
            True if the service has completed successfully
        """
        pass

    def submit_subtasks(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
        task_inputs: Sequence[Tuple[Dict[str, Any], Molecule, AllProcedureSpecifications]],
    ) -> List[ObjectId]:
        service_orm.tasks_obj = []

        all_added_ids = []

        for key, molecule, spec in task_inputs:
            meta, added_ids = self._core_socket.procedure.create([molecule], spec, session=session)

            if not meta.success:
                raise RuntimeError("Problem submitting task: {}.".format(meta.error_string))

            service_task = ServiceQueueTasks(procedure_id=added_ids[0], service_id=service_orm.id, extras=key)  # type: ignore
            service_orm.tasks_obj.append(service_task)
            all_added_ids.append(added_ids[0])

        return all_added_ids
