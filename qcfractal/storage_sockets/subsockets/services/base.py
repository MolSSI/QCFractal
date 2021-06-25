"""
Base class for computation procedure handlers
"""

from __future__ import annotations

import abc
from ....interface.models import AllServiceSpecifications, RecordStatusEnum
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
    def create_services(
        self, session: Session, service_orm: Sequence[_ORM_T], tag: Optional[str], priority: PriorityEnum
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        pass

    @abc.abstractmethod
    def iterate(self, session: Session, service_orm: ServiceQueueORM) -> bool:
        """
        Run the next iteration of a service

        Parameters
        ----------
        session
        service_orm

        Returns
        -------
        :
            True if the service has completed successfully
        """
        pass

    @staticmethod
    def tasks_done(service_orm: ServiceQueueORM) -> bool:
        """
        Check if requested tasks are complete.
        """

        tasks = service_orm.tasks_obj

        if len(tasks) == 0:
            return True

        status_values = set(x.procedure_obj.status for x in tasks)

        if RecordStatusEnum.waiting in status_values:
            return False
        elif RecordStatusEnum.running in status_values:
            return False
        else:
            return True

    def submit_tasks(
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

            service_task = ServiceQueueTasks(procedure_id=added_ids[0], service_id=service_orm.id, extras=key)
            service_orm.tasks_obj.append(service_task)
            all_added_ids.append(added_ids[0])

        return all_added_ids
