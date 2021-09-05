"""
Base class for computation procedure handlers
"""

from __future__ import annotations

import abc

from qcelemental.models import Molecule

from qcfractal.interface.models import (
    AllProcedureSpecifications,
    AllResultTypes,
    AllServiceSpecifications,
)
from typing import TYPE_CHECKING, Dict, Any

from qcfractal.db_socket.socket import SQLAlchemySocket

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket import BaseORM
    from qcfractal.components.services.db_models import ServiceQueueTasks, ServiceQueueORM
    from qcfractal.components.tasks.db_models import TaskQueueORM
    from qcfractal.interface.models import InsertMetadata, ObjectId, PriorityEnum
    from typing import Sequence, Tuple, List, Optional, TypeVar

    _ORM_T = TypeVar("_ORM_T", bound=BaseORM)


class BaseProcedureHandler(abc.ABC):
    @abc.abstractmethod
    def validate_input(self, spec: AllProcedureSpecifications):
        """
        Validates input from the user

        This function checks for any issues with the input and raises a ValidationError if
        there is an issue

        The session is flushed at the end of this function

        Parameters
        ----------
        spec
            An input specification to check

        Raises
        ------
        ValidationError if the specification is not valid
        """
        pass

    @abc.abstractmethod
    def create_records(
        self,
        session: Session,
        molecule_ids: Sequence[int],
        spec: AllProcedureSpecifications,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """Creates/Inserts procedures (and the corresponding tasks) into the database

        This will create the procedure objects in the database if they do not exist, and also tasks for the
        new procedures.

        The returned list of ids (the second element of the tuple) will be in the same order as the input molecules


        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        molecule_ids
            List or other sequence of molecule IDs to create results for
        spec
            Specification of the procedure

        Returns
        -------
        :
            A tuple containing information about which procedures were inserted, and a list of IDs corresponding
            to the procedures in the database (new or existing). This will be in the same order as the input
            molecules.
        """
        pass

    @abc.abstractmethod
    def create_tasks(
        self, session: Session, proc_orm: Sequence[_ORM_T], tag: Optional[str], priority: PriorityEnum
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Creates tasks in the database for a procedure from an ORM

        It is assumed that the tasks can be created for each procedure (ie, the status of the result is not complete).
        If this is not true, than an exception will be raised

        If an existing task assigned to a procedure is found, that will be left alone.

        The session is flushed at the end of this function

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        proc_orm
            An ORM representing the procedure to make the task for
        tag
            A tag to assign to new tasks
        priority
            A priority to assign to new tasks

        Returns
        -------
        :
            A tuple containing information about which tasks were inserted, and a list of IDs corresponding
            to the tasks in the database (new or existing). This will be in the same order as the input
            procedure ORM.
        """
        pass

    @abc.abstractmethod
    def update_completed(self, session: Session, task_orm: TaskQueueORM, manager_name: str, result: AllResultTypes):
        """
        Update the database with information from a completed procedure

        The session may or may not be flushed during this function

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        task_orm
            A TaskORM object corresponding to the incomplete procedure in the database
        manager_name
            Name of the manager that completed this task
        result
            The result of the computation to add to the database
        """
        pass


class BaseServiceHandler(abc.ABC):
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket

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
            meta, added_ids = self.root_socket.task.create([molecule], spec, session=session)

            if not meta.success:
                raise RuntimeError("Problem submitting task: {}.".format(meta.error_string))

            service_task = ServiceQueueTasks(procedure_id=added_ids[0], service_id=service_orm.id, extras=key)  # type: ignore
            service_orm.tasks_obj.append(service_task)
            all_added_ids.append(added_ids[0])

        return all_added_ids
