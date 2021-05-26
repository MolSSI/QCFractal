"""
Base class for computation procedure handlers
"""

from __future__ import annotations

import abc
from ....interface.models import AllProcedureSpecifications, AllResultTypes

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...models import TaskQueueORM
    from ....interface.models import InsertMetadata, ObjectId
    from typing import Sequence, Tuple, List


class BaseProcedureHandler(abc.ABC):
    @abc.abstractmethod
    def verify_input(self, data):
        pass

    @abc.abstractmethod
    def create(
        self, session: Session, molecule_ids: Sequence[int], data: AllProcedureSpecifications
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        pass

    @abc.abstractmethod
    def update_completed(self, session: Session, task_orm: TaskQueueORM, manager_name: str, result: AllResultTypes):
        pass
