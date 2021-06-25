from __future__ import annotations

import logging
from qcfractal.interface.models import ObjectId, KVStore, AllServiceSpecifications, InsertMetadata, RecordStatusEnum
from qcfractal.storage_sockets.models import BaseResultORM, ServiceQueueORM
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, calculate_limit
from qcfractal.storage_sockets.sqlalchemy_common import insert_general

from typing import TYPE_CHECKING

from .services import BaseServiceHandler, TorsionDriveHandler

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Union, Optional, Sequence, Tuple


class ServiceQueueSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.service_queue

    def add_orm(
        self, services: List[ServiceQueueORM], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds ServiceQueueORM to the database, taking into account duplicates

        If a service should not be added because the corresponding procedure is already marked
        complete, then that will raise an exception.

        The session is flushed at the end of this function.

        Parameters
        ----------
        services
            ORM objects to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata showing what was added, and a list of returned task ids. These will be in the
            same order as the inputs, and may correspond to newly-inserted ORMs or to existing data.
        """

        # Check for incompatible statuses
        base_result_ids = [x.procedure_id for x in services]
        statuses = self._core_socket.procedure.get(base_result_ids, include=["status"], session=session)

        # TODO - logic will need to be adjusted with new statuses
        # This is an error. These should have been checked before calling this function
        if any(x["status"] == RecordStatusEnum.complete for x in statuses):
            raise RuntimeError(
                "Cannot add ServiceQueueORM for a procedure that is already complete. This is a programmer error"
            )

        with self._core_socket.optional_session(session) as session:
            meta, ids = insert_general(session, services, (ServiceQueueORM.procedure_id,), (ServiceQueueORM.id,))

            return meta, [x[0] for x in ids]
