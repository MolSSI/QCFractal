from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload, load_only, contains_eager, joinedload
from sqlalchemy.dialects.postgresql import array_agg

from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.base_handlers import BaseServiceHandler
from qcfractal.components.records.gridoptimization.handlers import GridOptimizationHandler
from qcfractal.components.records.torsiondrive.handlers import TorsionDriveHandler
from qcfractal.components.records.db_models import RecordComputeHistoryORM
from qcfractal.portal.outputstore import OutputStore, OutputTypeEnum, CompressionEnum
from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.db_socket.helpers import (
    insert_general,
    get_count,
    get_count_2,
    calculate_limit,
)
from qcfractal.interface.models import (
    ObjectId,
    AllServiceSpecifications,
)
from qcfractal.portal.metadata_models import InsertMetadata, QueryMetadata
from qcfractal.portal.records.models import RecordStatusEnum
from .db_models import ServiceQueueORM, ServiceDependenciesORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Optional, Sequence, Tuple, Any, Sequence, Iterable

    ServiceQueueDict = Dict[str, Any]


class ServiceSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._user_service_limit = root_socket.qcf_config.response_limits.service_queue
        self._max_active_services = root_socket.qcf_config.max_active_services

    def iterate_services(self, *, session: Optional[Session] = None) -> int:

        self._logger.info("Iterating on services")
        # A CTE that contains just service id and all the statuses as an array
        status_cte = (
            select(ServiceDependenciesORM.service_id, array_agg(BaseRecordORM.status).label("task_statuses"))
            .join(BaseRecordORM, BaseRecordORM.id == ServiceDependenciesORM.record_id)
            .group_by(ServiceDependenciesORM.service_id)
            .cte()
        )

        with self.root_socket.optional_session(session) as session:

            # Services where a task has errored
            # Find only those that all tasks are completed or errored, but only those
            # with at least one error
            stmt = (
                select(ServiceQueueORM)
                .join(status_cte, status_cte.c.service_id == ServiceQueueORM.id)
                .join(ServiceQueueORM.record)
                .where(BaseRecordORM.status == RecordStatusEnum.running)
                .where(status_cte.c.task_statuses.contained_by(["complete", "error"]))
                .where(status_cte.c.task_statuses.contains(["error"]))
            )

            err_services = session.execute(stmt).scalars().all()

            # Services whose tasks this iteration are all successfully completed
            stmt = (
                select(ServiceQueueORM)
                .join(status_cte, status_cte.c.service_id == ServiceQueueORM.id)
                .join(ServiceQueueORM.record)
                .where(BaseRecordORM.status == RecordStatusEnum.running)
                .where(status_cte.c.task_statuses.contained_by(["complete"]))
            )
            completed_services = session.execute(stmt).scalars().all()

            if len(err_services) > 0:
                self._logger.info(f"Found {len(err_services)} running services with task failures")
            if len(completed_services) > 0:
                self._logger.info(f"Found {len(completed_services)} running services with completed tasks")

            # Services with an errored task
            for service_orm in err_services:
                error = {
                    "error_type": "service_iteration_error",
                    "error_message": "Some task(s) did not complete successfully",
                }

                self.root_socket.records.update_failed_service(service_orm.record, error)
                session.commit()

                self.root_socket.notify_completed_watch(service_orm.record_id, RecordStatusEnum.error)

            # Completed, successful service dependencies. Service is ready for the next iteration
            for service_orm in completed_services:
                try:
                    self._logger.info(
                        f"Record {service_orm.record_id} (service {service_orm.id}) has all tasks completed. Iterating..."
                    )
                    completed = self.root_socket.records.iterate_service(session, service_orm)
                    service_orm.record.modified_on = datetime.utcnow()
                except Exception as err:
                    session.rollback()

                    error = {
                        "error_type": "service_iteration_error",
                        "error_message": "Error iterating service: " + str(err),
                    }
                    self.root_socket.records.update_failed_service(service_orm.record, error)
                    session.commit()
                    self.root_socket.notify_completed_watch(service_orm.record_id, RecordStatusEnum.error)
                    continue

                # If the service has successfully completed, delete the entry from the Service Queue
                if completed:
                    service_orm.record.compute_history[-1].status = RecordStatusEnum.complete
                    service_orm.record.compute_history[-1].modified_on = datetime.utcnow()
                    service_orm.record.status = RecordStatusEnum.complete
                    service_orm.record.modified_on = datetime.utcnow()
                    session.delete(service_orm)

                    session.commit()
                    self.root_socket.notify_completed_watch(service_orm.record_id, RecordStatusEnum.complete)

            # Should we start more?
            stmt = select(BaseRecordORM).where(
                BaseRecordORM.status == RecordStatusEnum.running, BaseRecordORM.is_service.is_(True)
            )

            running_count = get_count_2(session, stmt)

            self._logger.info(
                f"After iteration, now {running_count} running services. Max is {self._max_active_services}"
            )

            new_service_count = self._max_active_services - running_count

            if new_service_count > 0:
                stmt = (
                    select(ServiceQueueORM)
                    .join(ServiceQueueORM.record)
                    .options(contains_eager(ServiceQueueORM.record))
                    .filter(BaseRecordORM.status == RecordStatusEnum.waiting)
                    .order_by(ServiceQueueORM.priority.desc(), ServiceQueueORM.created_on)
                    .limit(new_service_count)
                )

                new_services = session.execute(stmt).scalars().all()

                running_count += len(new_services)

                if len(new_services) > 0:
                    self._logger.info(f"Attempting to start {len(new_services)} services")

                for service_orm in new_services:

                    # Add a compute history entry. The iterate functions expect that at least one
                    # history entry exists
                    now = datetime.utcnow()

                    hist = RecordComputeHistoryORM()
                    hist.status = RecordStatusEnum.running
                    hist.modified_on = now

                    stdout = OutputStore.compress(
                        OutputTypeEnum.stdout,
                        f"Starting service: {service_orm.record.record_type} at {now}",
                        CompressionEnum.lzma,
                        1,
                    )
                    hist.outputs.append(OutputStoreORM.from_model(stdout))

                    service_orm.record.compute_history.append(hist)
                    service_orm.record.modified_on = now

                    session.commit()

                    try:
                        self.root_socket.records.iterate_service(session, service_orm)
                    except Exception as err:
                        session.rollback()

                        raise
                        error = {
                            "error_type": "service_iteration_error",
                            "error_message": "Error in first iteration of service: " + str(err),
                        }

                        self.root_socket.records.update_failed_service(service_orm.record, error)
                        session.commit()
                        self.root_socket.notify_completed_watch(service_orm.record_id, RecordStatusEnum.error)

        return running_count
