from __future__ import annotations

from datetime import datetime as dt

from qcfractal.storage_sockets.models import QueueManagerORM, QueueManagerLogORM
from qcfractal.storage_sockets.storage_utils import get_metadata_template, add_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, get_count_fast, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import Union, List, Dict


class ManagerSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._manager_limit = core_socket.qcf_config.response_limits.manager
        self._manager_log_limit = core_socket.qcf_config.response_limits.manager_log

    def update(self, name, **kwargs):

        do_log = kwargs.pop("log", False)

        inc_count = {
            # Increment relevant data
            "submitted": QueueManagerORM.submitted + kwargs.pop("submitted", 0),
            "completed": QueueManagerORM.completed + kwargs.pop("completed", 0),
            "returned": QueueManagerORM.returned + kwargs.pop("returned", 0),
            "failures": QueueManagerORM.failures + kwargs.pop("failures", 0),
        }

        upd = {key: kwargs[key] for key in QueueManagerORM.__dict__.keys() if key in kwargs}

        with self._core_socket.session_scope() as session:
            # QueueManagerORM.objects()  # init
            manager = session.query(QueueManagerORM).filter_by(name=name)
            if manager.count() > 0:  # existing
                upd.update(inc_count, modified_on=dt.utcnow())
                num_updated = manager.update(upd)
            else:  # create new, ensures defaults and validations
                manager = QueueManagerORM(name=name, **upd)
                session.add(manager)
                session.commit()
                num_updated = 1

            if do_log:
                # Pull again in case it was updated
                manager = session.query(QueueManagerORM).filter_by(name=name).first()

                manager_log = QueueManagerLogORM(
                    manager_id=manager.id,
                    completed=manager.completed,
                    submitted=manager.submitted,
                    failures=manager.failures,
                    total_worker_walltime=manager.total_worker_walltime,
                    total_task_walltime=manager.total_task_walltime,
                    active_tasks=manager.active_tasks,
                    active_cores=manager.active_cores,
                    active_memory=manager.active_memory,
                )

                session.add(manager_log)
                session.commit()

        return num_updated == 1

    def get(self, name: str = None, status: str = None, modified_before=None, modified_after=None, limit=None, skip=0):

        limit = calculate_limit(self._manager_limit, limit)
        meta = get_metadata_template()
        query = format_query(QueueManagerORM, name=name, status=status)

        if modified_before:
            query.append(QueueManagerORM.modified_on <= modified_before)

        if modified_after:
            query.append(QueueManagerORM.modified_on >= modified_after)

        data, meta["n_found"] = self._core_socket.get_query_projection(QueueManagerORM, query, limit=limit, skip=skip)
        meta["success"] = True

        return {"data": data, "meta": meta}

    def get_logs(self, manager_ids: Union[List[str], str], timestamp_after=None, limit=None, skip=0):
        limit = calculate_limit(self._manager_log_limit, limit)
        meta = get_metadata_template()
        query = format_query(QueueManagerLogORM, manager_id=manager_ids)

        if timestamp_after:
            query.append(QueueManagerLogORM.timestamp >= timestamp_after)

        data, meta["n_found"] = self._core_socket.get_query_projection(
            QueueManagerLogORM, query, limit=limit, skip=skip, exclude=["id"]
        )
        meta["success"] = True

        return {"data": data, "meta": meta}

    def _copy_managers(self, record_list: Dict):
        """
        copy the given managers as-is to the DB. Used for data migration

        Parameters
        ----------
        record_list : List[Dict[str, Any]]
            list of dict of managers data
        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        manager_names = []
        with self._core_socket.session_scope() as session:
            for manager in record_list:
                doc = session.query(QueueManagerORM).filter_by(name=manager["name"])

                if get_count_fast(doc) == 0:
                    doc = QueueManagerORM(**manager)
                    if isinstance(doc.created_on, float):
                        doc.created_on = dt.fromtimestamp(doc.created_on / 1e3)
                    if isinstance(doc.modified_on, float):
                        doc.modified_on = dt.fromtimestamp(doc.modified_on / 1e3)
                    session.add(doc)
                    session.commit()  # TODO: faster if done in bulk
                    manager_names.append(doc.name)
                    meta["n_inserted"] += 1
                else:
                    name = doc.first().name
                    meta["duplicates"].append(name)  # TODO
                    # If new or duplicate, add the id to the return list
                    manager_names.append(id)
        meta["success"] = True

        ret = {"data": manager_names, "meta": meta}
        return ret
