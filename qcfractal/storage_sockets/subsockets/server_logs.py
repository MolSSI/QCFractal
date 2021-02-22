from __future__ import annotations

import logging
from sqlalchemy import desc

from qcfractal.storage_sockets.models import AccessLogORM, ServerStatsLogORM
from qcfractal.storage_sockets.storage_utils import get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import get_count_fast, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket


class ServerLogSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._access_log_limit = core_socket.qcf_config.response_limits.access_logs
        self._server_log_limit = core_socket.qcf_config.response_limits.server_logs


    # TODO - getting access logs

    def save_access(self, log_data):
        with self._core_socket.session_scope() as session:
            log = AccessLogORM(**log_data)
            session.add(log)
            session.commit()


    def update(self):

        table_info = self._core_socket.custom_query("database_stats", "table_information")["data"]

        # Calculate table info
        table_size = 0
        index_size = 0
        for row in table_info["rows"]:
            table_size += row[2] - row[3] - (row[4] or 0)
            index_size += row[3]

        # Calculate result state info, turns out to be very costly for large databases
        # state_data = self.custom_query("result", "count", groupby={'result_type', 'status'})["data"]
        # result_states = {}

        # for row in state_data:
        #     result_states.setdefault(row["result_type"], {})
        #     result_states[row["result_type"]][row["status"]] = row["count"]
        result_states = {}

        counts = {}
        for table in ["collection", "molecule", "base_result", "kv_store", "access_log"]:
            counts[table] = self._core_socket.custom_query("database_stats", "table_count", table_name=table)["data"][0]

        # Build out final data
        data = {
            "collection_count": counts["collection"],
            "molecule_count": counts["molecule"],
            "result_count": counts["base_result"],
            "kvstore_count": counts["kv_store"],
            "access_count": counts["access_log"],
            "result_states": result_states,
            "db_total_size": self._core_socket.custom_query("database_stats", "database_size")["data"],
            "db_table_size": table_size,
            "db_index_size": index_size,
            "db_table_information": table_info,
        }

        with self._core_socket.session_scope() as session:
            log = ServerStatsLogORM(**data)
            session.add(log)
            session.commit()

        return data

    def get(self, before=None, after=None, limit=None, skip=0):

        limit = calculate_limit(self._server_log_limit, limit)
        meta = get_metadata_template()
        query = []

        if before:
            query.append(ServerStatsLogORM.timestamp <= before)

        if after:
            query.append(ServerStatsLogORM.timestamp >= after)

        with self._core_socket.session_scope() as session:
            pose = session.query(ServerStatsLogORM).filter(*query).order_by(desc("timestamp"))
            meta["n_found"] = get_count_fast(pose)

            data = pose.limit(limit).offset(skip).all()
            data = [d.to_dict() for d in data]

        meta["success"] = True

        return {"data": data, "meta": meta}
