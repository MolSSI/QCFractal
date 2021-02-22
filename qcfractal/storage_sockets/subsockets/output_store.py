from __future__ import annotations

from qcfractal.storage_sockets.models import KVStoreORM
from qcfractal.interface.models import KVStore
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.interface.models import ObjectId
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List


class OutputStoreSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._limit = core_socket.qcf_config.response_limits.output_store

    def add(self, outputs: List[KVStore]):
        """
        Adds to the key/value store table.

        Parameters
        ----------
        outputs : List[Any]
            A list of KVStore objects add.

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys data and meta, data is the ids of added blobs
        """

        meta = add_metadata_template()
        output_ids = []
        with self._core_socket.session_scope() as session:
            for output in outputs:
                if output is None:
                    output_ids.append(None)
                    continue

                entry = KVStoreORM(**output.dict())
                session.add(entry)
                session.commit()
                output_ids.append(str(entry.id))
                meta["n_inserted"] += 1

        meta["success"] = True

        return {"data": output_ids, "meta": meta}

    def get(self, id: List[ObjectId] = None, limit: int = None, skip: int = 0):
        """
        Pulls from the key/value store table.

        Parameters
        ----------
        id : List[str]
            A list of ids to query
        limit : Optional[int], optional
            Maximum number of results to return.
        skip : Optional[int], optional
            skip the `skip` results
        Returns
        -------
        Dict[str, Any]
            Dictionary with keys data and meta, data is a key-value dictionary of found key-value stored items.
        """

        limit = calculate_limit(self._limit, limit)
        meta = get_metadata_template()

        query = format_query(KVStoreORM, id=id)

        rdata, meta["n_found"] = self._core_socket.get_query_projection(KVStoreORM, query, limit=limit, skip=skip)

        meta["success"] = True

        data = {}
        # TODO - after migrating everything, remove the 'value' column in the table
        for d in rdata:
            val = d.pop("value")
            if d["data"] is None:
                # Set the data field to be the string or dictionary
                d["data"] = val

                # Remove these and let the model handle the defaults
                d.pop("compression")
                d.pop("compression_level")

            # The KVStore constructor can handle conversion of strings and dictionaries
            data[d["id"]] = KVStore(**d)

        return {"data": data, "meta": meta}
