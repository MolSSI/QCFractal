from __future__ import annotations

from qcfractal.storage_sockets.models import WavefunctionStoreORM
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import Dict, Any, List, Optional


class WavefunctionSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._limit = core_socket.qcf_config.response_limits.wavefunction

    def add(self, blobs_list: List[Dict[str, Any]]):
        """
        Adds to the wavefunction key/value store table.

        Parameters
        ----------
        blobs_list : List[Dict[str, Any]]
            A list of wavefunction data blobs to add.

        Returns
        -------
        Dict[str, Any]
            Dict with keys data and meta, where data represent the blob_ids of inserted wavefuction data blobs.
        """

        meta = add_metadata_template()
        blob_ids = []
        with self._core_socket.session_scope() as session:
            for blob in blobs_list:
                if blob is None:
                    blob_ids.append(None)
                    continue

                doc = WavefunctionStoreORM(**blob)
                session.add(doc)
                session.commit()
                blob_ids.append(str(doc.id))
                meta["n_inserted"] += 1

        meta["success"] = True

        return {"data": blob_ids, "meta": meta}

    def get(
        self,
        id: List[str] = None,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        limit: int = None,
        skip: int = 0,
    ) -> Dict[str, Any]:
        """
        Pulls from the wavefunction key/value store table.

        Parameters
        ----------
        id : List[str], optional
            A list of ids to query
        include : Optional[List[str]], optional
            The fields to return, default to return all
        exclude : Optional[List[str]], optional
            The fields to not return, default to return all
        limit : int, optional
            Maximum number of results to return.
            Default is set to 0
        skip : int, optional
            Skips a number of results in the query, used for pagination
            Default is set to 0

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys data and meta, where data is the found wavefunction items
        """

        limit = calculate_limit(self._limit, limit)
        meta = get_metadata_template()

        query = format_query(WavefunctionStoreORM, id=id)
        rdata, meta["n_found"] = self._core_socket.get_query_projection(
            WavefunctionStoreORM, query, limit=limit, skip=skip, include=include, exclude=exclude
        )

        meta["success"] = True

        return {"data": rdata, "meta": meta}
