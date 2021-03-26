from __future__ import annotations

import logging
from qcfractal.storage_sockets.models import KVStoreORM
from qcfractal.storage_sockets.sqlalchemy_common import get_query_proj_columns
from qcfractal.interface.models import KVStore, ObjectId

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Optional, Sequence, Dict, Any

    OutputDict = Dict[str, Any]


class OutputStoreSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.output_store

    @staticmethod
    def output_to_orm(output: KVStore) -> KVStoreORM:
        return KVStoreORM(**output.dict())  # type: ignore

    @staticmethod
    def cleanup_outputstore_dict(out_dict: OutputDict) -> OutputDict:
        # Old way: store a plain string or dict in "value"
        # New way: store (possibly) compressed output in "data"
        val = out_dict.pop("value")

        # If stored the old way, convert to the new way
        if out_dict["data"] is None:
            # Set the data field to be the string or dictionary
            out_dict["data"] = val

            # Remove these and let the model handle the defaults
            out_dict.pop("compression")
            out_dict.pop("compression_level")

        # TODO - int id
        if "id" in out_dict:
            out_dict["id"] = ObjectId(out_dict["id"])

        return out_dict

    def add_internal(self, session: Session, outputs: Sequence[KVStore]) -> List[ObjectId]:
        """
        Inserts output store data into an existing session

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        Changes are not committed to to the database, but they are flushed.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to add the data to
        outputs
            A sequence of output store data to add.

        Returns
        -------
        :
            A list of all newly-inserted IDs
        """

        # Since outputs can be relatively large, we don't explicitly create
        # a list of ORMs (one for each output). We only go one at a time

        output_ids = []

        for output in outputs:
            output_orm = self.output_to_orm(output)
            session.add(output_orm)
            session.flush()
            output_ids.append(ObjectId(output_orm.id))

        return output_ids

    def add(self, outputs: List[KVStore]) -> List[ObjectId]:
        """
        Inserts outputs into the database

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        Parameters
        ----------
        outputs
            A list/sequence of KVStore objects add.

        Returns
        -------
        :
            A list of all the newly-inserted IDs
        """

        # TODO - remove me after switching to only using add_internal!

        with self._core_socket.session_scope() as session:
            return self.add_internal(session, outputs)

    def get(self, id: Sequence[ObjectId], missing_ok: bool = False) -> List[Optional[OutputDict]]:
        """
        Obtain outputs from the database

        The returned dictionaries will be in the same order as the IDs specified with the `id` parameter.

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of KVStore will be None.

        Parameters
        ----------
        id
            A list or other sequence of ids to query
        missing_ok
           If set to True, then missing ids will be tolerated, and the returned list of
           KVStore will contain None for the corresponding IDs that were not found.
           Otherwise, an exception will be raised.

        Returns
        -------
        :
            A list of KVStore  in the same order as the given ids. If missing_ok is True, then this list
            will contain None where the id was missing.
        """

        # Check that we are not requesting more outputs than the limit
        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} outputs is over the limit of {self._limit}")

        # TODO - int id
        id = [int(x) for x in id]

        unique_ids = list(set(id))

        query_cols_names, query_cols = get_query_proj_columns(KVStoreORM)

        with self._core_socket.session_scope(True) as session:
            # No need to split out id like in other subsockets, we always include all columns
            results = session.query(*query_cols).filter(KVStoreORM.id.in_(unique_ids)).all()
            result_dicts = [dict(zip(query_cols_names, x)) for x in results]

        # Fixes some old fields
        result_dicts = list(map(self.cleanup_outputstore_dict, result_dicts))

        # TODO - int id
        # We previously changed int to ObjectId in cleanup_outputstore_dict
        result_map = {int(x["id"]): x for x in result_dicts}

        # Put into the original order
        ret = [result_map.get(x, None) for x in id]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested KVStore records")

        return ret
