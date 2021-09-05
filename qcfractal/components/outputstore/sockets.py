from __future__ import annotations

import logging
from qcfractal.components.outputstore.db_models import KVStoreORM
from qcfractal.interface.models import KVStore, ObjectId, CompressionEnum

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Optional, Sequence, Dict, Any, Union

    OutputDict = Dict[str, Any]


class OutputStoreSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.output_store

    @staticmethod
    def output_to_orm(output: KVStore) -> KVStoreORM:
        return KVStoreORM(**output.dict())  # type: ignore

    def add(self, outputs: Sequence[Union[KVStore, str, Dict]], *, session: Optional[Session] = None) -> List[ObjectId]:
        """
        Inserts output store data into the database

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        Changes are not committed to to the database, but they are flushed.

        Parameters
        ----------
        outputs
            A sequence of output store data to add. This can be a string or dictionary, which will be converted
            to a KVStore object, or a KVStore object itself
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            A list of all newly-inserted IDs
        """

        # Since outputs can be relatively large, we don't explicitly create
        # a list of ORMs (one for each output). We only go one at a time

        output_ids = []

        with self._core_socket.optional_session(session) as session:
            for output in outputs:
                if isinstance(output, KVStore):
                    kv_obj = output
                else:
                    kv_obj = KVStore.compress(output, CompressionEnum.lzma, 1)

                output_orm = self.output_to_orm(kv_obj)
                session.add(output_orm)
                session.flush()
                output_ids.append(ObjectId(output_orm.id))

        return output_ids

    def get(
        self, id: Sequence[ObjectId], missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> List[Optional[OutputDict]]:
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
        session
            An existing SQLAlchemy session to use. If None, one will be created

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
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        with self._core_socket.optional_session(session, True) as session:
            results = session.query(KVStoreORM).filter(KVStoreORM.id.in_(unique_ids)).yield_per(50)
            result_map = {r.id: r.dict() for r in results}

        # Put into the requested order
        ret = [result_map.get(x, None) for x in int_id]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested KVStore records")

        return ret

    def delete(self, id: Sequence[int], *, session: Optional[Session] = None) -> int:
        """
        Removes outputs objects from the database

        If the output is still being referred to, then an exception is raised. Since this is for internal
        use, that would be a bug.

        Parameters
        ----------
        id
            IDs of the output objects to remove
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            The number of deleted outputs
        """

        with self._core_socket.optional_session(session) as session:
            return session.query(KVStoreORM).filter(KVStoreORM.id.in_(id)).delete()

    def append(self, id: Optional[ObjectId], to_append: str, *, session: Optional[Session] = None) -> ObjectId:
        """
        Appends data to an output

        If the id is None, then one will be created.

        If the id does not exist, an exception will be raised

        Parameters
        ----------
        id
            The ID of the output to append data to
        to_append
            Data to append to the output
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.


        Returns
        -------
        :
            The ID of the output store. This is the same as the input if specified. If the input is None, then
            this will represent the ID of the new output object
        """

        with self._core_socket.optional_session(session) as session:
            if id is None:
                kv = KVStore.compress(to_append)
                return self.add([kv], session=session)[0]
            else:
                output = session.query(KVStoreORM).filter(KVStoreORM.id == id).one_or_none()

                if output is None:
                    raise RuntimeError(f"Cannot append to output {id} - does not exist!")

                kv = KVStore(**output.dict())
                s = kv.get_string() + to_append
                new_orm = self.output_to_orm(KVStore.compress(s, output.compression, output.compression_level))
                output.data = new_orm.data
                output.value = new_orm.value
                output.compression = new_orm.compression
                output.compression_level = new_orm.compression_level
                return ObjectId(output.id)
