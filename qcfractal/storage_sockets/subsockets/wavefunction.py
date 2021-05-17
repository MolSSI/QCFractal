from __future__ import annotations

import logging
from sqlalchemy.orm import load_only
from qcfractal.interface.models import WavefunctionProperties, ObjectId
from qcfractal.storage_sockets.models import WavefunctionStoreORM
from qcfractal.storage_sockets.sqlalchemy_common import get_query_proj_columns

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import Dict, Iterable, List, Optional, Sequence, Any

    WavefunctionDict = Dict[str, Any]


class WavefunctionSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.wavefunction

    @staticmethod
    def wavefunction_to_orm(wfn: WavefunctionProperties) -> WavefunctionStoreORM:
        return WavefunctionStoreORM(**wfn.dict())  # type: ignore

    def add(
        self, wavefunctions: Sequence[WavefunctionProperties], *, session: Optional[Session] = None
    ) -> List[ObjectId]:
        """
        Inserts wavefunction data into an existing session

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        Changes are not committed to to the database, but they are flushed.

        Parameters
        ----------
        wavefunctions
            A sequence of wavefunction data to add.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            A list of all newly-inserted IDs
        """

        # Since wavefunctions can be relatively large, we don't explicitly create
        # a list of ORMs (one for each wfn). We only go one at a time

        wfn_ids = []

        with self._core_socket.optional_session(session) as session:
            for wfn in wavefunctions:
                wfn_orm = self.wavefunction_to_orm(wfn)
                session.add(wfn_orm)
                session.flush()
                wfn_ids.append(ObjectId(wfn_orm.id))

        return wfn_ids

    def get(
        self,
        id: Sequence[ObjectId],
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[WavefunctionDict]]:
        """
        Obtain wavefunction data from the database.

        The order of the returned wavefunction data is in the same order as the id parameter.

        Parameters
        ----------
        id
            A list of wavefunction ids to query
        include
            The fields to return. If None, return all fields
        exclude
            The fields to not return. If None, return all fields
        missing_ok
            If set to True, missing ids will be tolerated and the returned list
            will contain None for missing ids. Otherwise, an exception will be raised.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Dictionary containing the wavefunction data
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} wavefunctions is over the limit of {self._limit}")

        # TODO - int id
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        load_cols, _ = get_query_proj_columns(WavefunctionStoreORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            results = (
                session.query(WavefunctionStoreORM)
                .filter(WavefunctionStoreORM.id.in_(unique_ids))
                .options(load_only(*load_cols))
                .yield_per(50)
            )

            result_map = {r.id: r.dict() for r in results}

        # Put things back into the requested order
        ret = [result_map.get(x, None) for x in int_id]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested wavefunction records")

        return ret

    def delete(self, id: Sequence[int], *, session: Optional[Session] = None) -> int:
        """
        Removes wavefunction objects from the database

        If the wavefunction is still being referred to, then an exception is raised. Since this is for internal
        use, that would be a bug.

        Parameters
        ----------
        id
            IDs of the wavefunction objects to remove
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            The number of deleted wavefunctions
        """

        with self._core_socket.optional_session(session) as session:
            return session.query(WavefunctionStoreORM).filter(WavefunctionStoreORM.id.in_(id)).delete()
