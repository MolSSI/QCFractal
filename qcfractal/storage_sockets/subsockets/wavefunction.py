from __future__ import annotations

import logging
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

    @staticmethod
    def cleanup_wavefunction_dict(wfn_dict: WavefunctionDict) -> WavefunctionDict:
        """
        Removes any fields of a molecule dictionary that are None
        """

        # TODO - int id
        if "id" in wfn_dict:
            wfn_dict["id"] = ObjectId(wfn_dict["id"])

        return wfn_dict

    def add_internal(self, session: Session, wavefunctions: Sequence[WavefunctionProperties]) -> List[ObjectId]:
        """
        Inserts wavefunction data into an existing session

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        Changes are not committed to to the database, but they are flushed.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to add the data to
        wavefunctions
            A sequence of wavefunction data to add.

        Returns
        -------
        :
            A list of all newly-inserted IDs
        """

        # Since wavefunctions can be relatively large, we don't explicitly create
        # a list of ORMs (one for each wfn). We only go one at a time

        wfn_ids = []
        for wfn in wavefunctions:
            wfn_orm = self.wavefunction_to_orm(wfn)
            session.add(wfn_orm)
            session.flush()
            wfn_ids.append(ObjectId(wfn_orm.id))

        return wfn_ids

    def add(self, wavefunctions: Sequence[WavefunctionProperties]) -> List[ObjectId]:
        """
        Inserts wavefunction data into the database

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        Parameters
        ----------
        wavefunctions
            A list/sequence of wavefunction data blobs to add.

        Returns
        -------
        :
            A list of all newly-inserted IDs
        """

        # TODO - remove me after switching to only using add_internal!

        with self._core_socket.session_scope() as session:
            return self.add_internal(session, wavefunctions)

    def get(
        self,
        id: Sequence[ObjectId],
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        missing_ok: bool = False,
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

        Returns
        -------
        :
            Dictionary containing the wavefunction data
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} wavefunctions is over the limit of {self._limit}")

        # TODO - int id
        id = [int(x) for x in id]

        unique_ids = list(set(id))

        query_cols_names, query_cols = get_query_proj_columns(WavefunctionStoreORM, include, exclude)

        with self._core_socket.session_scope(True) as session:
            results = (
                session.query(WavefunctionStoreORM.id, *query_cols)
                .filter(WavefunctionStoreORM.id.in_(unique_ids))
                .all()
            )

            # x[0] is the id, the rest are the columns we want to return
            # we zip it with the column names to form our dictionary
            result_map = {x[0]: dict(zip(query_cols_names, x[1:])) for x in results}

        result_map = {k: self.cleanup_wavefunction_dict(v) for k, v in result_map.items()}

        # Put things back into the same order
        ret = [result_map.get(x, None) for x in id]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested Wavefunction records")

        return ret

    def delete_internal(self, session: Session, id: Sequence[int]) -> int:
        """
        Removes wavefunction objects from the database

        If the wavefunction is still being referred to, then an exception is raised. Since this is for internal
        use, that would be a bug.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to add the data to
        id
            IDs of the wavefunction objects to remove

        Returns
        -------
        :
            The number of deleted wavefunctions
        """

        n = session.query(WavefunctionStoreORM).filter(WavefunctionStoreORM.id.in_(id)).delete()
        return n
