from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.db_socket.helpers import get_general
from qcfractal.exceptions import LimitExceededError
from qcfractal.portal.components.wavefunctions.models import WavefunctionProperties

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Dict, Iterable, List, Optional, Sequence, Any

    WavefunctionDict = Dict[str, Any]


class WavefunctionSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.wavefunction

    @staticmethod
    def wavefunction_to_orm(wfn: WavefunctionProperties) -> WavefunctionStoreORM:
        return WavefunctionStoreORM(**wfn.dict())  # type: ignore

    def add(self, wavefunctions: Sequence[WavefunctionProperties], *, session: Optional[Session] = None) -> List[int]:
        """
        Inserts wavefunction data into the database

        Since all entries are always inserted, we don't need to return any
        metadata like other types of insertions

        If session is specified, changes are not committed to to the database, but the session is flushed.

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

        # Wavefunctions are always added, so we don't use the general function

        # Since wavefunctions can be relatively large, we don't explicitly create
        # a list of ORMs (one for each wfn). We only go one at a time

        wfn_ids = []

        with self.root_socket.optional_session(session) as session:
            for wfn in wavefunctions:
                wfn_orm = self.wavefunction_to_orm(wfn)
                session.add(wfn_orm)
                session.flush()
                wfn_ids.append(wfn_orm.id)

        return wfn_ids

    def get(
        self,
        id: Sequence[int],
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[WavefunctionDict]]:
        """
        Obtain wavefunction data from the database.

        The order of the returned wavefunction data is in the same order as the id parameter.

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of OutputStore will be None.

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
            Dictionary containing the wavefunction data, in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the id was missing.
        """

        if len(id) > self._limit:
            raise LimitExceededError(f"Request for {len(id)} wavefunctions is over the limit of {self._limit}")

        with self.root_socket.optional_session(session, True) as session:
            res = get_general(
                session, WavefunctionStoreORM, WavefunctionStoreORM.id, id, include, exclude, None, missing_ok
            )

        # Remove the id field. It doesn't exist in the WavefunctionProperties
        # TODO - should it? Or maybe we should derive our own class and add it
        for r in res:
            if r is not None:
                r.pop("id", None)

        return res
