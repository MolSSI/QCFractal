from __future__ import annotations

import logging
from qcfractal.components.keywords.db_models import KeywordsORM
from qcfractal.interface.models import KeywordSet, InsertMetadata, DeleteMetadata, ObjectId
from qcfractal.db_socket.helpers import (
    insert_general,
    delete_general,
    insert_mixed_general,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Sequence, Union, Dict, Any, Tuple, Optional

    KeywordDict = Dict[str, Any]


class KeywordsSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.keyword

    @staticmethod
    def keywords_to_orm(keywords: Union[KeywordDict, KeywordSet]) -> KeywordsORM:
        if isinstance(keywords, KeywordSet):
            kw_dict = keywords.dict(exclude={"id"})
        else:
            kw_dict = KeywordSet(values=keywords).dict(exclude={"id"})

        return KeywordsORM(**kw_dict)  # type: ignore

    def add(
        self, keywords: Sequence[KeywordSet], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Add keywords to the database

        This checks if the keywords already exists in the database via its hash. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Changes are not committed to to the database, but they are flushed.

        Parameters
        ----------
        keywords
            Keyword data to add to the session
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of keyword ids. The ids will be in the
            order of the input keywords.
        """

        ###############################################################################
        # Exceptions in this function would usually be a programmer error, as any
        # valid KeywordSet object should be insertable into the database
        ###############################################################################

        kw_orm = [self.keywords_to_orm(x) for x in keywords]

        with self._core_socket.optional_session(session) as session:
            meta, added_ids = insert_general(session, kw_orm, (KeywordsORM.hash_index,), (KeywordsORM.id,))

        # insert_general should always succeed or raise exception
        assert meta.success

        # Added ids are a list of tuple, with each tuple only having one value
        return meta, [ObjectId(x[0]) for x in added_ids]

    def get(
        self, id: Sequence[ObjectId], missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> List[Optional[KeywordDict]]:
        """
        Obtain keywords with specified IDs

        The returned keyword information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of keywords will be None.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
        id
            A list or other sequence of keyword IDs
        missing_ok
           If set to True, then missing keywords will be tolerated, and the returned list of
           keywords will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Keyword information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the keywords were missing
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} molecules is over the limit of {self._limit}")

        # TODO - int id
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        with self._core_socket.optional_session(session, True) as session:
            results = session.query(KeywordsORM).filter(KeywordsORM.id.in_(unique_ids)).yield_per(500)
            result_map = {r.id: r.dict() for r in results}

        # Put into the requested order
        ret = [result_map.get(x, None) for x in int_id]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested keywords records")

        return ret

    def add_mixed(
        self, keyword_data: Sequence[Union[ObjectId, KeywordSet, KeywordDict]], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[Optional[ObjectId]]]:
        """
        Add a mixed format keywords specification to the database.

        This function can take KeywordSet objects, keyword ids, or dictionaries. If a keyword id is given
        in the list, then it is checked to make sure it exists. If it does not exist, then it will be
        marked as an error in the returned metadata and the corresponding entry in the returned
        list of IDs will be None.

        If a KeywordSet or dictionary is given, it will be added to the database if it does not already exist
        in the database (based on the hash) and the existing ID will be returned. Otherwise, the new
        ID will be returned.

        Parameters
        ----------
        keyword_data
            Keyword data to add. Can be a mix of IDs and Keyword objects
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        # TODO - INT ID
        keyword_data_2 = [int(x) if isinstance(x, (int, str, ObjectId)) else x for x in keyword_data]

        keyword_orm: List[Union[int, KeywordsORM]] = [
            x if isinstance(x, int) else self.keywords_to_orm(x) for x in keyword_data_2
        ]

        with self._core_socket.optional_session(session) as session:
            meta, all_ids = insert_mixed_general(
                session, KeywordsORM, keyword_orm, KeywordsORM.id, (KeywordsORM.hash_index,), (KeywordsORM.id,)
            )

        # all_ids is a list of Tuples
        # TODO - INT ID
        return meta, [ObjectId(x[0]) if x is not None else None for x in all_ids]

    def delete(self, id: List[ObjectId], *, session: Optional[Session] = None) -> DeleteMetadata:
        """
        Removes keywords from the database based on id

        Parameters
        ----------
        id
            IDs of the keywords to remove
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Information about what was deleted and any errors that occurred
        """

        # TODO - INT ID
        id_lst = [(int(x),) for x in id]

        with self._core_socket.optional_session(session) as session:
            return delete_general(session, KeywordsORM, (KeywordsORM.id,), id_lst)
