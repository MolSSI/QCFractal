from __future__ import annotations

import logging
from qcfractal.storage_sockets.models import KeywordsORM
from qcfractal.interface.models import KeywordSet, InsertMetadata, DeleteMetadata, ObjectId
from qcfractal.storage_sockets.sqlalchemy_common import (
    get_query_proj_columns,
    insert_general,
    delete_general,
    insert_mixed_general,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
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

    @staticmethod
    def cleanup_keywords_dict(kw_dict: KeywordDict) -> KeywordDict:
        """
        Perform any cleanup of a keywords dictionary
        """

        # TODO - int id
        if "id" in kw_dict:
            kw_dict["id"] = ObjectId(kw_dict["id"])

        return kw_dict

    def add_internal(self, session: Session, keywords: Sequence[KeywordSet]) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Add keywords to a session

        This checks if the keywords already exists in the database via its hash. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Changes are not committed to to the database, but they are flushed.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to add the data to
        keywords
            Keyword data to add to the session

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

        meta, added_ids = insert_general(session, kw_orm, (KeywordsORM.hash_index,), (KeywordsORM.id,))

        # insert_general should always succeed or raise exception
        assert meta.success

        # Added ids are a list of tuple, with each tuple only having one value
        return meta, [ObjectId(x[0]) for x in added_ids]

    def add(self, keywords: Sequence[KeywordSet]) -> Tuple[InsertMetadata, List[ObjectId]]:
        """Adds keywords to the database

        This function returns metadata about the insertion, and a list of IDs of the keywords in the database.
        This list will be in the same order as the `keywords` parameter.

        For each keyword given, the database will be checked to see if it already exists in the database.
        If it does, that ID will be returned. Otherwise, the keywords will be added to the database and the
        new ID returned.

        On any error, this function will raise an exception.

        Parameters
        ----------
        keywords
            A list of keyword sets to be inserted.

        Returns
        -------
        :
            Metadata about the insertion, and a list of keywordids. The ids will be in the
            order of the input keywords.
        """

        with self._core_socket.session_scope() as session:
            return self.add_internal(session, keywords)

    def get(
        self,
        id: Sequence[ObjectId],
        missing_ok: bool = False,
    ) -> List[Optional[KeywordDict]]:
        """
        Obtain keywords with specified IDs

        The returned keyword information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of keywords will be None.

        Parameters
        ----------
        id
            A list or other sequence of keyword IDs
        missing_ok
           If set to True, then missing keywords will be tolerated, and the returned list of
           keywords will contain None for the corresponding IDs that were not found.

        Returns
        -------
        :
            Keyword information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the keywords were missing
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} keywords is over the limit of {self._limit}")

        # TODO - int id
        id = [int(x) for x in id]

        unique_ids = list(set(id))

        query_cols_names, query_cols = get_query_proj_columns(KeywordsORM)

        with self._core_socket.session_scope(True) as session:
            results = session.query(*query_cols).filter(KeywordsORM.id.in_(unique_ids)).all()
            result_dicts = [dict(zip(query_cols_names, x)) for x in results]

        result_dicts = list(map(self.cleanup_keywords_dict, result_dicts))

        # TODO - int id
        # We previously changed int to ObjectId in cleanup_keywords_dict
        result_map = {int(x["id"]): x for x in result_dicts}

        # Put into the original order
        ret = [result_map.get(x, None) for x in id]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested keyword records")

        return ret

    def add_mixed(
        self, keyword_data: Sequence[Union[ObjectId, KeywordSet, KeywordDict]]
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
        """

        # TODO - INT ID
        keyword_data_2 = [int(x) if isinstance(x, (int, str, ObjectId)) else x for x in keyword_data]

        keyword_orm: List[Union[int, KeywordsORM]] = [
            x if isinstance(x, int) else self.keywords_to_orm(x) for x in keyword_data_2
        ]

        with self._core_socket.session_scope() as session:
            meta, all_ids = insert_mixed_general(
                session, KeywordsORM, keyword_orm, KeywordsORM.id, (KeywordsORM.hash_index,), (KeywordsORM.id,)
            )

        # all_ids is a list of Tuples
        # TODO - INT ID
        return meta, [ObjectId(x[0]) if x is not None else None for x in all_ids]

    def delete(self, id: List[ObjectId]) -> DeleteMetadata:
        """
        Removes keywords from the database based on id

        Parameters
        ----------
        id
            IDs of the keywords to remove

        Returns
        -------
        :
            Information about what was deleted and any errors that occurred
        """

        # TODO - INT ID
        id_lst = [(int(x),) for x in id]

        with self._core_socket.session_scope() as session:
            # session will commit on exiting from the context
            return delete_general(session, KeywordsORM, (KeywordsORM.id,), id_lst)
