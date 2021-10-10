from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcfractal.components.keywords.db_models import KeywordsORM
from qcfractal.db_socket.helpers import (
    get_general,
    insert_general,
    delete_general,
    insert_mixed_general,
)
from qcfractal.exceptions import LimitExceededError
from qcfractal.interface.models import KeywordSet, InsertMetadata, DeleteMetadata

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Sequence, Union, Dict, Any, Tuple, Optional

    KeywordDict = Dict[str, Any]


class KeywordsSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.keyword

    @staticmethod
    def keywords_to_orm(keywords: Union[KeywordDict, KeywordSet]) -> KeywordsORM:
        if isinstance(keywords, KeywordSet):
            kw_dict = keywords.dict(exclude={"id"})
        else:
            kw_dict = KeywordSet(values=keywords).dict(exclude={"id"})

        return KeywordsORM(**kw_dict)  # type: ignore

    def add(
        self, keywords: Sequence[KeywordSet], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Add keywords to the database

        This checks if the keywords already exists in the database via its hash. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to to the database, but the session is flushed.

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

        with self.root_socket.optional_session(session) as session:
            meta, added_ids = insert_general(session, kw_orm, (KeywordsORM.hash_index,), (KeywordsORM.id,))

        # added_ids is a list of tuple, with each tuple only having one value. Flatten that out
        return meta, [x[0] for x in added_ids]

    def get(
        self, id: Sequence[int], missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> List[Optional[KeywordDict]]:
        """
        Obtain keywords with specified IDs

        The returned keyword information will be in the same order as the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception.
        Otherwise, the corresponding entry in the returned list of keywords will be None.

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
            raise LimitExceededError(f"Request for {len(id)} keywords is over the limit of {self._limit}")

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, KeywordsORM, KeywordsORM.id, id, None, None, None, missing_ok)

    def add_mixed(
        self, keyword_data: Sequence[Union[int, KeywordSet, KeywordDict]], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
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

        keyword_orm: List[Union[int, KeywordsORM]] = [
            x if isinstance(x, int) else self.keywords_to_orm(x) for x in keyword_data
        ]

        with self.root_socket.optional_session(session) as session:
            meta, all_ids = insert_mixed_general(
                session, KeywordsORM, keyword_orm, KeywordsORM.id, (KeywordsORM.hash_index,), (KeywordsORM.id,)
            )

        # added_ids is a list of tuple, with each tuple only having one value. Flatten that out
        return meta, [x[0] if x is not None else None for x in all_ids]

    def delete(self, id: Sequence[int], *, session: Optional[Session] = None) -> DeleteMetadata:
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

        id_lst = [(x,) for x in id]

        with self.root_socket.optional_session(session) as session:
            return delete_general(session, KeywordsORM, (KeywordsORM.id,), id_lst)
