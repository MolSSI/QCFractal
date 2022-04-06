from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcfractal.db_socket.helpers import (
    get_general,
    insert_general,
)
from qcportal.metadata_models import InsertMetadata
from .db_models import KeywordsORM
from .models import KeywordSet

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Sequence, Union, Dict, Any, Tuple, Optional

    KeywordDict = Dict[str, Any]


class KeywordsSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def keywords_to_orm(keywords: Union[KeywordDict, KeywordSet]) -> KeywordsORM:
        if isinstance(keywords, KeywordSet):
            kw_dict = keywords.dict(exclude={"id"})
        else:
            kw_dict = KeywordSet(values=keywords).dict(exclude={"id"})  # type: ignore

        return KeywordsORM(**kw_dict)

    def add(
        self, keywords: Sequence[Dict[str, Any]], *, session: Optional[Session] = None
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

        keywords_obj = [KeywordSet(values=x) for x in keywords]

        # Make sure the hashes are correct
        kw_orm = []

        for k in keywords:
            k2 = KeywordSet(values=k, lowercase=True, exact_floats=True)
            k2.build_index()
            kw_orm.append(self.keywords_to_orm(k2))

        with self.root_socket.optional_session(session) as session:
            meta, added_ids = insert_general(session, kw_orm, (KeywordsORM.hash_index,), (KeywordsORM.id,))

        # added_ids is a list of tuple, with each tuple only having one value. Flatten that out
        return meta, [x[0] for x in added_ids]
