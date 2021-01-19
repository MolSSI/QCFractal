from __future__ import annotations

from qcfractal.storage_sockets.models import KeywordsORM
from qcfractal.interface.models import KeywordSet
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List


class KeywordsSocket:
    def __init__(self, core_socket):
        self._core_socket = core_socket

    def add(self, keyword_sets: List[KeywordSet]):
        """Add one KeywordSet uniquly identified by 'program' and the 'name'.

        Parameters
        ----------
        keywords_set : List[KeywordSet]
            A list of KeywordSets to be inserted.

        Returns
        -------
        Dict[str, Any]
            (see add_metadata_template())
            The 'data' part is a list of ids of the inserted options
            data['duplicates'] has the duplicate entries

        Notes
        ------
            Duplicates are not considered errors.

        """

        meta = add_metadata_template()

        keywords = []
        with self._core_socket.session_scope() as session:
            for kw in keyword_sets:

                kw_dict = kw.dict(exclude={"id"})

                # search by index keywords not by all keys, much faster
                found = session.query(KeywordsORM).filter_by(hash_index=kw_dict["hash_index"]).first()
                if not found:
                    doc = KeywordsORM(**kw_dict)
                    session.add(doc)
                    session.commit()
                    keywords.append(str(doc.id))
                    meta["n_inserted"] += 1
                else:
                    meta["duplicates"].append(str(found.id))  # TODO
                    keywords.append(str(found.id))
                meta["success"] = True

        ret = {"data": keywords, "meta": meta}

        return ret

    def get(
        self,
        id: Union[str, list] = None,
        hash_index: Union[str, list] = None,
        limit: int = None,
        skip: int = 0,
        return_json: bool = False,
        with_ids: bool = True,
    ) -> List[KeywordSet]:
        """Search for one (unique) option based on the 'program'
        and the 'name'. No overwrite allowed.

        Parameters
        ----------
        id : List[str] or str
            Ids of the keywords
        hash_index : List[str] or str
            hash index of keywords
        limit : Optional[int], optional
            Maximum number of results to return.
            If this number is greater than the SQLAlchemySocket.max_limit then
            the max_limit will be returned instead.
            Default is to return the socket's max_limit (when limit=None or 0)
        skip : int, optional
        return_json : bool, optional
            Return the results as a json object
            Default is True
        with_ids : bool, optional
            Include the DB ids in the returned object (names 'id')
            Default is True


        Returns
        -------
            A dict with keys: 'data' and 'meta'
            (see get_metadata_template())
            The 'data' part is an object of the result or None if not found
        """

        meta = get_metadata_template()
        query = format_query(KeywordsORM, id=id, hash_index=hash_index)

        rdata, meta["n_found"] = self._core_socket.get_query_projection(
            KeywordsORM, query, limit=limit, skip=skip, exclude=[None if with_ids else "id"]
        )

        meta["success"] = True

        # meta['error_description'] = str(err)

        if not return_json:
            data = [KeywordSet(**d) for d in rdata]
        else:
            data = rdata

        return {"data": data, "meta": meta}

    def get_add_mixed(self, data):
        """
        Get or add the given options (if they don't exit).
        KeywordsORM are given in a mixed format, either as a dict of mol data
        or as existing mol id

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ids = []
        for idx, kw in enumerate(data):
            if isinstance(kw, (int, str)):
                ids.append(kw)

            elif isinstance(kw, KeywordSet):
                new_id = self.add([kw])["data"][0]
                ids.append(new_id)
            else:
                meta["errors"].append((idx, "Data type not understood"))
                ids.append(None)

        missing = []
        ret = []
        for idx, id in enumerate(ids):
            if id is None:
                ret.append(None)
                missing.append(idx)
                continue

            tmp = self.get(id=id)["data"]
            if tmp:
                ret.append(tmp[0])
            else:
                ret.append(None)

        meta["success"] = True
        meta["n_found"] = len(ret) - len(missing)
        meta["missing"] = missing

        return {"meta": meta, "data": ret}

    def delete(self, id: str) -> int:
        """
        Removes a option set from the database based on its id.

        Parameters
        ----------
        id : str
            id of the keyword

        Returns
        -------
        int
           number of deleted documents
        """

        with self._core_socket.session_scope() as session:
            count = session.query(KeywordsORM).filter_by(id=id).delete(synchronize_session=False)

        return count
