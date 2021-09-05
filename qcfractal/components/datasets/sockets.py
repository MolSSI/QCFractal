from __future__ import annotations

import logging
from qcfractal.components.datasets.reaction.db_models import ReactionDatasetORM
from qcfractal.components.datasets.singlepoint.db_models import DatasetORM
from qcfractal.components.datasets.db_models import CollectionORM
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Any, Optional


def get_collection_class(collection_type):

    collection_map = {"dataset": DatasetORM, "reactiondataset": ReactionDatasetORM}

    collection_class = CollectionORM

    if collection_type in collection_map:
        collection_class = collection_map[collection_type]

    return collection_class


class DatasetSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.collection

    def add(self, data: Dict[str, Any], overwrite: bool = False):
        """Add (or update) a collection to the database.

        Parameters
        ----------
        data : Dict[str, Any]
            should inlcude at least(keys):
            collection : str (immutable)
            name : str (immutable)

        overwrite : bool
            Update existing collection

        Returns
        -------
        Dict[str, Any]
        A dict with keys: 'data' and 'meta'
            (see add_metadata_template())
            The 'data' part is the id of the inserted document or none

        Notes
        -----
        ** Change: The data doesn't have to include the ID, the document
        is identified by the (collection, name) pairs.
        ** Change: New fields will be added to the collection, but existing won't
            be removed.
        """

        meta = add_metadata_template()
        col_id = None
        # try:

        # if ("id" in data) and (data["id"] == "local"):
        #     data.pop("id", None)
        if "id" in data:  # remove the ID in any case
            data.pop("id", None)
        lname = data.get("name").lower()
        collection = data.pop("collection").lower()

        # Get collection class if special type is implemented
        collection_class = get_collection_class(collection)

        update_fields = {}
        for field in collection_class._all_col_names():
            if field in data:
                update_fields[field] = data.pop(field)

        update_fields["extra"] = data  # todo: check for sql injection

        with self._core_socket.session_scope() as session:

            try:
                if overwrite:
                    col = session.query(collection_class).filter_by(collection=collection, lname=lname).first()
                    for key, value in update_fields.items():
                        setattr(col, key, value)
                else:
                    col = collection_class(collection=collection, lname=lname, **update_fields)

                session.add(col)
                session.commit()
                col.update_relations(**update_fields)
                session.commit()

                col_id = str(col.id)
                meta["success"] = True
                meta["n_inserted"] = 1

            except Exception as err:
                session.rollback()
                meta["error_description"] = str(err)

        ret = {"data": col_id, "meta": meta}
        return ret

    def get(
        self,
        collection: Optional[str] = None,
        name: Optional[str] = None,
        col_id: Optional[int] = None,
        limit: Optional[int] = None,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        skip: int = 0,
    ) -> Dict[str, Any]:
        """Get collection by collection and/or name

        Parameters
        ----------
        collection: Optional[str], optional
            Type of the collection, e.g. ReactionDataset
        name: Optional[str], optional
            Name of the collection, e.g. S22
        col_id: Optional[int], optional
            Database id of the collection
        limit: Optional[int], optional
            Maximum number of results to return
        include: Optional[List[str]], optional
            Columns to return
        exclude: Optional[List[str]], optional
            Return all but these columns
        skip: int, optional
            Skip the first `skip` results

        Returns
        -------
        Dict[str, Any]
            A dict with keys: 'data' and 'meta'
            The data is a list of the collections found
        """

        limit = calculate_limit(self._limit, limit)
        meta = get_metadata_template()
        if name:
            name = name.lower()
        if collection:
            collection = collection.lower()

        collection_class = get_collection_class(collection)
        query = format_query(collection_class, lname=name, collection=collection, id=col_id)

        # try:
        rdata, meta["n_found"] = self._core_socket.get_query_projection(
            collection_class, query, include=include, exclude=exclude, limit=limit, skip=skip
        )

        meta["success"] = True
        # except Exception as err:
        #     meta['error_description'] = str(err)

        return {"data": rdata, "meta": meta}

    def delete(
        self, collection: Optional[str] = None, name: Optional[str] = None, col_id: Optional[int] = None
    ) -> bool:
        """
        Remove a collection from the database from its keys.

        Parameters
        ----------
        collection: Optional[str], optional
            CollectionORM type
        name : Optional[str], optional
            CollectionORM name
        col_id: Optional[int], optional
            Database id of the collection
        Returns
        -------
        int
            Number of documents deleted
        """

        # Assuming here that we don't want to allow deletion of all collections, all datasets, etc.
        if not (col_id is not None or (collection is not None and name is not None)):
            raise ValueError(
                "Either col_id ({col_id}) must be specified, or collection ({collection}) and name ({name}) must be specified."
            )

        filter_spec = {}
        if collection is not None:
            filter_spec["collection"] = collection.lower()
        if name is not None:
            filter_spec["lname"] = name.lower()
        if col_id is not None:
            filter_spec["id"] = col_id

        with self._core_socket.session_scope() as session:
            count = session.query(CollectionORM).filter_by(**filter_spec).delete(synchronize_session=False)
        return count
