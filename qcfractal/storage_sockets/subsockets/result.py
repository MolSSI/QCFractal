from __future__ import annotations

import logging
from sqlalchemy import and_
from qcfractal.storage_sockets.models import BaseResultORM, ResultORM, TaskQueueORM
from qcfractal.interface.models import ResultRecord, TaskStatusEnum, ObjectId, AtomicResult
from qcfractal.interface.models.query_meta import InsertMetadata
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, get_count_fast, calculate_limit
from qcfractal.storage_sockets.sqlalchemy_common import insert_general

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Union, Optional, Tuple


class ResultSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.result

    def get(
        self,
        id: Union[str, List] = None,
        program: str = None,
        method: str = None,
        basis: str = None,
        molecule: str = None,
        driver: str = None,
        keywords: str = None,
        task_id: Union[str, List] = None,
        manager_id: Union[str, List] = None,
        status: str = "COMPLETE",
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        limit: int = None,
        skip: int = 0,
        return_json=True,
        with_ids=True,
    ):
        """

        Parameters
        ----------
        id : str or List[str]
        program : str
        method : str
        basis : str
        molecule : str
            MoleculeORM id in the DB
        driver : str
        keywords : str
            The id of the option in the DB
        task_id: str or List[str]
            id or a list of ids of tasks
        manager_id: str or List[str]
            id or a list of ids of queue_mangers
        status : bool, optional
            The status of the result: 'COMPLETE', 'INCOMPLETE', or 'ERROR'
            Default is 'COMPLETE'
        include : Optional[List[str]], optional
            The fields to return, default to return all
        exclude : Optional[List[str]], optional
            he fields to not return, default to return all
        limit : Optional[int], optional
            maximum number of results to return
            if 'limit' is greater than the global setting self._limit,
            the self._limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, optional
            skip the first 'skip' results. Used to paginate
            Default is 0
        return_json : bool, optional
            Return the results as a list of json inseated of objects
            default is True
        with_ids : bool, optional
            Include the ids in the returned objects/dicts
            default is True

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta
            Data is the objects found
        """

        if task_id:
            return self._get_by_task_id(task_id)

        limit = calculate_limit(self._limit, limit)
        meta = get_metadata_template()

        # Ignore status if Id is present
        if id is not None:
            status = None

        query = format_query(
            ResultORM,
            id=id,
            program=program,
            method=method,
            basis=basis,
            molecule=molecule,
            driver=driver,
            keywords=keywords,
            manager_id=manager_id,
            status=status,
        )

        data, meta["n_found"] = self._core_socket.get_query_projection(
            ResultORM, query, include=include, exclude=exclude, limit=limit, skip=skip
        )
        meta["success"] = True

        return {"data": data, "meta": meta}

    def _get_by_task_id(self, task_id: Union[str, List] = None, return_json=True):
        """
        Parameters
        ----------
        task_id : str or List[str]
        return_json : bool, optional
            Return the results as a list of json inseated of objects
            Default is True
        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta
            Data is the objects found
        """

        print(task_id)
        meta = get_metadata_template()

        data = []
        task_id_list = [task_id] if isinstance(task_id, (int, str)) else task_id
        # try:
        with self._core_socket.session_scope() as session:
            data = (
                session.query(BaseResultORM)
                .filter(BaseResultORM.id == TaskQueueORM.base_result_id)
                .filter(TaskQueueORM.id.in_(task_id_list))
            )
            meta["n_found"] = get_count_fast(data)
            data = [d.to_dict() for d in data.all()]
            meta["success"] = True
            # except Exception as err:
            #     meta['error_description'] = str(err)

        return {"data": data, "meta": meta}

    def delete(self, ids: List[str]):
        """
        Removes results from the database using their ids
        (Should be cautious! other tables maybe referencing results)

        Parameters
        ----------
        ids : List[str]
            The Ids of the results to be deleted

        Returns
        -------
        int
            number of results deleted
        """

        with self._core_socket.session_scope() as session:
            results = session.query(ResultORM).filter(ResultORM.id.in_(ids)).all()
            # delete through session to delete correctly from base_result
            for result in results:
                session.delete(result)
            session.commit()
            count = len(results)

        return count
