from __future__ import annotations

from sqlalchemy import and_
from qcfractal.storage_sockets.models import BaseResultORM, ResultORM, TaskQueueORM
from qcfractal.interface.models import ResultRecord, TaskStatusEnum
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, get_count_fast

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Dict, Union, Optional


class ResultSocket:
    def __init__(self, core_socket):
        self._core_socket = core_socket

    def add(self, record_list: List[ResultRecord]):
        """
        Add results from a given dict. The dict should have all the required
        keys of a result.

        Parameters
        ----------
        data : List[ResultRecord]
            Each dict in the list must have:
            program, driver, method, basis, options, molecule
            Where molecule is the molecule id in the DB
            In addition, it should have the other attributes that it needs
            to store

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs, in the same order as the
            input record_list
        """

        meta = add_metadata_template()

        results_list = []
        duplicates_list = []

        # Stores indices referring to elements in record_list
        new_record_idx, duplicates_idx = [], []

        # creating condition for a multi-value select
        # This can be used to query for multiple results in a single query
        conds = [
            and_(
                ResultORM.program == res.program,
                ResultORM.driver == res.driver,
                ResultORM.method == res.method,
                ResultORM.basis == res.basis,
                ResultORM.keywords == res.keywords,
                ResultORM.molecule == res.molecule,
            )
            for res in record_list
        ]

        with self._core_socket.session_scope() as session:
            # Query for all existing
            # TODO: RACE CONDITION: Records could be inserted between this query and inserting later

            existing_results = {}

            for cond in conds:
                doc = (
                    session.query(
                        ResultORM.program,
                        ResultORM.driver,
                        ResultORM.method,
                        ResultORM.basis,
                        ResultORM.keywords,
                        ResultORM.molecule,
                        ResultORM.id,
                    )
                    .filter(cond)
                    .one_or_none()
                )

                if doc is not None:
                    existing_results[
                        (doc.program, doc.driver, doc.method, doc.basis, doc.keywords, str(doc.molecule))
                    ] = doc

            # Loop over all (input) records, keeping track each record's index in the list
            for i, result in enumerate(record_list):
                # constructing an index from the record compare against items existing_results
                idx = (
                    result.program,
                    result.driver.value,
                    result.method,
                    result.basis,
                    int(result.keywords) if result.keywords else None,
                    result.molecule,
                )

                if idx not in existing_results:
                    # Does not exist in the database. Construct a new ResultORM
                    doc = ResultORM(**result.dict(exclude={"id"}))

                    # Store in existing_results in case later records are duplicates
                    existing_results[idx] = doc

                    # add the object to the list for later adding and committing to database.
                    results_list.append(doc)

                    # Store the index of this record (in record_list) as a new_record
                    new_record_idx.append(i)
                    meta["n_inserted"] += 1
                else:
                    # This result already exists in the database
                    doc = existing_results[idx]

                    # Store the index of this record (in record_list) as a new_record
                    duplicates_idx.append(i)

                    # Store the entire object. Since this may be a duplicate of a record
                    # added in a previous iteration of the loop, and the data hasn't been added/committed
                    # to the database, the id may not be known here
                    duplicates_list.append(doc)

            session.add_all(results_list)
            session.commit()

            # At this point, all ids should be known. So store only the ids in the returned metadata
            meta["duplicates"] = [str(doc.id) for doc in duplicates_list]

            # Construct the ID list to return (in the same order as the input data)
            # Use a placeholder for all, and we will fill later
            result_ids = [None] * len(record_list)

            # At this point:
            #     results_list: ORM objects for all newly-added results
            #     new_record_idx: indices (referring to record_list) of newly-added results
            #     duplicates_idx: indices (referring to record_list) of results that already existed
            #
            # results_list and new_record_idx are in the same order
            # (ie, the index stored at new_record_idx[0] refers to some element of record_list. That
            # newly-added ResultORM is located at results_list[0])
            #
            # Similarly, duplicates_idx and meta["duplicates"] are in the same order

            for idx, new_result in zip(new_record_idx, results_list):
                result_ids[idx] = str(new_result.id)

            # meta["duplicates"] only holds ids at this point
            for idx, existing_result_id in zip(duplicates_idx, meta["duplicates"]):
                result_ids[idx] = existing_result_id

        assert None not in result_ids

        meta["success"] = True

        ret = {"data": result_ids, "meta": meta}
        return ret

    def update(self, record_list: List[ResultRecord]):
        """
        Update results from a given dict (replace existing)

        Parameters
        ----------
        id : list of str
            Ids of the results to update, must exist in the DB
        data : list of dict
            Data that needs to be updated
            Shouldn't update:
            program, driver, method, basis, options, molecule

        Returns
        -------
            number of records updated
        """
        query_ids = [res.id for res in record_list]
        # find duplicates among ids
        duplicates = len(query_ids) != len(set(query_ids))

        with self._core_socket.session_scope() as session:

            found = session.query(ResultORM).filter(ResultORM.id.in_(query_ids)).all()
            # found items are stored in a dictionary
            found_dict = {str(record.id): record for record in found}

            updated_count = 0
            for result in record_list:

                if result.id is None:
                    self._core_socket.logger.error("Attempted update without ID, skipping")
                    continue

                data = result.dict(exclude={"id"})
                # retrieve the found item
                found_db = found_dict[result.id]

                # updating the found item with input attribute values.
                for attr, val in data.items():
                    setattr(found_db, attr, val)

                # if any duplicate ids are found in the input, commit should be called each iteration
                if duplicates:
                    session.commit()

                updated_count += 1
            # if no duplicates found, only commit at the end of the loop.
            if not duplicates:
                session.commit()

        # Notify of completion only after committing
        # (also, session not needed)
        for result in record_list:
            if result.status in [TaskStatusEnum.complete, TaskStatusEnum.error]:
                self._core_socket.notify_completed_watch(result.id, result.status)


        return updated_count

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
            The fields to not return, default to return all
        limit : Optional[int], optional
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
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
