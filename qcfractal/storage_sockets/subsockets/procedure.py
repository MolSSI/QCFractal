from __future__ import annotations

from sqlalchemy.orm import with_polymorphic
from qcfractal.storage_sockets.models import BaseResultORM, OptimizationProcedureORM, TorsionDriveProcedureORM, GridOptimizationProcedureORM
from qcfractal.interface.models import TaskStatusEnum
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, get_count_fast, get_procedure_class, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Union


class ProcedureSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._limit = core_socket.qcf_config.response_limits.result

    def add(self, record_list: List["BaseRecord"]):
        """
        Add procedures from a given dict. The dict should have all the required
        keys of a result.

        Parameters
        ----------
        record_list : List["BaseRecord"]
            Each dict must have:
            procedure, program, keywords, qc_meta, hash_index
            In addition, it should have the other attributes that it needs
            to store

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys data and meta, data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        if not record_list:
            return {"data": [], "meta": meta}

        procedure_class = get_procedure_class(record_list[0])

        procedure_ids = []
        with self._core_socket.session_scope() as session:
            for procedure in record_list:
                doc = session.query(procedure_class).filter_by(hash_index=procedure.hash_index)

                if get_count_fast(doc) == 0:
                    data = procedure.dict(exclude={"id"})
                    proc_db = procedure_class(**data)
                    session.add(proc_db)
                    session.commit()
                    proc_db.update_relations(**data)
                    session.commit()
                    procedure_ids.append(str(proc_db.id))
                    meta["n_inserted"] += 1
                else:
                    id = str(doc.first().id)
                    meta["duplicates"].append(id)  # TODO
                    procedure_ids.append(id)
        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

    def get(
            self,
            id: Union[str, List] = None,
            procedure: str = None,
            program: str = None,
            hash_index: str = None,
            task_id: Union[str, List] = None,
            manager_id: Union[str, List] = None,
            status: str = "COMPLETE",
            include=None,
            exclude=None,
            limit: int = None,
            skip: int = 0,
            return_json=True,
            with_ids=True,
    ):
        """

        Parameters
        ----------
        id : str or List[str]
        procedure : str
        program : str
        hash_index : str
        task_id : str or List[str]
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
            skip the first 'skip' resaults. Used to paginate
            Default is 0
        return_json : bool, optional
            Return the results as a list of json inseated of objects
            Default is True
        with_ids : bool, optional
            Include the ids in the returned objects/dicts
            Default is True

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data and meta. Data is the objects found
        """

        limit = calculate_limit(self._limit, limit)
        meta = get_metadata_template()

        if id is not None or task_id is not None:
            status = None

        if procedure == "optimization":
            className = OptimizationProcedureORM
        elif procedure == "torsiondrive":
            className = TorsionDriveProcedureORM
        elif procedure == "gridoptimization":
            className = GridOptimizationProcedureORM
        else:
            # raise TypeError('Unsupported procedure type {}. Id: {}, task_id: {}'
            #                 .format(procedure, id, task_id))
            className = BaseResultORM  # all classes, including those with 'selectin'
            program = None  # make sure it's not used
            if id is None:
                self._core_socket.logger.error(f"Procedure type not specified({procedure}), and ID is not given.")
                raise KeyError("ID is required if procedure type is not specified.")

        query = format_query(
            className,
            id=id,
            procedure=procedure,
            program=program,
            hash_index=hash_index,
            task_id=task_id,
            manager_id=manager_id,
            status=status,
        )

        data = []
        try:
            # TODO: decide a way to find the right type

            data, meta["n_found"] = self._core_socket.get_query_projection(
                className, query, limit=limit, skip=skip, include=include, exclude=exclude
            )
            meta["success"] = True
        except Exception as err:
            meta["error_description"] = str(err)

        return {"data": data, "meta": meta}

    def update(self, records_list: List["BaseRecord"]):
        """
        TODO: needs to be of specific type
        """

        updated_count = 0
        with self._core_socket.session_scope() as session:
            for procedure in records_list:

                className = get_procedure_class(procedure)
                # join_table = get_procedure_join(procedure)
                # Must have ID
                if procedure.id is None:
                    self._core_socket.logger.error(
                        "No procedure id found on update (hash_index={}), skipping.".format(procedure.hash_index)
                    )
                    continue

                proc_db = session.query(className).filter_by(id=procedure.id).first()

                data = procedure.dict(exclude={"id"})
                proc_db.update_relations(**data)

                for attr, val in data.items():
                    setattr(proc_db, attr, val)

                # session.add(proc_db)

                # Upsert relations (insert or update)
                # needs primarykeyconstraint on the table keys
                # for result_id in procedure.trajectory:
                #     statement = postgres_insert(opt_result_association)\
                #         .values(opt_id=procedure.id, result_id=result_id)\
                #         .on_conflict_do_update(
                #             index_elements=[opt_result_association.c.opt_id, opt_result_association.c.result_id],
                #             set_=dict(result_id=result_id))
                #     session.execute(statement)

                session.commit()

                # Should only be done after committing
                if procedure.status in [TaskStatusEnum.complete, TaskStatusEnum.error]:
                    self._core_socket.notify_completed_watch(procedure.id, procedure.status)
                updated_count += 1

        # session.commit()  # save changes, takes care of inheritance

        return updated_count

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
            procedures = (
                session.query(
                    with_polymorphic(
                        BaseResultORM,
                        [OptimizationProcedureORM, TorsionDriveProcedureORM, GridOptimizationProcedureORM],
                    )
                )
                    .filter(BaseResultORM.id.in_(ids))
                    .all()
            )
            # delete through session to delete correctly from base_result
            for proc in procedures:
                session.delete(proc)
            # session.commit()
            count = len(procedures)

        return count

