from __future__ import annotations

import logging
from qcfractal.interface.models import KVStore
from qcfractal.storage_sockets.models import BaseResultORM, ServiceQueueORM
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Union


class ServiceSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.service

    def add(self, service_list: List["BaseService"]):
        """
        Add services from a given list of dict.

        Parameters
        ----------
        services_list : List[Dict[str, Any]]
            List of services to be added
        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the hash_index of the inserted/existing docs
        """

        meta = add_metadata_template()

        procedure_ids = []
        with self._core_socket.session_scope() as session:
            for service in service_list:

                # Add the underlying procedure
                new_procedure = self._core_socket.procedure.add([service.output])

                # ProcedureORM already exists
                proc_id = new_procedure["data"][0]

                if new_procedure["meta"]["duplicates"]:
                    procedure_ids.append(proc_id)
                    meta["duplicates"].append(proc_id)
                    continue

                # search by hash index
                doc = session.query(ServiceQueueORM).filter_by(hash_index=service.hash_index)
                service.procedure_id = proc_id

                if doc.count() == 0:
                    doc = ServiceQueueORM(**service.dict(include=set(ServiceQueueORM.__dict__.keys())))
                    doc.extra = service.dict(exclude=set(ServiceQueueORM.__dict__.keys()))
                    doc.priority = doc.priority.value  # Must be an integer for sorting
                    session.add(doc)
                    session.commit()  # TODO
                    procedure_ids.append(proc_id)
                    meta["n_inserted"] += 1
                else:
                    procedure_ids.append(None)
                    meta["errors"].append((doc.id, "Duplicate service, but not caught by procedure."))

        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

    def get(
            self,
            id: Union[List[str], str] = None,
            procedure_id: Union[List[str], str] = None,
            hash_index: Union[List[str], str] = None,
            status: str = None,
            limit: int = None,
            skip: int = 0,
            return_json=True,
    ):
        """

        Parameters
        ----------
        id / hash_index : List[str] or str, optional
            service id
        procedure_id : List[str] or str, optional
            procedure_id for the specific procedure
        status : str, optional
            status of the record queried for
        limit : Optional[int], optional
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, optional
            skip the first 'skip' resaults. Used to paginate
            Default is 0
        return_json : bool, deafult is True
            Return the results as a list of json instead of objects

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the objects found
        """

        # TODO this function is used by the periodics class, for which the limit shouldn't apply
        limit =  calculate_limit(self._limit, limit)
        meta = get_metadata_template()
        query = format_query(ServiceQueueORM, id=id, hash_index=hash_index, procedure_id=procedure_id, status=status)

        with self._core_socket.session_scope() as session:
            data = (
                session.query(ServiceQueueORM)
                    .filter(*query)
                    .order_by(ServiceQueueORM.priority.desc(), ServiceQueueORM.created_on)
                    .limit(limit)
                    .offset(skip)
                    .all()
            )
            data = [x.to_dict() for x in data]

        meta["n_found"] = len(data)
        meta["success"] = True

        # except Exception as err:
        #     meta['error_description'] = str(err)

        return {"data": data, "meta": meta}

    def update(self, records_list: List["BaseService"]) -> int:
        """
        Replace existing service

        Raises exception if the id is invalid

        Parameters
        ----------
        records_list: List[Dict[str, Any]]
            List of Service items to be updated using their id

        Returns
        -------
        int
            number of updated services
        """

        updated_count = 0
        for service in records_list:
            if service.id is None:
                self.logger.error("No service id found on update (hash_index={}), skipping.".format(service.hash_index))
                continue

            with self._core_socket.session_scope() as session:

                doc_db = session.query(ServiceQueueORM).filter_by(id=service.id).first()

                data = service.dict(include=set(ServiceQueueORM.__dict__.keys()))
                data["extra"] = service.dict(exclude=set(ServiceQueueORM.__dict__.keys()))

                data["id"] = int(data["id"])
                for attr, val in data.items():
                    setattr(doc_db, attr, val)

                session.add(doc_db)
                session.commit()

            procedure = service.output
            procedure.__dict__["id"] = service.procedure_id

            # Copy the stdout/error from the service itself to its procedure
            if service.stdout:
                stdout = KVStore(data=service.stdout)
                stdout_id = self._core_socket.output_store.add([stdout])["data"][0]
                procedure.__dict__["stdout"] = stdout_id
            if service.error:
                error = KVStore(data=service.error.dict())
                error_id = self._core_socket.output_store.add([error])["data"][0]
                procedure.__dict__["error"] = error_id

            self._core_socket.procedure.update([procedure])

            updated_count += 1

        return updated_count

    def update_status(
            self, status: str, id: Union[List[str], str] = None, procedure_id: Union[List[str], str] = None
    ) -> int:
        """
        Update the status of the existing services in the database.

        Raises an exception if any of the ids are invalid.
        Parameters
        ----------
        status : str
            The input status string ready to replace the previous status
        id : Optional[Union[List[str], str]], optional
            ids of all the services requested to be updated, by default None
        procedure_id : Optional[Union[List[str], str]], optional
            procedure_ids for the specific procedures, by default None

        Returns
        -------
        int
            1 indicating that the status update was successful
        """

        if (id is None) and (procedure_id is None):
            raise KeyError("id or procedure_id must not be None.")

        status = status.lower()
        with self._core_socket.session_scope() as session:

            query = format_query(ServiceQueueORM, id=id, procedure_id=procedure_id)

            # Update the service
            service = session.query(ServiceQueueORM).filter(*query).first()
            service.status = status

            # Update the procedure
            if status == "waiting":
                status = "incomplete"
            session.query(BaseResultORM).filter(BaseResultORM.id == service.procedure_id).update({"status": status})

            session.commit()

            # Should only be done after committing
            if status in ["error", "complete"]:
                self._core_socket.notify_completed_watch(service.procedure_id, service.status)

        return 1

    def completed(self, records_list: List["BaseService"]) -> int:
        """
        Delete the services which are completed from the database.

        Parameters
        ----------
        records_list : List["BaseService"]
            List of Service objects which are completed.

        Returns
        -------
        int
            Number of deleted active services from database.
        """
        done = 0
        for service in records_list:
            if service.id is None:
                self._logger.error(
                    "No service id found on completion (hash_index={}), skipping.".format(service.hash_index)
                )
                continue

            # in one transaction
            with self._core_socket.session_scope() as session:

                procedure = service.output
                procedure.__dict__["id"] = service.procedure_id
                self._core_socket.procedure.update([procedure])

                session.query(ServiceQueueORM).filter_by(id=service.id).delete()  # synchronize_session=False)

            # Should only be done after committing
            self._core_socket.notify_completed_watch(service.procedure_id, service.status)

            done += 1

        return done


