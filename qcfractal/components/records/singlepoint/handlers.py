"""
Procedure for a single computational task (energy, gradient, etc)
"""
from __future__ import annotations

import logging
from datetime import datetime
from sqlalchemy import and_, or_
from sqlalchemy.orm import load_only, selectinload

from qcfractal.components.records import helpers
from qcfractal.components.records.base_handlers import BaseProcedureHandler
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.components.records.singlepoint.db_models import ResultORM
from qcfractal.db_socket.helpers import insert_general, get_query_proj_columns, get_count, calculate_limit
from qcfractal.interface.models import ObjectId, RecordStatusEnum, PriorityEnum, AtomicInput, QueryMetadata

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.interface.models import AtomicResult, SingleProcedureSpecification, InsertMetadata
    from typing import List, Optional, Tuple, Dict, Any, Sequence, Iterable

    SingleProcedureDict = Dict[str, Any]


class SingleResultHandler(BaseProcedureHandler):
    """A task generator for a single QC computation task.

    This is a single quantum calculation, unique by program, driver, method, basis, keywords, molecule.
    """

    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.record

        BaseProcedureHandler.__init__(self)

    def add_orm(
        self, results: Sequence[ResultORM], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds ResultORM to the database, taking into account duplicates

        The session is flushed at the end of this function.

        Parameters
        ----------
        results
            ORM objects to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata showing what was added, and a list of returned result ids. These will be in the
            same order as the inputs, and may correspond to newly-inserted ORMs or to existing data.
        """
        dedup_cols = (
            ResultORM.program,
            ResultORM.driver,
            ResultORM.method,
            ResultORM.basis,
            ResultORM.keywords,
            ResultORM.molecule,
        )

        with self._core_socket.optional_session(session) as session:
            meta, orm = insert_general(session, results, dedup_cols, (ResultORM.id,))
            return meta, [x[0] for x in orm]

    def validate_input(self, spec: SingleProcedureSpecification):
        #####################################
        # See base class for method docstring
        #####################################
        pass

    def create_records(
        self,
        session: Session,
        molecule_ids: Sequence[int],
        spec: SingleProcedureSpecification,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        #####################################
        # See base class for method docstring
        #####################################

        # We should only have gotten here if procedure is 'single'
        assert spec.procedure == "single"

        # Handle keywords, which may be None
        if spec.keywords is not None:
            # QCSpec will only hold the ID
            meta, qc_keywords_ids = self._core_socket.keywords.add_mixed([spec.keywords], session=session)

            if meta.success is False or qc_keywords_ids[0] is None:
                raise KeyError("Could not find requested KeywordsSet from id key.")

            keywords_id = qc_keywords_ids[0]
        else:
            keywords_id = None

        # Create the ORM for everything
        all_result_orms = []
        for mol_id in molecule_ids:
            result_orm = ResultORM()
            result_orm.procedure = spec.procedure
            result_orm.version = 1
            result_orm.program = spec.program.lower()
            result_orm.driver = spec.driver.lower()
            result_orm.method = spec.method.lower()
            result_orm.basis = spec.basis.lower() if spec.basis else None  # Will make "" -> None
            result_orm.keywords = int(keywords_id) if keywords_id is not None else None  # TODO - INT ID
            result_orm.molecule = mol_id
            result_orm.protocols = spec.protocols.dict()
            result_orm.status = RecordStatusEnum.waiting
            result_orm.extras = dict()
            all_result_orms.append(result_orm)

        # Add all results to the database. Also flushes the session
        return self.add_orm(all_result_orms, session=session)

    def create_tasks(
        self,
        session: Session,
        results: Sequence[ResultORM],
        tag: Optional[str],
        priority: PriorityEnum,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        #####################################
        # See base class for method docstring
        #####################################

        all_tasks = []
        for result in results:
            # The "model" parameter for ResultInput is a few of our top-level fields
            model = {"method": result.method}

            if result.basis:
                model["basis"] = result.basis

            # TODO - fix after keywords not nullable
            keywords = result.keywords_obj.values if result.keywords_obj else {}

            qcschema_input = AtomicInput(
                id=result.id,
                driver=result.driver,
                model=model,
                molecule=result.molecule_obj.dict(),
                keywords=keywords,
                protocols=result.protocols,
            )

            spec = {
                "function": "qcengine.compute",
                "args": [qcschema_input.dict(), result.program],
                "kwargs": {},
            }

            # Build task object
            task = TaskQueueORM()
            task.spec = spec

            # For now, we just add the programs as top-level keys. Eventually I would like to add
            # version restrictions as well
            task.required_programs = {result.program: None}

            task.base_result_id = int(result.id)  # TODO - INT ID
            task.tag = tag
            task.priority = priority

            all_tasks.append(task)

        # Add all tasks to the database. Also flushes the session
        return self._core_socket.task.add_task_orm(all_tasks, session=session)

    def update_completed(self, session: Session, task_orm: TaskQueueORM, manager_name: str, result: AtomicResult):
        #####################################
        # See base class for method docstring
        #####################################

        # This should be of type ResultORM
        result_orm: ResultORM = task_orm.base_result_obj
        assert isinstance(result_orm, ResultORM)

        # Get the outputs
        helpers.retrieve_outputs(self._core_socket, session, result, result_orm)

        # Store Wavefunction data
        # Save the old id for later deletion
        old_wfn_id = result_orm.wavefunction_data_id

        wfn_id, wfn_info = helpers.wavefunction_helper(self._core_socket, session, result.wavefunction)
        result_orm.wavefunction_data_id = wfn_id
        result_orm.wavefunction = wfn_info

        # Now we can delete the old wavefunction (if it existed)
        if old_wfn_id is not None:
            self._core_socket.wavefunction.delete([old_wfn_id], session=session)

        # Double check to make sure everything is consistent
        assert result_orm.method == result.model.method
        assert result_orm.basis == result.model.basis
        assert result_orm.driver == result.driver

        # Single-result specific fields
        result_orm.return_result = result.return_result
        result_orm.properties = result.properties.dict()

        # Now set the rest of the info
        result_orm.extras = result.extras
        result_orm.provenance = result.provenance.dict()
        result_orm.manager_name = manager_name
        result_orm.status = RecordStatusEnum.complete
        result_orm.modified_on = datetime.utcnow()

    def get(
        self,
        id: Sequence[ObjectId],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[SingleProcedureDict]]:
        """
        Obtain results of single computations from with specified IDs from an existing session

        The returned information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of results will be None.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
        id
            A list or other sequence of result IDs
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, then missing results will be tolerated, and the returned list of
           Molecules will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Single result information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} single results is over the limit of {self._limit}")

        # TODO - int id
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        load_cols, load_rels = get_query_proj_columns(ResultORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(ResultORM).filter(ResultORM.id.in_(unique_ids)).options(load_only(*load_cols))

            for r in load_rels:
                query = query.options(selectinload(r))

            results = query.yield_per(100)
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested single result records")

            return ret

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        program: Optional[Iterable[str]] = None,
        driver: Optional[Iterable[str]] = None,
        method: Optional[Iterable[str]] = None,
        basis: Optional[Iterable[str]] = None,
        keywords: Optional[Iterable[ObjectId]] = None,
        molecule: Optional[Iterable[ObjectId]] = None,
        manager: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: int = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[SingleProcedureDict]]:
        """

        Parameters
        ----------
        id
            Query for procedures based on its ID
        program
            Query based on program
        driver
            Query based on driver
        method
            Query based on method
        basis
            Query based on basis
        keywords
            Query based on keywords
        molecule
            Query based on molecule
        manager
            Query based on manager
        status
            The status of the procedure
        created_before
            Query for records created before this date
        created_after
            Query for records created after this date
        modified_before
            Query for records modified before this date
        modified_after
            Query for records modified after this date
        include
            Which fields of the molecule to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        limit
            Limit the number of results. If None, the server limit will be used.
            This limit will not be respected if greater than the configured limit of the server.
        skip
            Skip this many results from the total list of matches. The limit will apply after skipping,
            allowing for pagination.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Metadata about the results of the query, and a list of procedure data (as dictionaries)
        """

        limit = calculate_limit(self._limit, limit)

        load_cols, load_rels = get_query_proj_columns(ResultORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(ResultORM.id.in_(id))
        if program is not None:
            and_query.append(ResultORM.program.in_(program))
        if driver is not None:
            and_query.append(ResultORM.driver.in_(driver))
        if method is not None:
            and_query.append(ResultORM.method.in_(method))
        if basis is not None:
            # Since basis can be null.....
            and_query.append(or_(ResultORM.basis == x for x in basis))
        if keywords is not None:
            # Since keywords can be null.....
            and_query.append(or_(ResultORM.keywords == x for x in keywords))
        if manager is not None:
            and_query.append(ResultORM.manager_name.in_(manager))
        if molecule is not None:
            and_query.append(ResultORM.molecule.in_(molecule))
        if status is not None:
            and_query.append(ResultORM.status.in_(status))
        if created_before is not None:
            and_query.append(ResultORM.created_on < created_before)
        if created_after is not None:
            and_query.append(ResultORM.created_on > created_after)
        if modified_before is not None:
            and_query.append(ResultORM.modified_on < modified_before)
        if modified_after is not None:
            and_query.append(ResultORM.modified_on > modified_after)

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(ResultORM).filter(and_(*and_query))
            query = query.options(load_only(*load_cols))

            for r in load_rels:
                query = query.options(selectinload(r))

            n_found = get_count(query)
            results = query.limit(limit).offset(skip).yield_per(500)

            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts
