"""
Procedure for a single computational task (energy, gradient, etc)
"""
from __future__ import annotations

import logging
from datetime import datetime as dt
from sqlalchemy.orm import joinedload, load_only, selectinload

from . import helpers
from .base import BaseProcedureHandler
from ...models import TaskQueueORM, ResultORM
from ...sqlalchemy_common import insert_general, get_query_proj_columns
from ....interface.models import (
    ObjectId,
    RecordStatusEnum,
    PriorityEnum,
    AtomicInput,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...sqlalchemy_socket import SQLAlchemySocket
    from ....interface.models import AtomicResult, SingleProcedureSpecification, InsertMetadata
    from typing import List, Optional, Tuple, Dict, Any, Sequence

    SingleProcedureDict = Dict[str, Any]


class SingleResultHandler(BaseProcedureHandler):
    """A task generator for a single QC computation task.

    This is a single quantum calculation, unique by program, driver, method, basis, keywords, molecule.
    """

    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.result

        BaseProcedureHandler.__init__(self)

    def add_orm(
        self, results: List[ResultORM], *, session: Optional[Session] = None
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

    def get(
        self,
        id: Sequence[ObjectId],
        include_molecule: bool = False,
        include_outputs: bool = False,
        include_wavefunction: bool = False,
        include_task: bool = False,
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
        include_molecule
            If True, include the full molecule information in the returned dictionary
        include_outputs
            If True, include the full calculation outputs (stdout, stderr, error) in the returned dictionary
        include_wavefunction
            If True, include the full wavefunction data in the returned dictionary
        include_task
            If True, include all info about the task in the returned dictionary
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

        load_cols = get_query_proj_columns(ResultORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(ResultORM).filter(ResultORM.id.in_(unique_ids)).options(load_only(*load_cols))

            if include_molecule:
                query = query.options(selectinload(ResultORM.molecule_obj))
            if include_outputs:
                query = query.options(selectinload(ResultORM.stdout_obj, ResultORM.stderr_obj, ResultORM.error_obj))
            if include_wavefunction:
                query = query.options(selectinload(ResultORM.wavefunction_data_obj))
            if include_task:
                query = query.options(selectinload(ResultORM.task_obj))

            results = query.yield_per(100)
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested single result records")

            return ret

    @staticmethod
    def build_schema_input(result: ResultORM, molecule: Dict[str, Any], keywords: Dict[str, Any]) -> AtomicInput:
        """
        Creates an input schema (QCSchema format) for a single calculation from an ORM

        Parameters
        ----------
        result
            An ORM of the containing information to build the schema with
        molecule
            A dictionary representing the molecule of the calculation
        keywords
            A dictionary representing the keywords of the calculation


        Returns
        -------
        :
            A self-contained AtomicInput (QCSchema) that can be used to run the calculation
        """

        # Now start creating the "model" parameter for ResultInput
        model = {"method": result.method}

        if result.basis:
            model["basis"] = result.basis

        return AtomicInput(
            id=result.id,
            driver=result.driver,
            model=model,
            molecule=molecule,
            keywords=keywords,
            protocols=result.protocols,
        )

    def verify_input(self, data):
        pass

    def create(
        self, session: Session, molecule_ids: Sequence[int], qc_spec: SingleProcedureSpecification
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """Create result objects and tasks for a single computation

        This will create the result objects in the database (if they do not exist), and also create the corresponding
        tasks.

        The returned list of ids (the first element of the tuple) will be in the same order as the input molecules

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        molecule_ids
            List or other sequence of molecule IDs to create results for
        qc_spec
            Specification of the single computation

        Returns
        -------
        :
            A tuple containing information about which results were inserted, and a list of IDs corresponding
            to all the results in the database (new or existing). This will be in the same order as the input
            molecules.
        """

        # We should only have gotten here if procedure is 'single'
        assert qc_spec.procedure == "single"

        # Handle keywords, which may be None
        if qc_spec.keywords is not None:
            # QCSpec will only hold the ID
            meta, qc_keywords_ids = self._core_socket.keywords.add_mixed([qc_spec.keywords])

            if meta.success is False or qc_keywords_ids[0] is None:
                raise KeyError("Could not find requested KeywordsSet from id key.")

            keywords_id = qc_keywords_ids[0]
        else:
            keywords_id = None

        # Create the ORM for everything
        all_result_orms = []
        for mol_id in molecule_ids:
            result_orm = ResultORM()
            result_orm.procedure = qc_spec.procedure
            result_orm.version = 1
            result_orm.program = qc_spec.program.lower()
            result_orm.driver = qc_spec.driver.lower()
            result_orm.method = qc_spec.method.lower()
            result_orm.basis = qc_spec.basis.lower() if qc_spec.basis else None  # Will make "" -> None
            result_orm.keywords = int(keywords_id) if keywords_id is not None else None  # TODO - INT ID
            result_orm.molecule = mol_id
            result_orm.protocols = qc_spec.protocols.dict()
            result_orm.extras = dict()
            all_result_orms.append(result_orm)

        # Add all results to the database
        insert_meta, result_ids = self.add_orm(all_result_orms, session=session)

        # Now generate all the tasks in the task queue
        self.create_tasks(session, result_ids, qc_spec.tag, qc_spec.priority)

        return insert_meta, result_ids

    def create_tasks(
        self,
        session: Session,
        id: Sequence[ObjectId],
        tag: Optional[str] = None,
        priority: Optional[PriorityEnum] = None,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Create entries in the task table for a given list of result ids

        For all the result ids, create the corresponding task if it does not exist.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        id
            List or other sequence of result IDs to create tasks for
        tag
            Tag to use for newly-created tasks
        priority
            Priority to use for newly-created tasks

        Returns
        -------
        :
            Metadata about which tasks were created or existing, and a list of Task IDs (new or existing)
        """

        # Load the ORM including the keywords
        # All we need from keywords is the 'values' column so no need to go
        # through the keywords subsocket
        result_orm: List[ResultORM] = (
            session.query(ResultORM).filter(ResultORM.id.in_(id)).options(joinedload(ResultORM.keywords_obj)).all()
        )

        #  Get all molecules in the same order
        molecule_ids = [x.molecule for x in result_orm]
        molecules = self._core_socket.molecule.get(molecule_ids, session=session)

        # Create QCSchema inputs and tasks for everything, too
        new_tasks = []
        for res, molecule in zip(result_orm, molecules):
            # TODO - can remove check when keywords made not nullable
            keywords = res.keywords_obj.values if res.keywords is not None else dict()

            qcschema_inp = self.build_schema_input(res, molecule, keywords)
            spec = {
                "function": "qcengine.compute",  # todo: add defaults in models
                "args": [qcschema_inp.dict(), res.program],
                "kwargs": {},  # todo: add defaults in models
            }

            # Build task object
            task = TaskQueueORM()
            task.spec = spec
            task.parser = "single"
            task.program = res.program
            task.base_result_id = res.id
            task.tag = tag
            task.priority = priority

            new_tasks.append(task)

            return self._core_socket.task.add_orm(new_tasks, session=session)

    def update_completed(self, session: Session, task_orm: TaskQueueORM, manager_name: str, result: AtomicResult):
        """
        Update the database with information from a completed single result task

        The session is flushed at the end of this function

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        task_orm
            A TaskORM object to fill out with the completed data
        manager_name
            Name of the manager that completed this task
        result
            The result of the computation to add to the database
        """

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

        # TODO - can we have managers return int here? molecule.id is Any

        # Single-result specific fields
        result_orm.return_result = result.return_result
        result_orm.properties = result.properties.dict()

        # Now set the rest of the info
        result_orm.extras = result.extras
        result_orm.provenance = result.provenance.dict()
        result_orm.manager_name = manager_name
        result_orm.status = RecordStatusEnum.complete
        result_orm.modified_on = dt.utcnow()

        session.flush()
