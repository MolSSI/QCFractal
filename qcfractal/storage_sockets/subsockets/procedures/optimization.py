"""
Optimization procedure/task
"""

from __future__ import annotations

import logging
from datetime import datetime as dt
from sqlalchemy.orm import selectinload, load_only

from . import helpers
from .base import BaseProcedureHandler
from ...models import TaskQueueORM, OptimizationProcedureORM, ResultORM, Trajectory
from ...sqlalchemy_common import insert_general, get_query_proj_columns
from ....interface.models import (
    ObjectId,
    OptimizationRecord,
    RecordStatusEnum,
    OptimizationResult,
    OptimizationInput,
    PriorityEnum,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...sqlalchemy_socket import SQLAlchemySocket
    from ....interface.models import AtomicResult, OptimizationProcedureSpecification, InsertMetadata
    from typing import List, Optional, Tuple, Dict, Any, Sequence

    OptimizationProcedureDict = Dict[str, Any]


class OptimizationHandler(BaseProcedureHandler):
    """
    Optimization task manipulation
    """

    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.result

        BaseProcedureHandler.__init__(self)

    def add_orm(
        self, optimizations: List[OptimizationProcedureORM], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds OptimizationProcedureORM to the database, taking into account duplicates

        The session is flushed at the end of this function.

        Parameters
        ----------
        optimizations
            ORM objects to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata showing what was added, and a list of returned optimization ids. These will be in the
            same order as the inputs, and may correspond to newly-inserted ORMs or to existing data.
        """

        # TODO - HACK
        # need to get the hash (for now)
        for opt in optimizations:
            d = opt.dict()
            d.pop("extras")
            d.pop("result_type")
            r = OptimizationRecord(**d)
            opt.hash_index = r.get_hash_index()

        with self._core_socket.optional_session(session) as session:
            meta, orm = insert_general(
                session, optimizations, (OptimizationProcedureORM.hash_index,), (OptimizationProcedureORM.id,)
            )
        return meta, [x[0] for x in orm]

    def get(
        self,
        id: Sequence[ObjectId],
        include_initial_molecule: bool = False,
        include_final_molecule: bool = False,
        include_outputs: bool = False,
        include_wavefunction: bool = False,
        include_task: bool = False,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[OptimizationProcedureDict]]:
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
        include_initial_molecule
            If True, include the full initial molecule information in the returned dictionary
        include_final_molecule
            If True, include the full final molecule information in the returned dictionary
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
            query = (
                session.query(OptimizationProcedureORM)
                .filter(OptimizationProcedureORM.id.in_(unique_ids))
                .options(load_only(*load_cols))
            )

            if include_initial_molecule:
                query = query.options(selectinload(OptimizationProcedureORM.initial_molecule_obj))
            if include_final_molecule:
                query = query.options(selectinload(OptimizationProcedureORM.final_molecule_obj))
            if include_outputs:
                query = query.options(
                    selectinload(
                        OptimizationProcedureORM.stdout_obj,
                        OptimizationProcedureORM.stderr_obj,
                        OptimizationProcedureORM.error_obj,
                    )
                )
            if include_wavefunction:
                query = query.options(selectinload(OptimizationProcedureORM.wavefunction_data_obj))
            if include_task:
                query = query.options(selectinload(OptimizationProcedureORM.task_obj))

            results = query.yield_per(100)
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested single result records")

            return ret

    @staticmethod
    def build_schema_input(
        optimization: OptimizationProcedureORM, molecule: Dict[str, Any], qc_keywords: Dict[str, Any]
    ) -> OptimizationInput:
        """
        Creates an input schema (QCSchema format) for an optimization calculation from an ORM

        Parameters
        ----------
        optimization
            An ORM of the containing information to build the schema with
        molecule
            A dictionary representing the initial molecule of the optimizaion
        qc_keywords
            A dictionary representing the keywords of the individual single calculations making up the optimization

        Returns
        -------
        :
            A self-contained OptimizaionInput (QCSchema) that can be used to run the calculation
        """

        # For the qcinput specification for the optimization (qcelemental QCInputSpecification)
        qcinput_spec = {"driver": optimization.qc_spec["driver"], "model": {"method": optimization.qc_spec["method"]}}

        if "basis" in optimization.qc_spec:
            qcinput_spec["model"]["basis"] = optimization.qc_spec["basis"]

        if qc_keywords is not None:
            qcinput_spec["keywords"] = qc_keywords
        else:
            qcinput_spec["keywords"] = {}

        return OptimizationInput(
            id=optimization.id,
            initial_molecule=molecule,
            keywords=optimization.keywords,
            extras=optimization.extras,
            hash_index=optimization.hash_index,
            input_specification=qcinput_spec,
            protocols=optimization.protocols,
        )

    def parse_trajectory(
        self, session: Session, results: Sequence[AtomicResult], qc_spec: Dict[str, Any], manager_name: str
    ) -> List[ResultORM]:
        """Parses the output of single results that form the trajectory of an optimization

        The ORM are added to the session, and the session is flushed

        Parameters
        ----------
        results
            A list or other sequence of AtomicResult that represent the trajectory
        qc_spec
            A dictionary representing the qc_spec that the optimizaion procedure used. This will be
            assigned to each result of the trajectory
        manager_name
            The name of the manager that completed the calculation
        session
            An existing SQLAlchemy session to use.

        Returns
        -------
        :
            A list of ResultORM corresponding to the trajectory. These are not
        """

        # Add all molecules at once
        molecules = [x.molecule for x in results]
        _, mol_ids = self._core_socket.molecule.add(molecules, session=session)

        ret = []
        for v, mol_id in zip(results, mol_ids):
            r = ResultORM()
            r.procedure = "single"
            r.program = qc_spec["program"].lower()
            r.driver = v.driver.lower()
            r.method = v.model.method.lower()
            r.basis = v.model.basis.lower() if v.model.basis else None
            r.keywords = qc_spec["keywords"] if "keywords" in qc_spec else None
            r.molecule = mol_id

            wfn_id, wfn_info = helpers.wavefunction_helper(self._core_socket, session, v.wavefunction)
            r.wavefunction = wfn_info
            r.wavefunction_data_id = wfn_id

            helpers.retrieve_outputs(self._core_socket, session, v, r)

            r.version = 1
            r.extras = v.extras
            r.return_result = v.return_result
            r.properties = v.properties.dict()
            r.provenance = v.provenance.dict()
            r.protocols = v.protocols.dict()

            if v.success:
                r.status = RecordStatusEnum.complete
            else:
                r.status = RecordStatusEnum.error

            r.manager_name = manager_name
            r.created_on = r.modified_on = dt.utcnow()
            ret.append(r)

        return ret

    def verify_input(self, data):
        pass

    def create(
        self, session: Session, molecule_ids: Sequence[int], opt_spec: OptimizationProcedureSpecification
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """Create optimization procedure objects and tasks for an optimizaion computation

        This will create the optimization procedure objects in the database (if they do not exist), and also create the corresponding
        tasks.

        The returned list of ids (the first element of the tuple) will be in the same order as the input molecules

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        molecule_ids
            List or other sequence of molecule IDs to create results for
        opt_spec
            Specification of the optimization

        Returns
        -------
        :
            A tuple containing information about which optimizaions were inserted, and a list of IDs corresponding
            to all the results in the database (new or existing). This will be in the same order as the input
            molecules.
        """

        # We should only have gotten here if procedure is 'optimization'
        assert opt_spec.procedure.lower() == "optimization"

        # Handle (optimization) keywords, which may be None
        # TODO: These are not stored in the keywords table (yet)
        opt_keywords = {} if opt_spec.keywords is None else opt_spec.keywords

        # Set the program used for gradient evaluations. This is stored in the input qcspec
        # but the QCInputSpecification does not have a place for program. So instead
        # we move it to the optimization keywords
        opt_keywords["program"] = opt_spec.qc_spec["program"]

        # Pull out the QCSpecification from the input
        qc_spec_dict = opt_spec.qc_spec

        # Handle qc specification keywords, which may be None
        qc_keywords = qc_spec_dict.get("keywords", None)
        if qc_keywords is not None:
            # The keywords passed in may contain the entire KeywordSet.
            # But the QCSpec will only hold the ID
            meta, qc_keywords_ids = self._core_socket.keywords.add_mixed([qc_keywords])

            if meta.success is False or qc_keywords_ids[0] is None:
                raise KeyError("Could not find requested KeywordsSet from id key.")

            qc_spec_dict["keywords"] = qc_keywords_ids[0]

        # Create the ORM for everything
        all_opt_orms = []
        for mol_id in molecule_ids:
            opt_orm = OptimizationProcedureORM()
            opt_orm.procedure = opt_spec.procedure
            opt_orm.version = 1
            opt_orm.program = opt_spec.program
            opt_orm.qc_spec = qc_spec_dict
            opt_orm.initial_molecule = mol_id
            opt_orm.keywords = opt_keywords
            opt_orm.status = RecordStatusEnum.incomplete
            opt_orm.extras = dict()

            # TODO - fix after defaults/nullable are fixed
            if hasattr(opt_spec, "protocols"):
                opt_orm.protocols = opt_spec.protocols.dict()
            else:
                opt_orm.protocols = {}

            all_opt_orms.append(opt_orm)

        insert_meta, opt_ids = self.add_orm(all_opt_orms, session=session)

        # Now generate all the tasks in the task queue
        self.create_tasks(session, opt_ids, opt_spec.tag, opt_spec.priority)

        return insert_meta, opt_ids

    def create_tasks(
        self,
        session: Session,
        id: List[ObjectId],
        tag: Optional[str] = None,
        priority: Optional[PriorityEnum] = None,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Create entries in the task table for a given list of optimizaion ids

        For all the optimization ids, create the corresponding task if it does not exist.

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
        opt_orm: List[OptimizationProcedureORM] = (
            session.query(OptimizationProcedureORM).filter(OptimizationProcedureORM.id.in_(id)).all()
        )

        # Find the molecules specified in the records
        molecule_ids = [x.initial_molecule for x in opt_orm]
        molecules = self._core_socket.molecule.get(molecule_ids, session=session)

        # Create QCSchema inputs and tasks for everything, too
        new_tasks = []
        for opt, molecule in zip(opt_orm, molecules):
            # TODO - fix when tables are normalized
            qc_keywords_id = opt.qc_spec.get("keywords", None)

            if qc_keywords_id is not None:
                qc_keywords = self._core_socket.keywords.get([qc_keywords_id])[0]["values"]
            else:
                qc_keywords = None

            qcschema_inp = self.build_schema_input(opt, molecule, qc_keywords)
            spec = {
                "function": "qcengine.compute_procedure",
                "args": [qcschema_inp.dict(), opt.program],
                "kwargs": {},
            }

            # Build task object
            task = TaskQueueORM()
            task.spec = spec
            task.parser = "optimization"
            task.program = opt.qc_spec["program"]
            task.procedure = opt.program
            task.tag = tag
            task.priority = priority
            task.base_result_id = opt.id

            new_tasks.append(task)

        return self._core_socket.task.add_orm(new_tasks, session=session)

    def update_completed(self, session: Session, task_orm: TaskQueueORM, manager_name: str, result: OptimizationResult):
        """
        Update the database with information from a completed optimization task

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

        # This should be of type OptimizationProcedureORM
        result_orm: OptimizationProcedureORM = task_orm.base_result_obj
        assert isinstance(result_orm, OptimizationProcedureORM)

        # Get the outputs
        helpers.retrieve_outputs(self._core_socket, session, result, result_orm)

        meta, mol_ids = self._core_socket.molecule.add([result.initial_molecule, result.final_molecule])

        assert ObjectId(mol_ids[0]) == ObjectId(result_orm.initial_molecule)
        assert result_orm.final_molecule is None

        result_orm.initial_molecule = mol_ids[0]
        result_orm.final_molecule = mol_ids[1]

        # use the QCSpec stored in the db rather than figure it out from the qcelemental model
        trajectory_orm = self.parse_trajectory(session, result.trajectory, result_orm.qc_spec, manager_name)
        meta, trajectory_ids = self._core_socket.procedure.single.add_orm(trajectory_orm, session=session)

        # Optimizations can have overlapping trajectories
        # An unhandled case is where the gradient is actually a requested calculation elsewhere
        # TODO - after allowing duplicates, this won't matter anymore
        if meta.n_existing > 0:
            existing_ids = [trajectory_ids[x] for x in meta.existing_idx]
            self._logger.info(
                f"Trajectory for {task_orm.base_result_id} overlaps on gradient calculations: {existing_ids}"
            )

        # Add as a list of Trajectory entries to the optimization orm
        result_orm.trajectory_obj = []
        for idx, t in enumerate(trajectory_orm):
            traj_assoc = Trajectory(opt_id=result_orm.id, result_id=t.id, position=idx)  # type: ignore
            result_orm.trajectory_obj.append(traj_assoc)

        # Optimization-specific fields
        result_orm.energies = result.energies

        # More general info
        result_orm.extras = result.extras
        result_orm.provenance = result.provenance.dict()
        result_orm.manager_name = manager_name
        result_orm.status = RecordStatusEnum.complete
        result_orm.modified_on = dt.utcnow()

        session.flush()
