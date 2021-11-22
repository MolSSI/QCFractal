"""
Optimization procedure/task
"""

from __future__ import annotations

import logging
from datetime import datetime
from sqlalchemy import and_
from sqlalchemy.orm import selectinload, load_only

from qcfractal.components.records import helpers
from qcfractal.components.records.base_handlers import BaseProcedureHandler
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.components.records.optimization.db_models import Trajectory, OptimizationProcedureORM
from qcfractal.components.records.singlepoint.db_models import ResultORM
from qcfractal.db_socket.helpers import insert_general, get_query_proj_options, get_count, calculate_limit
from qcfractal.interface.models import (
    ObjectId,
    OptimizationRecord,
    RecordStatusEnum,
    OptimizationResult,
    OptimizationInput,
    PriorityEnum,
)

from qcfractal.portal.metadata_models import QueryMetadata, InsertMetadata
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.interface.models import AtomicResult, OptimizationProcedureSpecification
    from typing import List, Optional, Tuple, Dict, Any, Sequence, Iterable

    OptimizationProcedureDict = Dict[str, Any]


class OptimizationHandler(BaseProcedureHandler):
    """
    Optimization task manipulation
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.record

        BaseProcedureHandler.__init__(self)

    def add_orm(
        self, optimizations: Sequence[OptimizationProcedureORM], *, session: Optional[Session] = None
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

        with self.root_socket.optional_session(session) as session:
            meta, orm = insert_general(
                session, optimizations, (OptimizationProcedureORM.hash_index,), (OptimizationProcedureORM.id,)
            )
            return meta, [x[0] for x in orm]

    def validate_input(self, spec: OptimizationProcedureSpecification):
        #####################################
        # See base class for method docstring
        #####################################
        pass

    def create_records(
        self,
        session: Session,
        molecule_ids: Sequence[int],
        spec: OptimizationProcedureSpecification,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        #####################################
        # See base class for method docstring
        #####################################

        # We should only have gotten here if procedure is 'optimization'
        assert spec.procedure.lower() == "optimization"

        # Handle (optimization) keywords, which may be None
        # TODO: These are not stored in the keywords table (yet)
        opt_keywords = {} if spec.keywords is None else spec.keywords

        # Set the program used for gradient evaluations. This is stored in the input qcspec
        # but the QCInputSpecification does not have a place for program. So instead
        # we move it to the optimization keywords
        opt_keywords["program"] = spec.qc_spec["program"]

        # Pull out the QCSpecification from the input
        qc_spec_dict = spec.qc_spec

        # Handle qc specification keywords, which may be None
        qc_keywords = qc_spec_dict.get("keywords", None)
        if qc_keywords is not None:
            # The keywords passed in may contain the entire KeywordSet.
            # But the QCSpec will only hold the ID
            meta, qc_keywords_ids = self.root_socket.keywords.add_mixed([qc_keywords], session=session)

            if meta.success is False or qc_keywords_ids[0] is None:
                raise KeyError("Could not find requested KeywordsSet from id key.")

            qc_spec_dict["keywords"] = qc_keywords_ids[0]

        # Create the ORM for everything
        all_opt_orms = []
        for mol_id in molecule_ids:
            opt_orm = OptimizationProcedureORM()
            opt_orm.procedure = spec.procedure
            opt_orm.version = 1
            opt_orm.program = spec.program
            opt_orm.qc_spec = qc_spec_dict
            opt_orm.initial_molecule = mol_id
            opt_orm.keywords = opt_keywords
            opt_orm.status = RecordStatusEnum.waiting
            opt_orm.extras = dict()

            # TODO - fix after defaults/nullable are fixed
            if hasattr(spec, "protocols"):
                opt_orm.protocols = spec.protocols.dict()
            else:
                opt_orm.protocols = {}

            all_opt_orms.append(opt_orm)

        # Add all optimizations to the database. Also flushes the session
        return self.add_orm(all_opt_orms, session=session)

    def create_tasks(
        self,
        session: Session,
        optimizations: Sequence[OptimizationProcedureORM],
        tag: Optional[str],
        priority: PriorityEnum,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        #####################################
        # See base class for method docstring
        #####################################

        all_tasks = []
        for optimization in optimizations:
            # TODO - fix when tables are normalized
            qc_keywords_id = optimization.qc_spec.get("keywords", None)

            # TODO - Also fix if made not nullable
            if qc_keywords_id is not None:
                qc_keywords = self.root_socket.keywords.get([qc_keywords_id], session=session)[0]["values"]
            else:
                qc_keywords = {}

            qcinput_spec = {
                "driver": optimization.qc_spec["driver"],
                "model": {"method": optimization.qc_spec["method"]},
                "keywords": qc_keywords,
            }

            if "basis" in optimization.qc_spec:
                qcinput_spec["model"]["basis"] = optimization.qc_spec["basis"]

            qcschema_input = OptimizationInput(
                id=optimization.id,
                initial_molecule=optimization.initial_molecule_obj.dict(),
                keywords=optimization.keywords,
                extras=optimization.extras,
                hash_index=optimization.hash_index,
                input_specification=qcinput_spec,
                protocols=optimization.protocols,
            )
            spec = {
                "function": "qcengine.compute_procedure",
                "args": [qcschema_input.dict(), optimization.program],
                "kwargs": {},
            }

            # Build task object
            task = TaskQueueORM()
            task.spec = spec

            # For now, we just add the programs as top-level keys. Eventually I would like to add
            # version restrictions as well
            task.required_programs = {optimization.qc_spec["program"]: None, optimization.program: None}

            task.base_result_id = optimization.id
            task.tag = tag
            task.priority = priority

            all_tasks.append(task)

        # Add all tasks to the database. Also flushes the session
        return self.root_socket.tasks.add_task_orm(all_tasks, session=session)

    def update_completed(
        self,
        session: Session,
        task_orm: TaskQueueORM,
        manager_name: str,
        result: OptimizationResult,
    ):
        #####################################
        # See base class for method docstring
        #####################################

        # This should be of type OptimizationProcedureORM
        result_orm: OptimizationProcedureORM = task_orm.base_result_obj
        assert isinstance(result_orm, OptimizationProcedureORM)

        # Get the outputs
        helpers.retrieve_outputs(self.root_socket, session, result, result_orm)

        meta, mol_ids = self.root_socket.molecules.add(
            [result.initial_molecule, result.final_molecule], session=session
        )

        assert ObjectId(mol_ids[0]) == ObjectId(result_orm.initial_molecule)
        assert result_orm.final_molecule is None

        result_orm.initial_molecule = mol_ids[0]
        result_orm.final_molecule = mol_ids[1]

        # use the QCSpec stored in the db rather than figure it out from the qcelemental model
        trajectory_orm = self.parse_trajectory(session, result.trajectory, result_orm.qc_spec, manager_name)
        meta, trajectory_ids = self.root_socket.tasks.single.add_orm(trajectory_orm, session=session)

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
        for idx, tid in enumerate(trajectory_ids):
            traj_assoc = Trajectory(opt_id=result_orm.id, result_id=tid, position=idx)  # type: ignore
            result_orm.trajectory_obj.append(traj_assoc)

        # Optimization-specific fields
        result_orm.energies = result.energies

        # More general info
        result_orm.extras = result.extras
        result_orm.provenance = result.provenance.dict()
        result_orm.manager_name = manager_name
        result_orm.status = RecordStatusEnum.complete
        result_orm.modified_on = datetime.utcnow()

    def parse_trajectory(
        self,
        session: Session,
        results: Sequence[AtomicResult],
        qc_spec: Dict[str, Any],
        manager_name: str,
    ) -> List[ResultORM]:
        """Parses the output of single results that form the trajectory of an optimization

        The ORM are added to the session, and the session is flushed

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use.
        results
            A list or other sequence of AtomicResult that represent the trajectory
        qc_spec
            A dictionary representing the qc_spec that the optimizaion procedure used. This will be
            assigned to each result of the trajectory
        manager_name
            The name of the manager that completed the calculation

        Returns
        -------
        :
            A list of ResultORM corresponding to the trajectory. These are not
        """

        # Add all molecules at once
        molecules = [x.molecule for x in results]
        _, mol_ids = self.root_socket.molecules.add(molecules, session=session)

        ret = []
        for v, mol_id in zip(results, mol_ids):
            r = ResultORM()
            r.procedure = "single"
            r.program = qc_spec["program"].lower()
            r.driver = v.driver.lower()
            r.method = v.model.method.lower()
            r.basis = v.model.basis.lower() if v.model.basis else None
            r.keywords = qc_spec["keywords"] if "keywords" in qc_spec else None
            r.molecule = int(mol_id)  # TODO - INT ID

            wfn_id, wfn_info = helpers.wavefunction_helper(self.root_socket, session, v.wavefunction)
            r.wavefunction = wfn_info
            r.wavefunction_data_id = wfn_id

            helpers.retrieve_outputs(self.root_socket, session, v, r)

            r.version = 1
            r.extras = v.extras
            r.return_result = v.return_result
            r.properties = v.properties.dict(encoding="json")
            r.provenance = v.provenance.dict()
            r.protocols = v.protocols.dict()

            if v.success:
                r.status = RecordStatusEnum.complete
            else:
                r.status = RecordStatusEnum.error

            r.manager_name = manager_name
            r.created_on = r.modified_on = datetime.utcnow()
            ret.append(r)

        session.flush()
        return ret

    def get(
        self,
        id: Sequence[ObjectId],
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
            raise RuntimeError(f"Request for {len(id)} optimization records is over the limit of {self._limit}")

        # TODO - int id
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        load_cols, load_rels = get_query_proj_columns(OptimizationProcedureORM, include, exclude)

        with self.root_socket.optional_session(session, True) as session:
            query = (
                session.query(OptimizationProcedureORM)
                .filter(OptimizationProcedureORM.id.in_(unique_ids))
                .options(load_only(*load_cols))
            )

            for r in load_rels:
                query = query.options(selectinload(r))

            results = query.yield_per(100)
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested optimization records")

            return ret

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        program: Optional[Iterable[str]] = None,
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
    ) -> Tuple[QueryMetadata, List[OptimizationProcedureDict]]:
        """

        Parameters
        ----------
        id
            Query for procedures based on its ID
        program
            Query based on program
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

        load_cols, load_rels = get_query_proj_columns(OptimizationProcedureORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(OptimizationProcedureORM.id.in_(id))
        if program is not None:
            and_query.append(OptimizationProcedureORM.program.in_(program))
        if manager is not None:
            and_query.append(OptimizationProcedureORM.manager_name.in_(manager))
        if status is not None:
            and_query.append(OptimizationProcedureORM.status.in_(status))
        if created_before is not None:
            and_query.append(OptimizationProcedureORM.created_on < created_before)
        if created_after is not None:
            and_query.append(OptimizationProcedureORM.created_on > created_after)
        if modified_before is not None:
            and_query.append(OptimizationProcedureORM.modified_on < modified_before)
        if modified_after is not None:
            and_query.append(OptimizationProcedureORM.modified_on > modified_after)

        with self.root_socket.optional_session(session, True) as session:
            query = session.query(OptimizationProcedureORM).filter(and_(*and_query))
            query = query.options(load_only(*load_cols))

            for r in load_rels:
                query = query.options(selectinload(r))

            n_found = get_count(query)
            results = query.limit(limit).offset(skip).yield_per(500)

            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts
