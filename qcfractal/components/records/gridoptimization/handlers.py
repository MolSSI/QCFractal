"""
Procedure for a gridoptimization service
"""
from __future__ import annotations

import json
import logging
import numpy as np
from datetime import datetime

import sqlalchemy.orm.attributes
from sqlalchemy.orm import load_only, selectinload

from qcfractal import __version__ as qcfractal_version
from qcfractal.components.records.base_handlers import BaseServiceHandler
from qcfractal.storage_sockets.models import ServiceQueueORM
from qcfractal.components.records.gridoptimization.db_models import (
    GridOptimizationAssociation,
    GridOptimizationProcedureORM,
)
from qcfractal.components.molecule.db_models import MoleculeORM
from qcfractal.storage_sockets.sqlalchemy_common import insert_general, get_query_proj_columns
from qcfractal.interface.models import (
    ProtoModel,
    ObjectId,
    PriorityEnum,
    Molecule,
    RecordStatusEnum,
    OptimizationProcedureSpecification,
)

from qcfractal.interface.models.gridoptimization import (
    ScanDimension,
    StepTypeEnum,
    GridOptimizationKeywords,
    GridOptimizationInput,
    GridOptimizationRecord,
)


from typing import TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from qcfractal.interface.models import InsertMetadata
    from typing import Sequence, Dict, Optional, Any, Set, Union

    GridOptimizationProcedureDict = Dict[str, Any]


def expand_ndimensional_grid(
    dimensions: Tuple[int, ...], seeds: Set[Tuple[int, ...]], complete: Set[Tuple[int, ...]]
) -> List[Tuple[Tuple[int, ...], Tuple[int, ...]]]:
    """
    Expands an n-dimensional key/value grid.

    Example
    -------
    >>> expand_ndimensional_grid((3, 3), {(1, 1)}, set())
    [((1, 1), (0, 1)), ((1, 1), (2, 1)), ((1, 1), (1, 0)), ((1, 1), (1, 2))]
    """

    dimensions = tuple(dimensions)
    compute = set()
    connections = []

    for d in range(len(dimensions)):

        # Loop over all compute seeds
        for seed in seeds:

            # Iterate both directions
            for disp in [-1, 1]:
                new_dim = seed[d] + disp

                # Bound check
                if new_dim >= dimensions[d]:
                    continue
                if new_dim < 0:
                    continue

                new = list(seed)
                new[d] = new_dim
                new = tuple(new)

                # Push out duplicates from both new compute and copmlete
                if new in compute:
                    continue
                if new in complete:
                    continue

                compute |= {new}
                connections.append((seed, new))

    return connections


def serialize_key(key: Union[str, Sequence[int]]) -> str:
    """Serializes the key to map to the internal keys.

    Parameters
    ----------
    key : Union[int, Tuple[int]]
        A integer or list of integers denoting the position in the grid
        to find.

    Returns
    -------
    str
        The internal key value.
    """

    return json.dumps(key)


def deserialize_key(key: str) -> Union[str, Tuple[int, ...]]:
    """Deserializes a map key"""

    r = json.loads(key)
    if isinstance(r, str):
        return r
    else:
        return tuple(r)


def calculate_starting_grid(scans: Sequence[ScanDimension], molecule: Molecule) -> List[int]:
    starting_grid = []
    for scan in scans:

        # Find closest index
        if scan.step_type == StepTypeEnum.absolute:
            m = molecule.measure(scan.indices)
        elif scan.step_type == StepTypeEnum.relative:
            m = 0
        else:
            raise KeyError("'step_type' of '{}' not understood.".format(scan.step_type))

        idx = np.abs(np.array(scan.steps) - m).argmin()
        starting_grid.append(int(idx))  # converts from numpy int type

    return starting_grid


class GridOptimizationServiceState(ProtoModel):
    """
    This represents the current state of a torsiondrive service which is stored in the 'extra' field
    """

    class Config(ProtoModel.Config):
        allow_mutation = True
        validate_assignment = True

    iteration: int
    complete: List[Tuple[int, ...]]
    dimensions: tuple

    # These are stored as JSON (ie, dict encoded into a string)
    optimization_template: str
    constraint_template: str


class GridOptimizationHandler(BaseServiceHandler):
    """A handler for gridoptimization services"""

    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.record

        BaseServiceHandler.__init__(self, core_socket)

    def add_orm(
        self, gridopt_orms: Sequence[GridOptimizationProcedureORM], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds GridOptimizationProcedureORM to the database, taking into account duplicates

        The session is flushed at the end of this function.

        Parameters
        ----------
        gridopt_orms
            ORM objects to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata showing what was added, and a list of returned gridoptimization ids. These will be in the
            same order as the inputs, and may correspond to newly-inserted ORMs or to existing data.
        """

        # TODO - HACK
        # need to get the hash (for now)
        for gridopt in gridopt_orms:
            r = GridOptimizationRecord(
                initial_molecule=gridopt.initial_molecule_obj.id,
                starting_molecule=gridopt.initial_molecule_obj.id,  # Not a mistake
                keywords=gridopt.keywords,
                optimization_spec=gridopt.optimization_spec,
                qc_spec=gridopt.qc_spec,
                final_energy_dict={},
                grid_optimizations={},
                starting_grid=tuple(),
            )
            gridopt.hash_index = r.get_hash_index()

        with self._core_socket.optional_session(session) as session:
            meta, orm = insert_general(
                session, gridopt_orms, (GridOptimizationProcedureORM.hash_index,), (GridOptimizationProcedureORM.id,)
            )
        return meta, [x[0] for x in orm]

    def get(
        self,
        id: Sequence[ObjectId],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[GridOptimizationProcedureDict]]:
        """
        Obtain grid optimization procedure information from the database

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
            GridOptimization information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} grid optimization records is over the limit of {self._limit}")

        # TODO - int id
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        load_cols, load_rels = get_query_proj_columns(GridOptimizationProcedureORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            query = (
                session.query(GridOptimizationProcedureORM)
                .filter(GridOptimizationProcedureORM.id.in_(unique_ids))
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

    def verify_input(self, data):
        pass

    def create_records(
        self, session: Session, service_input: GridOptimizationInput
    ) -> Tuple[InsertMetadata, List[ObjectId]]:

        meta, mol_ids = self._core_socket.molecule.add_mixed([service_input.initial_molecule])

        # TODO - int id
        mol_id = int(mol_ids[0])

        initial_molecule_orm = session.query(MoleculeORM).filter(MoleculeORM.id == mol_id).one()

        gridopt_orm = GridOptimizationProcedureORM()
        gridopt_orm.keywords = service_input.keywords.dict()
        gridopt_orm.optimization_spec = service_input.optimization_spec.dict()
        gridopt_orm.qc_spec = service_input.qc_spec.dict()
        gridopt_orm.initial_molecule_obj = initial_molecule_orm

        # Set this to be the same as the initial molecule for now
        # (initial molecule = what we gave, starting molecule = what we are actually starting
        #  with, taking into account preoptimization)
        # This may change during the first iteration(s)
        gridopt_orm.starting_molecule_obj = initial_molecule_orm

        gridopt_orm.provenance = {
            "creator": "qcfractal",
            "version": qcfractal_version,
            "routine": "qcfractal.services.gridoptimization",
        }

        gridopt_orm.final_energy_dict = {}
        gridopt_orm.starting_grid = []
        gridopt_orm.protocols = {}
        gridopt_orm.extras = {}

        # Add this ORM to the database, taking into account duplicates
        insert_meta, td_ids = self.add_orm([gridopt_orm], session=session)

        return insert_meta, td_ids

    def create_tasks(
        self,
        session: Session,
        gridopt_orms: Sequence[GridOptimizationProcedureORM],
        tag: Optional[str],
        priority: PriorityEnum,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:

        new_services = []

        # Go over the input ids in order
        for idx, gridopt_orm in enumerate(gridopt_orms):
            service_state = {}

            # Completed seeds
            service_state["complete"] = []

            # Read in the keywords from the database. Converting to the pydantic
            # model is mostly for ergonomics
            keywords = GridOptimizationKeywords(**gridopt_orm.keywords)

            # Build constraint template
            constraint_template = []
            for scan in keywords.scans:
                tmp = {"type": scan.type, "indices": scan.indices}
                constraint_template.append(tmp)

            service_state["constraint_template"] = json.dumps(constraint_template)

            # Build optimization template
            opt_template = {
                "procedure": "optimization",
                "qc_spec": gridopt_orm.qc_spec,
                "tag": tag,
                "priority": priority,
            }

            opt_template.update(gridopt_orm.optimization_spec)
            service_state["optimization_template"] = json.dumps(opt_template)

            # The number of steps along each axis
            service_state["dimensions"] = tuple(len(x.steps) for x in keywords.scans)

            if keywords.preoptimization:
                service_state["iteration"] = -2
                gridopt_orm.starting_grid = []
            else:
                initial_molecule = gridopt_orm.initial_molecule_obj.to_model(Molecule)

                service_state["iteration"] = 0

                gridopt_orm.starting_grid = calculate_starting_grid(keywords.scans, initial_molecule)

            # Now create the service ORM
            svc_orm = ServiceQueueORM()
            svc_orm.tag = tag
            svc_orm.priority = priority
            svc_orm.procedure_id = gridopt_orm.id
            svc_orm.created_on = datetime.utcnow()
            svc_orm.modified_on = datetime.utcnow()
            svc_orm.service_state = service_state

            new_services.append(svc_orm)

            # Add the output to the base procedure
            # TODO Add info to output
            # gridopt_orm.stdout = self._core_socket.output_store.add([stdout])[0]

        return self._core_socket.service.add_task_orm(new_services, session=session)

    def iterate(self, session: Session, service_orm: ServiceQueueORM) -> bool:
        gridopt_orm = service_orm.procedure_obj
        keywords = GridOptimizationKeywords(**gridopt_orm.keywords)

        if gridopt_orm.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            # This is a programmer error
            raise RuntimeError(
                f"Grid Optimization {service_orm.id}/Base result {gridopt_orm.id} has status {gridopt_orm.status} - cannot iterate with that status!"
            )

        # Is this the first iteration?
        if gridopt_orm.status == RecordStatusEnum.waiting:
            gridopt_orm.status = RecordStatusEnum.running

        # Load the state from the service_state column
        service_state = GridOptimizationServiceState(**service_orm.service_state)

        # Special preoptimization iterations
        if service_state.iteration == -2:
            new_opt_spec = json.loads(service_state.optimization_template)

            new_task = (
                {"key": "initial_opt"},
                gridopt_orm.initial_molecule_obj.to_model(Molecule),
                OptimizationProcedureSpecification(**new_opt_spec),
            )

            added_ids = self.submit_subtasks(session, service_orm, [new_task])

            # Add to the association table for the grid opt ORM
            opt_assoc = GridOptimizationAssociation()
            opt_assoc.opt_id = added_ids[0]
            opt_assoc.grid_opt_id = gridopt_orm.id
            opt_assoc.key = serialize_key("preoptimization")

            gridopt_orm.grid_optimizations_obj.append(opt_assoc)

            service_state.iteration = -1
            finished = False

        elif service_state.iteration == -1:

            complete_tasks = service_orm.tasks_obj

            if len(complete_tasks) != 1:
                raise RuntimeError(f"Expected one complete task for preoptimization, but got {len(complete_tasks)}")

            starting_molecule = complete_tasks[0].procedure_obj.final_molecule_obj.to_model(Molecule)

            # Assign the true starting molecule and grid to the grid optimization record
            gridopt_orm.starting_molecule = starting_molecule.id
            gridopt_orm.starting_grid = calculate_starting_grid(keywords.scans, starting_molecule)

            opt_key = serialize_key(gridopt_orm.starting_grid)
            self.submit_optimization_subtasks(session, service_state, service_orm, {opt_key: starting_molecule})

            # Skips the normal 0th iteration
            service_state.iteration = 1

            finished = False

        # Special start iteration
        elif service_state.iteration == 0:

            # Remember we set starting_molecule to initial_molecule
            starting_molecule = gridopt_orm.starting_molecule_obj.to_model(Molecule)
            opt_key = serialize_key(gridopt_orm.starting_grid)
            self.submit_optimization_subtasks(session, service_state, service_orm, {opt_key: starting_molecule})

            service_state.iteration = 1

            finished = False

        else:
            # Obtain complete tasks and figure out future tasks
            complete_tasks = service_orm.tasks_obj

            # Maps keys to Molecule (for the next iteration)
            molecule_map = {}

            for task in complete_tasks:
                key = task.extras["key"]
                gridopt_orm.final_energy_dict[key] = task.procedure_obj.energies[-1]

                molecule_map[key] = task.procedure_obj.final_molecule_obj.to_model(Molecule)

            # Build out the new set of seeds
            complete_seeds = set(deserialize_key(k.extras["key"]) for k in complete_tasks)

            # Store what we have already completed
            service_state.complete = set(service_state.complete) | complete_seeds

            # Compute new points
            new_points_list = expand_ndimensional_grid(service_state.dimensions, complete_seeds, service_state.complete)

            next_tasks = {}
            for new_points in new_points_list:
                old = serialize_key(new_points[0])
                new = serialize_key(new_points[1])

                next_tasks[new] = molecule_map[old]

            # If no tasks are left, we are all done
            if len(next_tasks) == 0:
                gridopt_orm.status = RecordStatusEnum.complete
                finished = True
            else:
                self.submit_optimization_subtasks(session, service_state, service_orm, next_tasks)
                finished = False

        # Set the new service state. We must then mark it as modified
        # so that SQLAlchemy can pick up changes. This is because SQLAlchemy
        # cannot track mutations in nested dicts
        service_orm.service_state = service_state.dict()
        sqlalchemy.orm.attributes.flag_modified(service_orm, "service_state")

        # Also mark the final energy dict as being changed
        sqlalchemy.orm.attributes.flag_modified(gridopt_orm, "final_energy_dict")

        # Return True to indicate that this service has successfully completed
        return finished

    def submit_optimization_subtasks(
        self,
        session: Session,
        service_state: GridOptimizationServiceState,
        gridopt_orm: ServiceQueueORM,
        task_dict: Dict[str, Molecule],
    ):
        new_tasks = []

        starting_molecule = gridopt_orm.procedure_obj.starting_molecule_obj.to_model(Molecule)

        for key, mol in task_dict.items():
            # Create an optimization input based on the new geometry and the optimization template
            new_opt_spec = json.loads(service_state.optimization_template)

            # Construct constraints
            constraints = json.loads(service_state.constraint_template)

            scan_indices = deserialize_key(key)

            for con_num, scan in enumerate(gridopt_orm.procedure_obj.keywords["scans"]):
                idx = scan_indices[con_num]
                if scan["step_type"] == "absolute":
                    constraints[con_num]["value"] = scan["steps"][idx]
                else:
                    constraints[con_num]["value"] = scan["steps"][idx] + starting_molecule.measure(scan["indices"])

            # update the constraints
            new_opt_spec["keywords"].setdefault("constraints", {})
            new_opt_spec["keywords"]["constraints"].setdefault("set", [])
            new_opt_spec["keywords"]["constraints"]["set"].extend(constraints)

            new_tasks.append(
                (
                    {"key": key},
                    mol,
                    OptimizationProcedureSpecification(**new_opt_spec),
                )
            )

        added_ids = self.submit_subtasks(session, gridopt_orm, new_tasks)

        # Update association
        for id, (task_info, _, _) in zip(added_ids, new_tasks):
            key = task_info["key"]

            opt_assoc = GridOptimizationAssociation()
            opt_assoc.opt_id = id
            opt_assoc.grid_opt_id = gridopt_orm.procedure_obj.id
            opt_assoc.key = key

            gridopt_orm.procedure_obj.grid_optimizations_obj.append(opt_assoc)
