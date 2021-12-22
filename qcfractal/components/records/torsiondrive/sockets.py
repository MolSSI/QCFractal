from __future__ import annotations

import contextlib
import io
import json
import logging
from importlib.util import find_spec
from typing import TYPE_CHECKING

import sqlalchemy.orm.attributes
from pydantic import BaseModel
from qcelemental.models import OptimizationResult
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert, array_agg, aggregate_order_by
from sqlalchemy.orm import contains_eager
from qcfractal.portal.outputstore import OutputTypeEnum, OutputStore

from qcfractal.components.records.singlepoint.db_models import SinglepointRecordORM, SinglepointSpecificationORM
from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependenciesORM
from qcfractal.db_socket.helpers import get_general, get_general_multi
from qcfractal.portal.metadata_models import InsertMetadata, QueryMetadata
from qcfractal.portal.molecules import Molecule
from qcfractal.portal.records import PriorityEnum, RecordStatusEnum
from qcfractal.portal.records.optimization import OptimizationQueryBody, OptimizationInputSpecification
from qcfractal.portal.records.torsiondrive import (
    TorsiondriveSpecification,
    TorsiondriveInputSpecification,
)
from .db_models import (
    TorsiondriveSpecificationORM,
    TorsiondriveInitialMoleculeORM,
    TorsiondriveOptimizationHistoryORM,
    TorsiondriveRecordORM,
)

# Torsiondrive package is optional
__td_spec = find_spec("torsiondrive")

if __td_spec is not None:
    __td_api_spec = find_spec("torsiondrive.td_api")

    torsiondrive = __td_spec.loader.load_module()
    td_api = __td_api_spec.loader.load_module()


def _check_td():
    if __td_spec is None:
        raise ModuleNotFoundError(
            "Unable to find the torsiondrive package, which must be installed to use the torsion drive service"
        )


if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union, Iterable

    TorsiondriveSpecificationDict = Dict[str, Any]
    TorsiondriveRecordDict = Dict[str, Any]


class TorsionDriveServiceState(BaseModel):
    """
    This represents the current state of a torsiondrive service
    """

    class Config(BaseModel.Config):
        allow_mutation = True
        validate_assignment = True

    torsiondrive_state = {}

    # These are stored as JSON (ie, dict encoded into a string)
    # This makes for faster loads and makes them somewhat tamper-proof
    molecule_template: str
    dihedral_template: str
    optimization_template: str


class TorsiondriveRecordSocket(BaseRecordSocket):
    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    def get_children_ids(self, session: Session, record_id: Iterable[int]) -> List[int]:
        stmt = select(TorsiondriveOptimizationHistoryORM.optimization_id)
        stmt = stmt.where(TorsiondriveOptimizationHistoryORM.torsiondrive_id.in_(record_id))
        return session.execute(stmt).scalars().all()

    def get_specification(
        self, id: int, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[TorsiondriveSpecificationDict]:
        """
        Obtain a specification with the specified ID

        If missing_ok is False, then any ids that are missing in the database will raise an exception.
        Otherwise, the returned id will be None

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
        id
            An id for a single point specification
        missing_ok
           If set to True, then missing keywords will be tolerated, and the returned list of
           keywords will contain None for the corresponding IDs that were not found.
        session
            n existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Specification information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the keywords were missing
        """

        with self.root_socket.optional_session(session, True) as session:
            return get_general(
                session, TorsiondriveSpecificationORM, TorsiondriveSpecificationORM.id, [id], None, None, missing_ok
            )[0]

    def add_specification(
        self, td_spec: TorsiondriveInputSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:

        # TODO - add protocols to models when we have something there
        # protocols_dict = td_spec.protocols.dict(exclude_defaults=True)
        protocols_dict = {}
        td_kw_dict = td_spec.keywords.dict(exclude_defaults=True)

        with self.root_socket.optional_session(session, False) as session:
            # Add the optimization specification
            meta, opt_spec_id = self.root_socket.records.optimization.add_specification(
                td_spec.optimization_specification, session=session
            )
            if not meta.success:
                return (
                    InsertMetadata(
                        error_description="Unable to add optimization specification: " + meta.error_string,
                    ),
                    None,
                )

            stmt = (
                insert(TorsiondriveSpecificationORM)
                .values(
                    program=td_spec.program,
                    keywords=td_kw_dict,
                    optimization_specification_id=opt_spec_id,
                )
                .on_conflict_do_nothing()
                .returning(TorsiondriveSpecificationORM.id)
            )

            r = session.execute(stmt).scalar_one_or_none()
            if r is not None:
                return InsertMetadata(inserted_idx=[0]), r
            else:
                # Specification was already existing
                stmt = select(TorsiondriveSpecificationORM.id).filter_by(
                    program=td_spec.program,
                    keywords=td_kw_dict,
                    optimization_specification_id=opt_spec_id,
                )

                r = session.execute(stmt).scalar_one()
                return InsertMetadata(existing_idx=[0]), r

    def get(
        self,
        record_id: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[TorsiondriveRecordDict]]:
        """
        Obtain a torsiondrive record with specified IDs

        The returned information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of results will be None.

        Parameters
        ----------
        record_id
            A list or other sequence of record IDs
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
            Records as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        return self.root_socket.records.get_base(
            TorsiondriveRecordORM, record_id, include, exclude, missing_ok, session=session
        )

    def get_optimization_history(
        self,
        record_id: int,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ):

        with self.root_socket.optional_session(session, True) as session:
            hist = get_general_multi(
                session,
                TorsiondriveOptimizationHistoryORM,
                TorsiondriveOptimizationHistoryORM.optimization_id,
                [record_id],
                include,
                exclude,
                missing_ok,
            )
            return sorted(hist[0], key=lambda x: x["position"])

    def query(
        self,
        query_data: OptimizationQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[TorsiondriveRecordDict]]:

        and_query = []
        need_spspec_join = False
        need_optspec_join = False

        if query_data.program is not None:
            and_query.append(OptimizationSpecificationORM.program.in_(query_data.program))
            need_optspec_join = True
        if query_data.singlepoint_program is not None:
            and_query.append(SinglepointSpecificationORM.program.in_(query_data.singlepoint_program))
            need_spspec_join = True
        if query_data.singlepoint_method is not None:
            and_query.append(SinglepointSpecificationORM.method.in_(query_data.singlepoint_method))
            need_spspec_join = True
        if query_data.singlepoint_basis is not None:
            and_query.append(SinglepointSpecificationORM.basis.in_(query_data.singlepoint_basis))
            need_spspec_join = True
        if query_data.singlepoint_keywords_id is not None:
            and_query.append(SinglepointSpecificationORM.keywords_id.in_(query_data.singlepoint_keywords_id))
            need_spspec_join = True
        if query_data.initial_molecule_id is not None:
            and_query.append(OptimizationRecordORM.initial_molecule_id.in_(query_data.initial_molecule_id))
        if query_data.final_molecule_id is not None:
            and_query.append(OptimizationRecordORM.final_molecule_id.in_(query_data.final_molecule_id))

        stmt = select(OptimizationRecordORM)

        # If we need the singlepoint spec, we also need the optimization spec
        if need_optspec_join or need_spspec_join:
            stmt = stmt.join(OptimizationRecordORM.specification).options(
                contains_eager(OptimizationRecordORM.specification)
            )

        if need_spspec_join:
            stmt = stmt.join(OptimizationSpecificationORM.singlepoint_specification).options(
                contains_eager(
                    OptimizationRecordORM.specification, OptimizationSpecificationORM.singlepoint_specification
                )
            )

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=OptimizationRecordORM,
            query_data=query_data,
            session=session,
        )

    def _create_state(self, td_orm: TorsiondriveRecordORM) -> TorsionDriveServiceState:

        specification = TorsiondriveSpecification(**td_orm.specification.dict())
        initial_molecules: List[Dict[str, Any]] = [x.dict() for x in td_orm.initial_molecules]

        # For this part, sort the initial molecule by hash
        # This makes everything a bit more deterministic (and helps with testing)
        initial_molecules.sort(key=lambda x: x["identifiers"]["molecule_hash"])

        keywords = specification.keywords

        # Create a template from the first initial molecule
        # we will assume they all have the same symbols, etc
        # TODO - can simplify this after removing numpy from db (ie, just copy initial_molecules[0])
        molecule_template = Molecule(**initial_molecules[0]).dict(encoding="json")
        molecule_template.pop("id", None)
        molecule_template.pop("identifiers", None)

        # The torsiondrive package uses print, so capture that using contextlib
        td_stdout = io.StringIO()
        with contextlib.redirect_stdout(td_stdout):
            td_state = td_api.create_initial_state(
                dihedrals=keywords.dihedrals,
                grid_spacing=keywords.grid_spacing,
                elements=molecule_template["symbols"],
                init_coords=[x["geometry"].tolist() for x in initial_molecules],
                dihedral_ranges=keywords.dihedral_ranges,
                energy_decrease_thresh=keywords.energy_decrease_thresh,
                energy_upper_limit=keywords.energy_upper_limit,
            )

        stdout = td_stdout.getvalue()

        # Build dihedral template. Just for convenience later
        dihedral_template = []
        for idx in keywords.dihedrals:
            tmp = {"type": "dihedral", "indices": idx}
            dihedral_template.append(tmp)

        dihedral_template_str = json.dumps(dihedral_template)
        molecule_template_str = json.dumps(molecule_template)

        opt_template = specification.optimization_specification.dict()

        # TODO - is there a better place to do this? as_input function on models? Some pydantic export magic?
        opt_template.pop("id")
        opt_template.pop("singlepoint_specification_id")
        opt_template["singlepoint_specification"].pop("id")
        opt_template["singlepoint_specification"].pop("keywords_id")
        opt_template_str = json.dumps(opt_template)

        if stdout:
            stdout_orm = td_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
            stdout_orm.append(stdout)

        return TorsionDriveServiceState(
            torsiondrive_state=td_state,
            dihedral_template=dihedral_template_str,
            molecule_template=molecule_template_str,
            optimization_template=opt_template_str,
        )

    def add(
        self,
        td_spec: TorsiondriveInputSpecification,
        initial_molecules: Sequence[Union[int, Molecule]],
        as_service: bool,
        tag: Optional[str] = None,
        priority: PriorityEnum = PriorityEnum.normal,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new torsiondrive calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        td_spec
            Specification for the calculations
        initial_molecules
            Molecules to compute using the specification
        as_service
            Whether this record should be run as a service or as a regular calculation
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        # tags should be lowercase
        if tag is not None:
            tag = tag.lower()

        with self.root_socket.optional_session(session, False) as session:

            # First, add the specification
            spec_meta, spec_id = self.add_specification(td_spec, session=session)
            if not spec_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add specification: " + spec_meta.error_description
                    ),
                    [],
                )

            # Now the molecules
            init_mol_ids = []
            for init_mol in initial_molecules:
                mol_meta, mol_ids = self.root_socket.molecules.add_mixed(init_mol, session=session)
                if not mol_meta.success:
                    return (
                        InsertMetadata(
                            error_description="Aborted - could not add all molecules: " + mol_meta.error_description
                        ),
                        [],
                    )

                init_mol_ids.append(mol_ids)

            # Load the spec as is from the db
            # May be different due to normalization, or because keywords were passed by
            # ID where we need the full keywords
            real_spec_dict = self.get_specification(spec_id, session=session)

            td_ids = []
            inserted_idx = []
            existing_idx = []

            # Torsiondrives are a bit more complicated because we have a many-to-many relationship
            # between torsiondrives and initial molecules. So skip the general insert
            # function and do this one at a time

            # Create a cte with the initial molecules we can query against
            # This is like a table, with the specification id and the initial molecule ids
            # as a postgres array (sorted)
            # We then use this to determine if there are duplicates
            init_mol_cte = (
                select(
                    TorsiondriveRecordORM.id,
                    TorsiondriveRecordORM.specification_id,
                    array_agg(
                        aggregate_order_by(
                            TorsiondriveInitialMoleculeORM.molecule_id, TorsiondriveInitialMoleculeORM.molecule_id.asc()
                        )
                    ).label("molecule_ids"),
                )
                .join(
                    TorsiondriveInitialMoleculeORM,
                    TorsiondriveInitialMoleculeORM.torsiondrive_id == TorsiondriveRecordORM.id,
                )
                .group_by(TorsiondriveRecordORM.id)
                .cte()
            )

            for idx, mol_ids in enumerate(init_mol_ids):
                # sort molecules by increasing ids, and remove duplicates
                mol_ids = sorted(set(mol_ids))

                # does this exist?
                stmt = select(init_mol_cte.c.id)
                stmt = stmt.where(init_mol_cte.c.specification_id == spec_id)
                stmt = stmt.where(init_mol_cte.c.molecule_ids == mol_ids)
                existing = session.execute(stmt).scalars().first()

                if not existing:
                    td_orm = TorsiondriveRecordORM(
                        is_service=as_service,
                        specification_id=spec_id,
                        status=RecordStatusEnum.waiting,
                    )

                    self.create_service(td_orm, tag, priority)

                    session.add(td_orm)
                    session.flush()

                    for mid in mol_ids:
                        mid_orm = TorsiondriveInitialMoleculeORM(molecule_id=mid, torsiondrive_id=td_orm.id)
                        session.add(mid_orm)

                    session.flush()

                    td_ids.append(td_orm.id)
                    inserted_idx.append(idx)
                else:
                    td_ids.append(existing)
                    existing_idx.append(idx)

            meta = InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)
            return meta, td_ids

    def iterate_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ):

        td_orm = service_orm.record

        if td_orm.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            # This is a programmer error
            raise RuntimeError(
                f"Torsiondrive {td_orm.id} (service {service_orm.id}) has status {td_orm.status} - cannot iterate!"
            )

        # Is this the first iteration?
        if td_orm.status == RecordStatusEnum.waiting:
            td_orm.status = RecordStatusEnum.running
            service_state = self._create_state(td_orm)

        else:
            # Load the state from the service_state column
            service_state = TorsionDriveServiceState(**service_orm.service_state)

        td_orm.compute_history[-1].provenance = {
            "creator": "torsiondrive",
            "version": torsiondrive.__version__,
            "routine": "torsiondrive.td_api",
        }

        # Sort by position
        # Fully sorting by the key is not important since that ends up being a key in the dict
        # All that matters is that position 1 for a particular key comes before position 2, etc
        complete_tasks = sorted(service_orm.dependencies, key=lambda x: x.extras["position"])

        # Populate task results needed by the torsiondrive package
        task_results = {}
        for task in complete_tasks:
            td_api_key = task.extras["td_api_key"]
            task_results.setdefault(td_api_key, [])

            # This is an ORM for an optimization
            opt_record = task.record

            # Lookup molecules
            initial_id = opt_record.initial_molecule_id
            final_id = opt_record.final_molecule_id
            mol_ids = [initial_id, final_id]
            mol_data = self.root_socket.molecules.get(molecule_id=mol_ids, include=["geometry"], session=session)

            # Use plain lists rather than numpy arrays
            initial_mol_geom = mol_data[0]["geometry"].tolist()
            final_mol_geom = mol_data[1]["geometry"].tolist()

            task_results[td_api_key].append((initial_mol_geom, final_mol_geom, opt_record.energies[-1]))

        # The torsiondrive package uses print, so capture that using contextlib
        td_stdout = io.StringIO()
        with contextlib.redirect_stdout(td_stdout):
            td_api.update_state(service_state.torsiondrive_state, task_results)
            next_tasks = td_api.next_jobs_from_state(service_state.torsiondrive_state, verbose=True)

        stdout_append = "\n" + td_stdout.getvalue()

        # If no tasks are left, we are all done
        if len(next_tasks) == 0:
            td_orm.status = RecordStatusEnum.complete
        else:
            self.submit_optimization_subtasks(session, service_state, service_orm, next_tasks)

        # Update the torsiondrive procedure itself
        min_positions = {}
        final_energy = {}
        for k, v in service_state.torsiondrive_state["grid_status"].items():
            energies = [x[2] for x in v]
            idx = energies.index(min(energies))
            key = json.dumps(td_api.grid_id_from_string(k))
            min_positions[key] = idx
            final_energy[key] = energies[idx]

        td_orm.minimum_positions = min_positions
        td_orm.final_energy_dict = final_energy

        # append to the existing stdout
        stdout_orm = td_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
        stdout_orm.append(stdout_append)

        # Set the new service state. We must then mark it as modified
        # so that SQLAlchemy can pick up changes. This is because SQLAlchemy
        # cannot track mutations in nested dicts
        service_orm.service_state = service_state.dict()
        sqlalchemy.orm.attributes.flag_modified(service_orm, "service_state")

        # Return True to indicate that this service has successfully completed
        return len(next_tasks) == 0

    def submit_optimization_subtasks(
        self,
        session: Session,
        service_state: TorsionDriveServiceState,
        service_orm: ServiceQueueORM,
        task_dict: Dict[str, Any],
    ):

        # delete all existing entries in the dependency list
        service_orm.dependencies = []

        for td_api_key, geometries in task_dict.items():
            for position, geometry in enumerate(geometries):

                # Create an optimization input based on the new geometry and the optimization template
                new_opt_spec = json.loads(service_state.optimization_template)

                # Construct constraints
                constraints = json.loads(service_state.dihedral_template)

                grid_id = td_api.grid_id_from_string(td_api_key)
                for con_num, k in enumerate(grid_id):
                    constraints[con_num]["value"] = k

                # update the constraints
                new_opt_spec["keywords"].setdefault("constraints", {})
                new_opt_spec["keywords"]["constraints"].setdefault("set", [])
                new_opt_spec["keywords"]["constraints"]["set"].extend(constraints)

                # Build new molecule
                mol = json.loads(service_state.molecule_template)
                mol["geometry"] = geometry

                # Submit the new optimization
                meta, opt_ids = self.root_socket.records.optimization.add(
                    OptimizationInputSpecification(**new_opt_spec),
                    [Molecule(**mol)],
                    service_orm.tag,
                    service_orm.priority,
                    session=session,
                )

                if not meta.success:
                    raise RuntimeError("Error adding optimization - likely a developer error: " + meta.error_string)

                svc_task = ServiceDependenciesORM(
                    record_id=opt_ids[0],
                    extras={"td_api_key": td_api_key, "position": position},
                )

                opt_key = json.dumps(td_api.grid_id_from_string(td_api_key))
                opt_history = TorsiondriveOptimizationHistoryORM(
                    torsiondrive_id=service_orm.record_id,
                    optimization_id=opt_ids[0],
                    key=opt_key,
                )

                service_orm.dependencies.append(svc_task)
                service_orm.record.optimization_history.append(opt_history)
