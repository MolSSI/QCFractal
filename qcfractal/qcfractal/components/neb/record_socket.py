from __future__ import annotations

import importlib
import json
import logging
from typing import TYPE_CHECKING

import numpy as np
import sqlalchemy.orm.attributes
import tabulate

try:
    from pydantic.v1 import BaseModel, Extra
except ImportError:
    from pydantic import BaseModel, Extra

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert, array_agg, aggregate_order_by, DOUBLE_PRECISION, TEXT
from sqlalchemy.orm import lazyload, joinedload, defer, undefer

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM, SinglepointRecordORM
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.neb import (
    NEBSpecification,
    NEBQueryFilters,
)
from qcportal.optimization import OptimizationSpecification
from qcportal.record_models import PriorityEnum, RecordStatusEnum, OutputTypeEnum
from qcportal.serialization import convert_numpy_recursive
from qcportal.singlepoint import QCSpecification
from qcportal.utils import capture_all_output
from qcportal.utils import hash_dict
from .record_db_models import (
    NEBOptimizationsORM,
    NEBSpecificationORM,
    NEBSinglepointsORM,
    NEBInitialchainORM,
    NEBRecordORM,
)
from ..record_socket import BaseRecordSocket

# geometric package is optional
_geo_spec = importlib.util.find_spec("geometric")

if _geo_spec is not None:
    geometric = importlib.util.module_from_spec(_geo_spec)
    _geo_spec.loader.exec_module(geometric)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union, Iterable

# Meaningless, but unique to neb
neb_insert_lock_id = 14600
neb_spec_insert_lock_id = 14601


class NEBServiceState(BaseModel):
    """
    This represents the current state of a NEB service
    """

    class Config(BaseModel.Config):
        extra = Extra.forbid
        allow_mutation = True
        validate_assignment = True

    # These are stored as JSON (ie, dict encoded into a string)
    # This makes for faster loads and makes them somewhat tamper-proof

    nebinfo: dict
    keywords: dict
    optimized: bool
    tsoptimize: bool
    converged: bool
    iteration: int
    molecule_template: str
    tshessian: list


class NEBRecordSocket(BaseRecordSocket):
    # Used by the base class
    record_orm = NEBRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    def available(self) -> bool:
        return _geo_spec is not None

    @staticmethod
    def get_children_select() -> List[Any]:
        stmt = [
            select(
                NEBSinglepointsORM.neb_id.label("parent_id"),
                NEBSinglepointsORM.singlepoint_id.label("child_id"),
            ),
            select(
                NEBOptimizationsORM.neb_id.label("parent_id"),
                NEBOptimizationsORM.optimization_id.label("child_id"),
            ),
        ]
        return stmt

    def initialize_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ):
        neb_orm: NEBRecordORM = service_orm.record
        output = "\n\nCreated NEB calculation\n"
        spec: NEBSpecification = neb_orm.specification.to_model(NEBSpecification)
        keywords = spec.keywords.dict()
        table_rows = sorted(keywords.items())
        output += tabulate.tabulate(table_rows, headers=["NEB keywords", "value"])
        output += "\n\n"
        table_rows = sorted(spec.singlepoint_specification.dict().items())
        output += tabulate.tabulate(table_rows, headers=["NEB QC keywords", "value"])
        output += "\n\n"
        if bool(spec.optimization_specification):
            table_rows = sorted(spec.optimization_specification.qc_specification.dict().items())
            output += tabulate.tabulate(table_rows, headers=["OPT QC keywords", "value"])
        output += "\n\n"

        initial_chain: List[Dict[str, Any]] = [x.molecule.model_dict() for x in neb_orm.initial_chain]
        output += f"{keywords.get('images', 11)} images will be used to guess a transition state structure.\n"
        output += f"Molecular formula = {Molecule(**initial_chain[0]).get_molecular_formula()}\n"
        molecule_template = Molecule(**initial_chain[0]).dict(encoding="json")

        molecule_template.pop("geometry", None)
        molecule_template.pop("identifiers", None)
        molecule_template.pop("id", None)

        self.root_socket.records.append_output(session, neb_orm, OutputTypeEnum.stdout, output)

        molecule_template_str = json.dumps(molecule_template)
        service_state = NEBServiceState(
            nebinfo={
                "elems": molecule_template.get("symbols"),
                "charge": molecule_template.get("molecular_charge", 0),
                "mult": molecule_template.get("molecular_multiplicity", 1),
            },
            iteration=0,
            keywords=keywords,
            optimized=keywords.get("optimize_endpoints"),
            tsoptimize=keywords.get("optimize_ts"),
            converged=False,
            tshessian=[],
            molecule_template=molecule_template_str,
        )

        service_orm.service_state = service_state.dict()
        sqlalchemy.orm.attributes.flag_modified(service_orm, "service_state")

    def iterate_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ) -> bool:
        finished = False

        neb_orm: NEBRecordORM = service_orm.record
        # Always update with the current provenance
        neb_orm.compute_history[-1].provenance = {
            "creator": "neb",
            "version": geometric.__version__,
            "routine": "qcfractal.services.neb",
        }
        # Load the state from the service_state column
        spec: NEBSpecification = neb_orm.specification.to_model(NEBSpecification)
        params = spec.keywords.dict()
        service_state = NEBServiceState(**service_orm.service_state)
        molecule_template = json.loads(service_state.molecule_template)

        params["iteration"] = service_state.iteration
        output = ""
        if service_state.iteration == 0:
            service_state.converged = False
            initial_molecules = [x.molecule.to_model(Molecule) for x in neb_orm.initial_chain]

            if service_state.optimized:
                # First iteration, but we have been told to optimize the endpoints
                output += "\nFirst, optimizing the end points"
                self.submit_optimizations(session, service_orm, [initial_molecules[0], initial_molecules[-1]])
                service_state.optimized = False
            else:
                # Either no endpoint optimizations, or they have been done
                complete_opts = sorted(service_orm.dependencies, key=lambda x: x.extras["position"])

                # If there are completed opts, replace the endpoints with the optimized molecules
                if len(complete_opts) != 0:
                    if len(complete_opts) != 2:
                        raise RuntimeError(f"Expected 2 completed optimizations, got {len(complete_opts)}")

                    initial_molecules[0] = complete_opts[0].record.final_molecule.to_model(Molecule)
                    initial_molecules[-1] = complete_opts[-1].record.final_molecule.to_model(Molecule)

                with capture_all_output("geometric.nifty") as (rdout, _):
                    respaced_chain = geometric.qcf_neb.arrange(initial_molecules)

                output += "\n" + rdout.getvalue()

                # Submit the first batch of singlepoint calculations
                self.submit_singlepoints(session, service_state, service_orm, respaced_chain)
                service_state.iteration += 1

        else:
            if not service_state.converged:
                # Returned task a nextchain computation
                if service_orm.dependencies and service_orm.dependencies[0].record.record_type == "servicesubtask":
                    newcoords, prev = service_orm.dependencies[0].record.results
                    service_state.nebinfo = prev

                    # Append the output
                    stdout = self.root_socket.records.service_subtask.get_single_output_uncompressed(
                        service_orm.dependencies[0].record.id,
                        service_orm.dependencies[0].record.compute_history[-1].id,
                        OutputTypeEnum.stdout,
                    )

                    output += stdout

                    # Delete the subtask - no longer needed
                    to_delete = service_orm.dependencies.pop()
                    self.root_socket.records.delete([to_delete.record_id], soft_delete=False, session=session)

                    # If nextchain returns None, then we are now converged
                    if newcoords is None:
                        service_state.converged = True
                    else:
                        next_chain = [Molecule(**molecule_template, geometry=geometry) for geometry in newcoords]
                        self.submit_singlepoints(session, service_state, service_orm, next_chain)
                        service_state.iteration += 1

                else:
                    complete_tasks = sorted(service_orm.dependencies, key=lambda x: x.extras["position"])
                    geometries = []
                    energies = []
                    gradients = []
                    for task in complete_tasks:
                        sp_record = task.record
                        mol_data = self.root_socket.molecules.get(
                            molecule_id=[sp_record.molecule_id], include=["geometry"], session=session
                        )
                        geometries.append(mol_data[0]["geometry"])
                        energies.append(sp_record.properties["return_energy"])
                        gradients.append(convert_numpy_recursive(sp_record.properties["return_result"], flatten=True))
                    service_state.nebinfo["geometry"] = convert_numpy_recursive(geometries, flatten=False)
                    service_state.nebinfo["energies"] = energies
                    service_state.nebinfo["gradients"] = gradients
                    service_state.nebinfo["params"] = params

                    with capture_all_output("geometric.nifty") as (rdout, _):
                        if service_state.iteration == 1:
                            newcoords, prev = geometric.qcf_neb.prepare(service_state.nebinfo)
                            service_state.nebinfo = prev

                            next_chain = [Molecule(**molecule_template, geometry=geometry) for geometry in newcoords]
                            self.submit_singlepoints(session, service_state, service_orm, next_chain)
                            service_state.iteration += 1
                        else:
                            self.submit_nextchain_subtask(session, service_state, service_orm)

                    output += "\n" + rdout.getvalue()

            # We are converged, but need to handle TS optimization
            if service_state.converged and service_state.tsoptimize:
                stmt = (
                    select(MoleculeORM)
                    .join(SinglepointRecordORM)
                    .join(NEBSinglepointsORM)
                    .where(NEBSinglepointsORM.neb_id == neb_orm.id)
                    .order_by(
                        NEBSinglepointsORM.chain_iteration.desc(),
                        SinglepointRecordORM.properties["return_energy"].cast(TEXT).cast(DOUBLE_PRECISION).desc(),
                    )
                    .limit(1)
                )

                TS_mol = session.execute(stmt).scalar_one_or_none()
                if TS_mol is None:
                    raise MissingDataError("MoleculeORM of a guessed transition state from NEB can't be found.")

                # Has the TS hessian calculation been completed?
                if len(service_state.tshessian) == 0:
                    output += (
                        "\nOptimizing the guessed transition state structure to locate a first-order saddle point.\n"
                        "Hessian will be calculated and passed to geomeTRIC."
                    )

                    self.submit_singlepoints(session, service_state, service_orm, [Molecule(**TS_mol.model_dict())])

                    # Mark that we are expecting the tshessian the next iteration
                    service_state.tshessian = [1]
                else:
                    # Hessian completed, do optimization
                    complete_sp = service_orm.dependencies[0]
                    service_orm.service_state["tshessian"] = complete_sp.record.properties["return_hessian"]
                    self.submit_optimizations(session, service_orm, [Molecule(**TS_mol.model_dict())])
                    service_state.tsoptimize = False

            elif service_state.converged:
                # Converged, but not waiting for anything else
                finished = True
                output += "\nNEB calculation is completed with %i iterations" % service_state.iteration

        self.root_socket.records.append_output(session, neb_orm, OutputTypeEnum.stdout, output)
        service_orm.service_state = service_state.dict()
        sqlalchemy.orm.attributes.flag_modified(service_orm, "service_state")
        return finished

    def submit_optimizations(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
        molecules: List[Molecule],
    ):
        neb_orm: NEBRecordORM = service_orm.record
        # delete all existing entries in the dependency list
        service_orm.dependencies = []
        service_state = NEBServiceState(**service_orm.service_state)
        neb_spec = neb_orm.specification.to_model(NEBSpecification)
        has_optimization = bool(neb_spec.optimization_specification)
        qc_spec = neb_orm.specification.singlepoint_specification.model_dict()
        if service_state.tsoptimize and service_state.converged:
            if has_optimization:
                opt_spec = neb_orm.specification.optimization_specification.model_dict()
                opt_spec["keywords"]["hessian"] = service_state.tshessian
                opt_spec["keywords"]["transition"] = True
                opt_spec["program"] = "geometric"
                opt_spec = OptimizationSpecification(
                    **opt_spec
                )  # neb_orm.specification.optimization_specification.to_model(OptimizationSpecification)

            else:
                opt_spec = OptimizationSpecification(
                    program="geometric",
                    qc_specification=QCSpecification(**qc_spec),
                    keywords={
                        "transition": True,
                        "coordsys": "tric",
                        "hessian": service_state.tshessian,
                    },
                )
            ts = True
        else:
            opt_spec = OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(**qc_spec),
                keywords={"coordsys": "tric"},
            )
            ts = False

        meta, opt_ids = self.root_socket.records.optimization.add(
            molecules,
            opt_spec,
            service_orm.tag,
            service_orm.priority,
            neb_orm.owner_user_id,
            neb_orm.owner_group_id,
            service_orm.find_existing,
            session=session,
        )
        for pos, opt_id in enumerate(opt_ids):
            svc_dep = ServiceDependencyORM(record_id=opt_id, extras={"position": pos})
            opt_history = NEBOptimizationsORM(
                neb_id=service_orm.record_id,
                optimization_id=opt_id,
                position=pos,
                ts=ts,
            )
            service_orm.dependencies.append(svc_dep)
            neb_orm.optimizations.append(opt_history)

    def submit_singlepoints(
        self,
        session: Session,
        service_state: NEBServiceState,
        service_orm: ServiceQueueORM,
        chain: List[Molecule],
    ):
        neb_orm: NEBRecordORM = service_orm.record
        # delete all existing entries in the dependency list
        service_orm.dependencies = []
        neb_spec = neb_orm.specification.to_model(NEBSpecification)
        has_optimization = bool(neb_spec.optimization_specification)

        # Create a singlepoint input based on the multiple geometries
        qc_spec = neb_orm.specification.singlepoint_specification.model_dict()
        qc_spec["driver"] = "gradient"

        if service_state.converged and service_state.tsoptimize and len(service_state.tshessian) == 0:
            if has_optimization:
                opt_spec = neb_orm.specification.optimization_specification.model_dict()
                qc_spec = opt_spec["qc_specification"]
            qc_spec["driver"] = "hessian"

        meta, sp_ids = self.root_socket.records.singlepoint.add(
            chain,
            QCSpecification(**qc_spec),
            service_orm.tag,
            service_orm.priority,
            neb_orm.owner_user_id,
            neb_orm.owner_group_id,
            service_orm.find_existing,
            session=session,
        )

        if not meta.success:
            raise RuntimeError("Error adding singlepoints - likely a developer error: " + meta.error_string)

        for pos, sp_id in enumerate(sp_ids):
            svc_dep = ServiceDependencyORM(
                record_id=sp_id,
                extras={"position": pos},
            )
            sp_history = NEBSinglepointsORM(
                neb_id=service_orm.record_id,
                singlepoint_id=sp_id,
                chain_iteration=service_state.iteration,
                position=pos,
            )

            service_orm.dependencies.append(svc_dep)
            neb_orm.singlepoints.append(sp_history)

    def submit_nextchain_subtask(
        self,
        session: Session,
        service_state: NEBServiceState,
        service_orm: ServiceQueueORM,
    ):
        neb_orm: NEBRecordORM = service_orm.record

        # delete all existing entries in the dependency list
        service_orm.dependencies = []

        meta, ids = self.root_socket.records.service_subtask.add(
            {"geometric": None},
            "geometric.qcf_neb.nextchain",
            [{"info_dict": service_state.nebinfo}],
            service_orm.tag,
            service_orm.priority,
            neb_orm.owner_user_id,
            neb_orm.owner_group_id,
            session=session,
        )

        svc_dep = ServiceDependencyORM(
            record_id=ids[0],
            extras={},
        )

        service_orm.dependencies.append(svc_dep)

    def add_specification(
        self, neb_spec: NEBSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        neb_kw_dict = neb_spec.keywords.dict()
        kw_hash = hash_dict(neb_kw_dict)

        with self.root_socket.optional_session(session, False) as session:
            # Add the singlepoint specification
            meta, sp_spec_id = self.root_socket.records.singlepoint.add_specification(
                neb_spec.singlepoint_specification, session=session
            )
            if not meta.success:
                return (
                    InsertMetadata(
                        error_description="Unable to add singlepoint specification: " + meta.error_string,
                    ),
                    None,
                )
            # Add the optimization specification
            opt_spec_id = None
            if neb_spec.optimization_specification is not None:
                meta, opt_spec_id = self.root_socket.records.optimization.add_specification(
                    neb_spec.optimization_specification, session=session
                )
                if not meta.success:
                    return (
                        InsertMetadata(
                            error_description="Unable to add optimization specification: " + meta.error_string,
                        ),
                        None,
                    )

            # Lock for the rest of the transaction (since we have to query then add)
            session.execute(select(func.pg_advisory_xact_lock(neb_spec_insert_lock_id))).scalar()

            stmt = select(NEBSpecificationORM.id).filter_by(
                program=neb_spec.program,
                keywords_hash=kw_hash,
                singlepoint_specification_id=sp_spec_id,
                optimization_specification_id=opt_spec_id,
            )

            if opt_spec_id is not None:
                stmt = stmt.filter(NEBSpecificationORM.optimization_specification_id == opt_spec_id)
            else:
                stmt = stmt.filter(NEBSpecificationORM.optimization_specification_id.is_(None))

            r = session.execute(stmt).scalar_one_or_none()

            if r is not None:
                return InsertMetadata(existing_idx=[0]), r
            else:
                # Specification did not already exist
                stmt = (
                    insert(NEBSpecificationORM)
                    .values(
                        program=neb_spec.program,
                        keywords=neb_kw_dict,
                        keywords_hash=kw_hash,
                        singlepoint_specification_id=sp_spec_id,
                        optimization_specification_id=opt_spec_id,
                    )
                    .returning(NEBSpecificationORM.id)
                )

                r = session.execute(stmt).scalar_one()
                return InsertMetadata(inserted_idx=[0]), r

    def query(
        self,
        query_data: NEBQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        Query neb records

        Parameters
        ----------
        query_data
            Fields/filters to query for
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of record ids that were found in the database.
        """

        and_query = []
        need_spspec_join = False
        need_initchain_join = False
        need_nebspec_join = False

        if query_data.qc_program is not None:
            and_query.append(QCSpecificationORM.program.in_(query_data.qc_program))
            need_spspec_join = True
        if query_data.qc_method is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.qc_method))
            need_spspec_join = True
        if query_data.qc_basis is not None:
            and_query.append(QCSpecificationORM.basis.in_(query_data.qc_basis))
            need_spspec_join = True
        if query_data.program is not None:
            and_query.append(NEBSpecificationORM.program.in_(query_data.program))
            need_nebspec_join = True
        if query_data.molecule_id is not None:
            and_query.append(NEBInitialchainORM.molecule_id.in_(query_data.molecule_id))
            need_initchain_join = True

        stmt = select(NEBRecordORM.id)

        if need_nebspec_join or need_spspec_join:
            stmt = stmt.join(NEBRecordORM.specification)

        if need_spspec_join:
            stmt = stmt.join(NEBSpecificationORM.singlepoint_specification)

        if need_initchain_join:
            stmt = stmt.join(
                NEBInitialchainORM,
                NEBInitialchainORM.neb_id == NEBRecordORM.id,
            )

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=NEBRecordORM,
            query_data=query_data,
            session=session,
        )

    def add_internal(
        self,
        initial_chain_ids: Sequence[Sequence[int]],
        neb_spec_id: int,
        tag: str,
        priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Internal function for adding new NEB calculations

        This function expects that the chains and specifications are already added to the
        database and that the ids are known.

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        initial_chain_ids
            IDs of the chains to optimize. One record will be added per chain.
        neb_spec_id
            ID of the specification
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority for the computation
        owner_user_id
            ID of the user who owns the record
        owner_group_id
            ID of the group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input chains
        """
        tag = tag.lower()

        with self.root_socket.optional_session(session, False) as session:
            self.root_socket.users.assert_group_member(owner_user_id, owner_group_id, session=session)

            # Lock for the entire transaction
            session.execute(select(func.pg_advisory_xact_lock(neb_insert_lock_id))).scalar()

            neb_ids = []
            inserted_idx = []
            existing_idx = []

            if find_existing:
                init_mol_cte = (
                    select(
                        NEBRecordORM.id,
                        NEBRecordORM.specification_id,
                        array_agg(
                            aggregate_order_by(
                                NEBInitialchainORM.molecule_id,
                                NEBInitialchainORM.position.asc(),
                            )
                        ).label("molecule_ids"),
                    )
                    .join(
                        NEBInitialchainORM,
                        NEBInitialchainORM.neb_id == NEBRecordORM.id,
                    )
                    .group_by(NEBRecordORM.id)
                    .cte()
                )

                for idx, mol_ids in enumerate(initial_chain_ids):
                    # does this exist?
                    stmt = select(init_mol_cte.c.id)
                    stmt = stmt.where(init_mol_cte.c.specification_id == neb_spec_id)
                    stmt = stmt.where(init_mol_cte.c.molecule_ids == mol_ids)
                    existing = session.execute(stmt).scalars().first()

                    if not existing:
                        neb_orm = NEBRecordORM(
                            is_service=True,
                            specification_id=neb_spec_id,
                            status=RecordStatusEnum.waiting,
                            owner_user_id=owner_user_id,
                            owner_group_id=owner_group_id,
                        )

                        self.create_service(neb_orm, tag, priority, find_existing)

                        session.add(neb_orm)
                        session.flush()

                        for pos, mid in enumerate(mol_ids):
                            mid_orm = NEBInitialchainORM(molecule_id=mid, neb_id=neb_orm.id, position=pos)
                            session.add(mid_orm)

                        session.flush()

                        neb_ids.append(neb_orm.id)
                        inserted_idx.append(idx)
                    else:
                        neb_ids.append(existing)
                        existing_idx.append(idx)
            else:
                for idx, mol_ids in enumerate(initial_chain_ids):
                    neb_orm = NEBRecordORM(
                        is_service=True,
                        specification_id=neb_spec_id,
                        status=RecordStatusEnum.waiting,
                        owner_user_id=owner_user_id,
                        owner_group_id=owner_group_id,
                    )

                    self.create_service(neb_orm, tag, priority, find_existing)

                    session.add(neb_orm)
                    session.flush()

                    for pos, mid in enumerate(mol_ids):
                        mid_orm = NEBInitialchainORM(molecule_id=mid, neb_id=neb_orm.id, position=pos)
                        session.add(mid_orm)

                    session.flush()

                    neb_ids.append(neb_orm.id)
                    inserted_idx.append(idx)

            meta = InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)
            return meta, neb_ids

    def add(
        self,
        initial_chains: Sequence[Sequence[Union[int, Molecule]]],
        neb_spec: NEBSpecification,
        tag: str,
        priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new neb calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to the database, but the session is flushed.

        Parameters
        ----------
        initial_chains
            Molecules to compute using the specification
        neb_spec
            Specification for the NEB calculations
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority for the computation
        owner_user
            Name or ID of the user who owns the record
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """
        images = neb_spec.keywords.images
        with self.root_socket.optional_session(session, False) as session:
            owner_user_id, owner_group_id = self.root_socket.users.get_owner_ids(
                owner_user, owner_group, session=session
            )

            # First, add the specification
            spec_meta, spec_id = self.add_specification(neb_spec, session=session)
            if not spec_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add specification: " + spec_meta.error_string
                    ),
                    [],
                )

            # Now the molecules
            init_molecule_ids = []
            for init_chain in initial_chains:
                if len(init_chain) < images:
                    return (
                        InsertMetadata(
                            error_description="Aborted - number of images for NEB can not exceed the number of input frames:"
                            + spec_meta.error_string
                        ),
                        [],
                    )
                selected_chain = np.array(init_chain)[
                    np.array([int(round(i)) for i in np.linspace(0, len(init_chain) - 1, images)])
                ]

                mol_meta, molecule_ids = self.root_socket.molecules.add_mixed(selected_chain, session=session)
                if not mol_meta.success:
                    return (
                        InsertMetadata(
                            error_description="Aborted - could not add all molecules: " + mol_meta.error_string
                        ),
                        [],
                    )

                init_molecule_ids.append(molecule_ids)

            return self.add_internal(
                init_molecule_ids, spec_id, tag, priority, owner_user_id, owner_group_id, find_existing, session=session
            )

    ####################################################
    # Some stuff to be retrieved for NEB records
    ####################################################

    def get_initial_molecules_ids(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(NEBRecordORM.initial_chain).options(undefer(NEBInitialchainORM.molecule_id)),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(NEBRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return [x.molecule_id for x in rec.initial_chain]

    def get_singlepoints(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        options = [lazyload("*"), defer("*"), joinedload(NEBRecordORM.singlepoints).options(undefer("*"))]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(NEBRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return [x.model_dict() for x in rec.singlepoints]

    def get_optimizations(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Dict[str, Any]]:
        options = [lazyload("*"), defer("*"), joinedload(NEBRecordORM.optimizations).options(undefer("*"))]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(NEBRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")

            ret = {}

            for opt in rec.optimizations:
                if opt.ts:
                    ret["transition"] = opt.model_dict()
                elif opt.position == 0:
                    ret["initial"] = opt.model_dict()
                else:
                    ret["final"] = opt.model_dict()

            return ret

    def get_neb_result(
        self,
        neb_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        stmt = (
            select(MoleculeORM)
            .join(SinglepointRecordORM)
            .join(NEBSinglepointsORM)
            .where(NEBSinglepointsORM.neb_id == neb_id)
            .order_by(
                NEBSinglepointsORM.chain_iteration.desc(),
                SinglepointRecordORM.properties["return_energy"].cast(TEXT).cast(DOUBLE_PRECISION).desc(),
            )
            .limit(1)
        )

        with self.root_socket.optional_session(session, True) as session:
            r = session.execute(stmt).scalar_one_or_none()
            if r is None:
                raise MissingDataError("The final guessed transition state can't be found")
            return r.model_dict()
