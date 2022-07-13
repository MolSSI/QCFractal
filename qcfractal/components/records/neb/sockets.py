from __future__ import annotations

import contextlib
import io
import json
import logging
import time

import numpy as np
from typing import TYPE_CHECKING


import geometric
import sqlalchemy.orm.attributes
import tabulate
from pydantic import BaseModel
from sqlalchemy import select, func, or_
from sqlalchemy.dialects.postgresql import insert, array_agg, aggregate_order_by, DOUBLE_PRECISION, TEXT
from sqlalchemy.orm import contains_eager

from qcfractal.components.records.singlepoint.db_models import QCSpecificationORM, SinglepointRecordORM
from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcfractal.db_socket.helpers import get_query_proj_options
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import InsertMetadata, QueryMetadata
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputTypeEnum
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.optimization import OptimizationSpecification
from qcportal.records.neb import (
    NEBSpecification,
    NEBQueryFilters,
)
from .db_models import (
    NEBOptimiationsORM,
    NEBSpecificationORM,
    NEBSinglepointsORM,
    NEBInitialchainORM,
    NEBRecordORM,
)
from ..optimization.db_models import OptimizationRecordORM
from ...molecules.db_models import MoleculeORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union, Iterable

    NEBSpecificationDict = Dict[str, Any]
    NEBRecordDict = Dict[str, Any]


class NEBServiceState(BaseModel):
    """
    This represents the current state of a NEB service
    """

    class Config(BaseModel.Config):
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




class NEBRecordSocket(BaseRecordSocket):

    # Used by the base class
    record_orm = NEBRecordORM
    specification_orm = QCSpecificationORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        stmt = select(
            NEBSinglepointsORM.neb_id.label("parent_id"),
            NEBSinglepointsORM.singlepoint_id.label("child_id"),
        )
        return [stmt]

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
        output += tabulate.tabulate(table_rows, headers=["keywords", "value"])
        output += "\n\n"
        table_rows = sorted(spec.singlepoint_specification.dict().items())
        output += tabulate.tabulate(table_rows, headers=["keywords", "value"])
        output += "\n\n"

        initial_chain: List[Dict[str, Any]] = [x.model_dict() for x in neb_orm.initial_chain]
        output += f"{keywords.get('images', 11)} images will be used to guess a transition state structure.\n"
        output += f"Molecular formula = {Molecule(**initial_chain[0]).get_molecular_formula()}\n"
        molecule_template = Molecule(**initial_chain[0]).dict(encoding="json")

        molecule_template.pop("geometry", None)
        molecule_template.pop("identifiers", None)
        molecule_template.pop("id", None)
        #elems = molecule_template.get("symbols")

        stdout_orm = neb_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
        stdout_orm.append(output)

        molecule_template_str = json.dumps(molecule_template)

        service_state = NEBServiceState(
            nebinfo={'elems': molecule_template.get("symbols"),
                  'charge': molecule_template.get("molecular_charge", 0),
                  'mult': molecule_template.get("molecular_multiplicity", 1)
                  },
            iteration=0,
            keywords=keywords,
            optimized=False,
            tsoptimize=keywords.get('optimize_ts'),
            converged=False,
            molecule_template=molecule_template_str,
        )

        service_orm.service_state = service_state.dict()
        sqlalchemy.orm.attributes.flag_modified(service_orm, "service_state")

    def iterate_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ) -> bool:
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


        params['iteration'] = service_state.iteration
        output = ''
        if service_state.iteration==0:
            initial_chain: List[Dict[str, Any]] = [x.model_dict() for x in neb_orm.initial_chain]
            initial_molecules = [Molecule(**M) for M in initial_chain]
            if not service_state.optimized:
                output += "\nFirst, optimizing the end points"
                self.submit_optimizations(session, service_orm, [initial_molecules[0], initial_molecules[-1]])
                service_state.optimized = True
                finished = False
            else:
                complete_opts = sorted(service_orm.dependencies, key=lambda x: x.extras["position"])
                initial_molecules[0] = Molecule(**complete_opts[0].record.final_molecule.model_dict())
                initial_molecules[-1] = Molecule(**complete_opts[-1].record.final_molecule.model_dict())
                self.submit_singlepoints(session, service_state, service_orm, initial_molecules)
                service_state.iteration += 1
                finished = False
        else:
            if not service_state.converged:
                complete_tasks = sorted(service_orm.dependencies, key=lambda x: x.extras["position"])
                geometries = []
                energies = []
                gradients = []
                for task in complete_tasks:
                    # This is an ORM for singlepoint calculations
                    sp_record = task.record
                    mol_data = self.root_socket.molecules.get(molecule_id=[sp_record.molecule_id], include=["geometry"], session=session)
                    geometries.append(mol_data[0]["geometry"])
                    energies.append(sp_record.properties["return_energy"])
                    gradients.append(sp_record.properties["return_gradient"])
                service_state.nebinfo['geometry']=geometries
                service_state.nebinfo['energies']=energies
                service_state.nebinfo['gradients']=gradients
                service_state.nebinfo['params']=params
                neb_stdout = io.StringIO()
                logging.captureWarnings(True)
                with contextlib.redirect_stdout(neb_stdout):
                    if service_state.iteration == 1:
                        newcoords, prev = geometric.neb.prepare(service_state.nebinfo)
                    else:
                        newcoords, prev = geometric.neb.nextchain(service_state.nebinfo)
                output += "\n" + neb_stdout.getvalue()
                logging.captureWarnings(False)
                service_state.nebinfo = prev
            else:
                newcoords = None
            if newcoords is not None:
                next_chain = [Molecule(**molecule_template, geometry=geometry) for geometry in newcoords]
                self.submit_singlepoints(session, service_state, service_orm, next_chain)
                service_state.iteration += 1
                finished = False
            else:
                if service_state.converged:
                    if service_state.tsoptimize:
                        output += "\nOptimizing the guessed transition state structure to locate a first-order saddle point."

                        stmt = (select(MoleculeORM)
                                .join(SinglepointRecordORM)
                                .join(NEBSinglepointsORM)
                                .where(NEBSinglepointsORM.neb_id == neb_orm.id)
                                .order_by(NEBSinglepointsORM.chain_iteration.desc(),
                                          SinglepointRecordORM.properties["return_energy"].cast(TEXT).cast(
                                              DOUBLE_PRECISION).desc())
                                .limit(1)
                                )

                        with self.root_socket.optional_session(session, True) as session:
                            TS_mol = session.execute(stmt).scalar_one_or_none()
                            if TS_mol is None:
                                raise MissingDataError(
                                    "MoleculeORM of a guessed transition state from NEB can't be found.")
                        self.submit_optimizations(session, service_orm, [Molecule(**TS_mol.model_dict())])
                        service_state.tsoptimize = False
                        finished = False
                    else:
                        output += "\nNEB calculation is completed with %i iterations" % service_state.iteration
                        finished = True
                else:
                    service_state.converged=True
                    finished = False

        stdout_orm = neb_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
        stdout_orm.append(output)
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
        qc_spec = neb_orm.specification.singlepoint_specification.model_dict()
        if service_state.tsoptimize and service_state.converged:
            opt_spec = OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(**qc_spec),
                keywords={'transition':True,
                          'coordsys': service_state.keywords['coordinate_system']},
            )
            ts = True
        else:
            opt_spec = OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(**qc_spec),
                keywords={'coordsys': service_state.keywords['coordinate_system']}
            )

            ts = False
        meta, opt_ids = self.root_socket.records.optimization.add(
            molecules,
            opt_spec,
            service_orm.tag,
            service_orm.priority,
            session=session,
        )
        for pos, opt_id in enumerate(opt_ids):
            svc_dep = ServiceDependencyORM(
                record_id=opt_id,
                extras={"position": pos})
            opt_history = NEBOptimiationsORM(
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

        # Create a singlepoint input based on the multiple geometries
        qc_spec = neb_orm.specification.singlepoint_specification.model_dict()
        meta, sp_ids = self.root_socket.records.singlepoint.add(
            chain,
            QCSpecification(**qc_spec),
            service_orm.tag,
            service_orm.priority,
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
                position= pos,
            )
            
            service_orm.dependencies.append(svc_dep)
            neb_orm.singlepoints.append(sp_history)

    def add_specification(
            self, neb_spec: NEBSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:

        neb_kw_dict = neb_spec.keywords.dict(exclude_defaults=True)

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

            stmt = (
                insert(NEBSpecificationORM)
                    .values(
                    program=neb_spec.program,
                    keywords=neb_kw_dict,
                    singlepoint_specification_id=sp_spec_id,
                )
                    .on_conflict_do_nothing()
                    .returning(NEBSpecificationORM.id)
            )

            r = session.execute(stmt).scalar_one_or_none()

            if r is not None:
                return InsertMetadata(inserted_idx=[0]), r
            else:
                # Specification was already existing
                stmt = select(NEBSpecificationORM.id).filter_by(
                    program=neb_spec.program,
                    keywords=neb_kw_dict,
                    singlepoint_specification_id=sp_spec_id,
                )

                r = session.execute(stmt).scalar_one()
                return InsertMetadata(existing_idx=[0]), r

    def query(
            self,
            query_data: NEBQueryFilters,
            *,
            session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[NEBRecordDict]]:

        and_query = []
        need_spspec_join = False
        need_nebspec_join = False
        need_initchain_join = False


        if query_data.qc_program is not None:
            and_query.append(QCSpecificationORM.program.in_(query_data.qc_program))
            need_spspec_join = True
        if query_data.qc_method is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.qc_method))
            need_spspec_join = True
        if query_data.qc_basis is not None:
            and_query.append(QCSpecificationORM.basis.in_(query_data.qc_basis))
            need_spspec_join = True
        if query_data.neb_program is not None:
            and_query.append('geometric')
            need_qcspec_join = True
        if query_data.initial_chain_id is not None:
            and_query.append(NEBInitialchainORM.neb_id.in_(query_data.initial_chain_id))
            need_initchain_join = True

        stmt = select(NEBRecordORM)

        if need_spspec_join:
            stmt = stmt.join(QCSpecificationORM.singlepoint_specification).options(
                contains_eager(
                    NEBRecordORM.specification,
                    NEBSpecificationORM.singlepoint_specification,
                    QCSpecificationORM.singlepoint_specification,
                )
            )

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
        initial_chain_ids: Sequence[Iterable[int]],
        neb_spec_id: int,
        tag: str,
        priority: PriorityEnum,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        tag = tag.lower()

        with self.root_socket.optional_session(session, False) as session:
            neb_ids = []
            inserted_idx = []
            existing_idx = []

            init_mol_cte = (
                select(
                    NEBRecordORM.id,
                    NEBRecordORM.specification_id,
                    array_agg(
                        aggregate_order_by(
                            NEBInitialchainORM.molecule_id, NEBInitialchainORM.position.asc(),
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
                    )

                    self.create_service(neb_orm, tag, priority)

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

            meta = InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)

            return meta, neb_ids

    def add(
        self,
        initial_chains: Sequence[Iterable[Union[Molecule]]],
        neb_spec: NEBSpecification,
        tag: str,
        priority: PriorityEnum,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new neb calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        initial_chains
            Molecules to compute using the specification
        neb_spec
            Specification for the calculations
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
                    return(
                        InsertMetadata(
                            error_description="Aborted - number of images for NEB can not exceed the number of input frames:" + spec_meta.error_string
                        ),
                        [],
                    )
                selected_chain = np.array(init_chain)[np.array([int(round(i)) for i in np.linspace(0, len(init_chain)-1 ,images)])]
                neb_stdout = io.StringIO()
                logging.captureWarnings(True)
                with contextlib.redirect_stdout(neb_stdout):
                    aligned_chain = geometric.neb.arrange(selected_chain)
                logging.captureWarnings(False)
                mol_meta, molecule_ids = self.root_socket.molecules.add_mixed(aligned_chain, session=session)
                if not mol_meta.success:
                    return (
                        InsertMetadata(
                            error_description="Aborted - could not add all molecules: " + mol_meta.error_string
                        ),
                        [],
                    )

                init_molecule_ids.append(molecule_ids)

            return self.add_internal(init_molecule_ids, spec_id, tag, priority, session=session)

    def get_neb_result(
        self,
        neb_id: int,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:

        query_mol = get_query_proj_options(MoleculeORM, include, exclude)

        stmt = (select(MoleculeORM)
                .options(*query_mol)
                .join(SinglepointRecordORM)
                .join(NEBSinglepointsORM)
                .where(NEBSinglepointsORM.neb_id == neb_id)
                .order_by(NEBSinglepointsORM.chain_iteration.desc(), SinglepointRecordORM.properties["return_energy"].cast(TEXT).cast(DOUBLE_PRECISION).desc())
                .limit(1)
                )

        with self.root_socket.optional_session(session, True) as session:
            r = session.execute(stmt).scalar_one_or_none()
            if r is None:
                raise MissingDataError("A guessed transition state from the NEB can't be found")
            return r.model_dict()