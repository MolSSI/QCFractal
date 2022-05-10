from __future__ import annotations

import contextlib
import copy
import io
import json
import logging
from importlib.util import find_spec
from typing import TYPE_CHECKING


from geometric.neb import nextchain
import numpy as np
import sqlalchemy.orm.attributes
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert, array_agg, aggregate_order_by
from sqlalchemy.orm import contains_eager

from qcfractal.components.records.singlepoint.db_models import QCSpecificationORM
from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcportal.metadata_models import InsertMetadata, QueryMetadata
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputTypeEnum
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.neb import (
    NEBSpecification,
    NEBQueryBody,
)
from .db_models import (
    NEBSpecificationORM,
    NEBSinglepointsORM,
    NEBInitialchainORM,
    NEBRecordORM,
)

    


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

    neb_state = {}

    # These are stored as JSON (ie, dict encoded into a string)
    # This makes for faster loads and makes them somewhat tamper-proof
    itretaion: int
    elems: list
    molecule_template: str
    


class NEBRecordSocket(BaseRecordSocket):

    # Used by the base class
    record_orm = NEBRecordORM
    specification_orm = NEBSpecificationORM

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

    def add_specification(
        self, neb_spec: NEBSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:

        neb_kw_dict = neb_spec.keywords.dict(exclude_defaults=True)

        with self.root_socket.optional_session(session, False) as session:
            # Add the singlepoint specification
            meta, sp_spec_id = self.root_socket.records.singlepoint.add_specification(
                neb_spec.qc_specification, session=session
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
                    qc_specification_id=sp_spec_id,
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
                    qc_specification_id=sp_spec_id,
                )

                r = session.execute(stmt).scalar_one()
                return InsertMetadata(existing_idx=[0]), r

    def query(
        self,
        query_data: NEBQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[NEBRecordDict]]:

        and_query = []
        need_spspec_join = False
        #need_nebspec_join = False
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
        if query_data.qc_keywords_id is not None:
            and_query.append(QCSpecificationORM.keywords_id.in_(query_data.qc_keywords_id))
            need_spspec_join = True
        if query_data.neb_program is not None: # Needs review
            and_query.append('geometric')
            need_qcspec_join = True
        if query_data.initial_chain_id is not None:
            and_query.append(NEBInitialchainORM.neb_id.in_(query_data.initial_chain_id))
            need_initchain_join = True

        stmt = select(NEBRecordORM)



        if need_spspec_join:
            stmt = stmt.join(QCSpecificationORM.qc_specification).options(
                contains_eager(
                    NEBRecordORM.specification,
                    NEBSpecificationORM.singlepoint_specification,
                    OptimizationSpecificationORM.qc_specification,
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
                mol_meta, molecule_ids = self.root_socket.molecules.add_mixed(init_chain, session=session)
                if not mol_meta.success:
                    return (
                        InsertMetadata(
                            error_description="Aborted - could not add all molecules: " + mol_meta.error_string
                        ),
                        [],
                    )

                init_molecule_ids.append(molecule_ids)

            return self.add_internal(init_molecule_ids, spec_id, tag, priority, session=session)

    
    def initialize_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ):

        neb_orm: NEBRecordORM = service_orm.record
        specification = NEBSpecification(**neb_orm.specification.model_dict())
        keywords = specification.keywords
        initial_chain: List[Dict[str, Any]] = [x.model_dict() for x in neb_orm.initial_chain]

        #pick_images=np.array([int(round(i)) for i in np.linspace(0, len(initial_chain)-1, keywords.images)])

        molecule_template = Molecule(**initial_chain[0]).dict(encoding="json")

        molecule_template.pop("geometry", None)
        molecule_template.pop("identifiers", None)
        molecule_template.pop("id", None)

        neb_stdout = io.StringIO()
        stdout = neb_stdout.getvalue()
        elems = molecule_template["symbols"]

        #params = {'neb': True, 'images': keywords.images, 'nebk': keywords.spring_constant, 'nebew': keywords.energy_weighted }

        if stdout:
            stdout_orm = neb_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
            stdout_orm.append(stdout)


        molecule_template_str = json.dumps(molecule_template)

        service_state = NEBServiceState(
           # neb_state = params,
            iteration = 0,
            elems = elems,
            molecule_template=molecule_template_str,
        )

        service_orm.service_state = service_state.dict()
        sqlalchemy.orm.attributes.flag_modified(service_orm, "service_state")

    def iterate_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ) -> None:

        neb_orm: NEBRecordORM = service_orm.record

        # Always update with the current provenance
        neb_orm.compute_history[-1].provenance = {
            "creator": "neb",
            "version": neb.__version__,
            "routine": "neb.neb_api",
        }


        # Load the state from the service_state column
        service_state = NEBServiceState(**service_orm.service_state)
        molecule_template = json.loads(service_state.molecule_template)
        # Sort by position
        complete_tasks = sorted(service_orm.dependencies, key=lambda x: x.extras["position"])

        geometries = []
        energies = []
        gradients = []
        
        for task in complete_tasks:
            # This is an ORM for singlepoint calculations
            sp_record = task.record
            mol_data = self.root_socket.molecules.get(molecule_id=sp.record.molecule_id, include=["geometry"], session=session)

            geometries.append(mol_data[0]["geometry"].tolist())
            energies.append(sp_record.properties.return_energy)
            gradients.append(sp_record.properties.return_gradient.tolist())



        #call geometric.neb.next_chain
        #return the next chain(list of geometries)
        #Assemble a new molecule object, I can return None for the converged chain
        newcoords = nextchain(service_state.elems, geometries, gradients, energies ,service_state.neb_state)
        if newcoords is not None:
            next_chain = [Molecule(**molecule_template, geometry=geometry) for geometry in newcoords]
            service_state.iteration += 1
            submit_singlepoints(session, service_state, service_orm, next_chain)

        stdout_orm = neb_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
        stdout_orm.append(stdout_append)

        service_orm.service_state = service_state.dict()
        sqlalchemy.orm.attributes.flag_modified(service_orm, "service_state")

        # Return True to indicate that this service has successfully completed
        return len(next_tasks) == 0 # if returned value from geometric == none -> True (converged)

    def submit_singlepoints(
        self,
        session: Session,
        service_state: NEBServiceState,
        service_orm: ServiceQueueORM,
        next_chain: List[Molecule],
    ):

        neb_orm: NEBRecordORM = service_orm.record

        # delete all existing entries in the dependency list
        service_orm.dependencies = []

        # Create a singlepoint input based on the multiple geometries

        qc_spec = neb_orm.specification.qc_specification.to_model(QCSpecification)

        meta, sp_ids = self.root_socket.records.singlepoint.add(
            next_chain,
            qc_spec,
            service_orm.tag,
            service_orm.priority,
            session=session,
        )

        if not meta.success:
                raise RuntimeError("Error adding singlepoints - likely a developer error: " + meta.error_string)
        
        for position, sp_id in enumerate(sp_ids):

            svc_dep = ServiceDependencyORM(
                record_id=sp_id,
                extras={"position": position},
            )
            
            sp_history = NEBSinglepointsORM(
                neb_id=service_orm.record_id,
                chain_iteration=service_state.iteration, 
                singlepoint_id=sp_id,
                position=position,
            )
            
            service_orm.dependencies.append(svc_dep)
            neb_orm.singlepoints.append(sp_history)
