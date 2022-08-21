from __future__ import annotations

import logging
from typing import List, Dict, Tuple, Optional, Iterable, Sequence, Any, Union, TYPE_CHECKING

import tabulate
from sqlalchemy import select, union
from sqlalchemy.dialects.postgresql import insert, array_agg, aggregate_order_by
from sqlalchemy.orm import contains_eager

from qcfractal import __version__ as qcfractal_version
from qcfractal.components.records.optimization.db_models import OptimizationSpecificationORM
from qcfractal.components.records.singlepoint.db_models import QCSpecificationORM
from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcportal.metadata_models import InsertMetadata, QueryMetadata
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputTypeEnum
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.reaction import (
    ReactionSpecification,
    ReactionQueryFilters,
)
from .db_models import ReactionComponentORM, ReactionSpecificationORM, ReactionRecordORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket


class ReactionRecordSocket(BaseRecordSocket):
    """
    Socket for handling reaction computations
    """

    # Used by the base class
    record_orm = ReactionRecordORM
    specification_orm = ReactionSpecificationORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        stmt = union(
            select(
                ReactionComponentORM.reaction_id.label("parent_id"),
                ReactionComponentORM.singlepoint_id.label("child_id"),
            ),
            select(
                ReactionComponentORM.reaction_id.label("parent_id"),
                ReactionComponentORM.optimization_id.label("child_id"),
            ),
        )
        return [stmt]

    def initialize_service(self, session: Session, service_orm: ServiceQueueORM) -> None:
        rxn_orm: ReactionRecordORM = service_orm.record

        output = "\n\nCreated reaction. Molecules:\n\n"

        output += "-" * 80 + "\nManybody Keywords:\n\n"
        spec: ReactionSpecification = rxn_orm.specification.to_model(ReactionSpecification)
        table_rows = sorted(spec.keywords.dict().items())
        output += tabulate.tabulate(table_rows, headers=["keyword", "value"])

        if spec.singlepoint_specification:
            output += "\n\n" + "-" * 80 + "\nQC Specification:\n\n"
            table_rows = sorted(spec.singlepoint_specification.dict().items())
            output += tabulate.tabulate(table_rows, headers=["keyword", "value"])
            output += "\n\n"

        if spec.optimization_specification:
            output += "\n\n" + "-" * 80 + "\nOptimization Specification:\n\n"
            table_rows = sorted(spec.optimization_specification.dict().items())
            output += tabulate.tabulate(table_rows, headers=["keyword", "value"])
            output += "\n\n"

        output += "\n\n" + "-" * 80 + "\nReaction Stoichiometry:\n\n"
        table_rows = [
            (f"{m.coefficient:.8f}", m.molecule.identifiers["molecular_formula"], m.molecule_id)
            for m in rxn_orm.components
        ]
        output += tabulate.tabulate(table_rows, headers=["coefficient", "molecule", "molecule id"])
        output += "\n\n"

        stdout_orm = rxn_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
        stdout_orm.append(output)

        # Reactions are simple and don't require a service state

    def iterate_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ):

        rxn_orm: ReactionRecordORM = service_orm.record

        spec: ReactionSpecification = rxn_orm.specification.to_model(ReactionSpecification)
        has_singlepoint = bool(spec.singlepoint_specification)
        has_optimization = bool(spec.optimization_specification)

        # Always update with the current provenance
        rxn_orm.compute_history[-1].provenance = {
            "creator": "qcfractal",
            "version": qcfractal_version,
            "routine": "qcfractal.services.reaction",
        }

        # Component by molecule id
        component_map = {x.molecule_id: x for x in rxn_orm.components}

        # Molecules we need to do an optimization on
        required_opt_mols = {x.molecule_id for x in rxn_orm.components if has_optimization}

        # Molecules we need to do a singlepoint calculation on
        # (this is the molecule of the reaction, so it may actually be the INITIAL molecule
        # of an optimization)
        required_sp_mols = {x.molecule_id for x in rxn_orm.components if has_singlepoint}

        complete_tasks = service_orm.dependencies

        # What was already completed and/or submitted
        sub_opt_mols = {x.molecule_id for x in rxn_orm.components if x.optimization_id is not None}
        sub_sp_mols = {x.molecule_id for x in rxn_orm.components if x.singlepoint_id is not None}

        # What we need to compute
        opt_mols_to_compute = required_opt_mols - sub_opt_mols
        sp_mols_to_compute = required_sp_mols - sub_sp_mols

        # Singlepoint calculations must wait for optimizations
        sp_mols_to_compute -= opt_mols_to_compute

        service_orm.dependencies = []

        if opt_mols_to_compute:
            opt_spec_id = rxn_orm.specification.optimization_specification_id

            meta, opt_ids = self.root_socket.records.optimization.add_internal(
                opt_mols_to_compute, opt_spec_id, service_orm.tag, service_orm.priority, session=session
            )

            for mol_id, opt_id in zip(opt_mols_to_compute, opt_ids):
                component = component_map[mol_id]

                svc_dep = ServiceDependencyORM(record_id=opt_id, extras={})

                assert component.singlepoint_id is None
                component.optimization_id = opt_id

                service_orm.dependencies.append(svc_dep)

            output = "\n\nSubmitted optimization calculations:\n"
            output += tabulate.tabulate(zip(opt_mols_to_compute, opt_ids), headers=["molecule id", "optimization id"])

        if sp_mols_to_compute:

            # If an optimization was specified, we need to get the final molecule from that
            if has_optimization:
                real_mols_to_compute = {
                    x.optimization_record.final_molecule_id
                    for x in rxn_orm.components
                    if x.molecule_id in sp_mols_to_compute
                }
            else:
                real_mols_to_compute = sp_mols_to_compute

            qc_spec_id = rxn_orm.specification.singlepoint_specification_id
            meta, sp_ids = self.root_socket.records.singlepoint.add_internal(
                real_mols_to_compute, qc_spec_id, service_orm.tag, service_orm.priority, session=session
            )

            # Note the mapping back to the original molecule id (not the optimized one)
            for mol_id, sp_id in zip(sp_mols_to_compute, sp_ids):
                component = component_map[mol_id]

                svc_dep = ServiceDependencyORM(record_id=sp_id, extras={})

                assert component.singlepoint_id is None
                component.singlepoint_id = sp_id

                service_orm.dependencies.append(svc_dep)

            output = "\n\nSubmitted singlepoint calculations:\n"
            output += tabulate.tabulate(zip(sp_mols_to_compute, sp_ids), headers=["molecule id", "singlepoint id"])

        if not (opt_mols_to_compute or sp_mols_to_compute):
            output = "\n\n" + "*" * 80 + "\n"
            output += "All reaction components are complete!\n\n"

            output += "Reaction results:\n"
            table = []
            total_energy = 0.0

            coef_map = {x.molecule_id: x.coefficient for x in rxn_orm.components}

            for component in rxn_orm.components:
                mol_form = component.molecule.identifiers["molecular_formula"]
                mol_id = component.molecule_id
                coefficient = coef_map[mol_id]

                if has_optimization and has_singlepoint:
                    energy = component.singlepoint_record.properties["return_energy"]
                    table_row = [
                        mol_id,
                        mol_form,
                        component.optimization_record.final_molecule_id,
                        component.optimization_id,
                        component.singlepoint_id,
                        energy,
                        coefficient,
                    ]
                elif has_singlepoint:
                    energy = component.singlepoint_record.properties["return_energy"]
                    table_row = [mol_id, mol_form, component.singlepoint_id, energy, coefficient]
                else:
                    # has optimization only
                    energy = component.optimization_record.energies[-1]
                    table_row = [
                        mol_id,
                        mol_form,
                        component.optimization_record.final_molecule_id,
                        component.optimization_id,
                        energy,
                        coefficient,
                    ]

                table.append(table_row)
                total_energy += coefficient * energy

            if has_optimization and has_singlepoint:
                output += tabulate.tabulate(
                    table,
                    headers=[
                        "initial molecule id",
                        "molecule",
                        "optimized molecule id",
                        "optimization id",
                        "singlepoint id",
                        "energy (hartree)",
                        "coefficient",
                    ],
                )
            elif has_singlepoint:
                output += tabulate.tabulate(
                    table,
                    headers=["initial molecule id", "molecule", "singlepoint id", "energy (hartree)", "coefficient"],
                )
            else:
                output += tabulate.tabulate(
                    table,
                    headers=[
                        "initial molecule id",
                        "molecule",
                        "optimized molecule id",
                        "optimization id",
                        "energy (hartree)",
                        "coefficient",
                    ],
                )

            output += "\n\n"
            output += f"Total reaction energy: {total_energy:.16f} hartrees"

            rxn_orm.total_energy = total_energy

        stdout_orm = rxn_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
        stdout_orm.append(output)

        return not (opt_mols_to_compute or sp_mols_to_compute)

    def add_specification(
        self, rxn_spec: ReactionSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        """
        Adds a specification for a reaction service to the database, returning its id.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Parameters
        ----------
        rxn_spec
            Specification to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the id of the specification.
        """
        with self.root_socket.optional_session(session) as session:
            qc_spec_id = None
            opt_spec_id = None

            if rxn_spec.singlepoint_specification is not None:
                meta, qc_spec_id = self.root_socket.records.singlepoint.add_specification(
                    qc_spec=rxn_spec.singlepoint_specification, session=session
                )

                if not meta.success:
                    return (
                        InsertMetadata(
                            error_description="Unable to add singlepoint specification: " + meta.error_string,
                        ),
                        None,
                    )

            if rxn_spec.optimization_specification is not None:
                meta, opt_spec_id = self.root_socket.records.optimization.add_specification(
                    opt_spec=rxn_spec.optimization_specification, session=session
                )

                if not meta.success:
                    return (
                        InsertMetadata(
                            error_description="Unable to add optimization specification: " + meta.error_string,
                        ),
                        None,
                    )

            kw_dict = rxn_spec.keywords.dict(exclude_defaults=True)

            # Query first, due to behavior of NULL in postgres
            stmt = select(ReactionSpecificationORM.id).filter_by(
                program=rxn_spec.program,
                keywords=kw_dict,
            )

            if qc_spec_id is not None:
                stmt = stmt.filter(ReactionSpecificationORM.singlepoint_specification_id == qc_spec_id)
            else:
                stmt = stmt.filter(ReactionSpecificationORM.singlepoint_specification_id.is_(None))

            if opt_spec_id is not None:
                stmt = stmt.filter(ReactionSpecificationORM.optimization_specification_id == opt_spec_id)
            else:
                stmt = stmt.filter(ReactionSpecificationORM.optimization_specification_id.is_(None))

            r = session.execute(stmt).scalar_one_or_none()
            if r is not None:
                return InsertMetadata(existing_idx=[0]), r
            else:
                stmt = (
                    insert(ReactionSpecificationORM)
                    .values(
                        program=rxn_spec.program,
                        singlepoint_specification_id=qc_spec_id,
                        optimization_specification_id=opt_spec_id,
                        keywords=kw_dict,
                    )
                    .returning(ReactionSpecificationORM.id)
                )

            r = session.execute(stmt).scalar_one_or_none()
            return InsertMetadata(inserted_idx=[0]), r

    def query(
        self,
        query_data: ReactionQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[Dict[str, Any]]]:
        """
        Query reaction records

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
            Metadata about the results of the query, and a list of records (as dictionaries)
            that were found in the database.
        """

        and_query = []
        need_qc_spec_join = False
        need_opt_spec_join = False
        need_spec_join = False
        need_component_join = False

        if query_data.program is not None:
            and_query.append(ReactionSpecificationORM.program.in_(query_data.program))
            need_spec_join = True
        if query_data.qc_program is not None:
            and_query.append(QCSpecificationORM.program.in_(query_data.qc_program))
            need_qc_spec_join = True
        if query_data.qc_method is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.qc_method))
            need_qc_spec_join = True
        if query_data.qc_basis is not None:
            and_query.append(QCSpecificationORM.basis.in_(query_data.qc_basis))
            need_qc_spec_join = True
        if query_data.optimization_program is not None:
            and_query.append(OptimizationSpecificationORM.program.in_(query_data.qc_basis))
            need_opt_spec_join = True
        if query_data.molecule_id is not None:
            and_query.append(ReactionComponentORM.molecule_id.in_(query_data.molecule_id))
            need_component_join = True

        stmt = select(ReactionRecordORM)

        if need_spec_join or need_qc_spec_join or need_opt_spec_join:
            stmt = stmt.join(ReactionRecordORM.specification).options(contains_eager(ReactionRecordORM.specification))

        if need_qc_spec_join:
            stmt = stmt.join(ReactionSpecificationORM.singlepoint_specification).options(
                contains_eager(ReactionRecordORM.specification, ReactionSpecificationORM.singlepoint_specification)
            )

        if need_opt_spec_join:
            stmt = stmt.join(ReactionSpecificationORM.optimization_specification).options(
                contains_eager(ReactionRecordORM.specification, ReactionSpecificationORM.optimization_specification)
            )

        if need_component_join:
            stmt = stmt.join(ReactionRecordORM.components).options(contains_eager(ReactionRecordORM.components))

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=ReactionRecordORM,
            query_data=query_data,
            session=session,
        )

    def add_internal(
        self,
        stoichiometries: Sequence[Iterable[Tuple[float, int]]],  # coefficient, molecule_id
        rxn_spec_id: int,
        tag: str,
        priority: PriorityEnum,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Internal function for adding new reaction computations

        This function expects that the molecules and specification are already added to the
        database and that the ids are known.

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        stoichiometries
            Coefficients and IDs of the molecules that form the reaction
        rxn_spec_id
            ID of the specification
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority for the computation
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        tag = tag.lower()

        with self.root_socket.optional_session(session, False) as session:
            rxn_ids = []
            inserted_idx = []
            existing_idx = []

            # Deduplication is a bit complicated we have a many-to-many relationship
            # between reactions and molecules. So skip the general insert

            # Create a cte with the molecules we can query against
            # This is like a table, with the specification id and the molecule ids
            # as a postgres array (sorted)
            # We then use this to determine if there are duplicates
            init_mol_cte = (
                select(
                    ReactionRecordORM.id,
                    ReactionRecordORM.specification_id,
                    array_agg(
                        aggregate_order_by(ReactionComponentORM.molecule_id, ReactionComponentORM.molecule_id.asc())
                    ).label("molecule_ids"),
                )
                .join(
                    ReactionComponentORM,
                    ReactionComponentORM.reaction_id == ReactionRecordORM.id,
                )
                .group_by(ReactionRecordORM.id)
                .cte()
            )

            for idx, rxn_mols in enumerate(stoichiometries):
                # sort molecule ids by increasing ids, and remove duplicates
                rxn_mol_ids = sorted(set(x[1] for x in rxn_mols))

                # does this exist?
                stmt = select(init_mol_cte.c.id)
                stmt = stmt.where(init_mol_cte.c.specification_id == rxn_spec_id)
                stmt = stmt.where(init_mol_cte.c.molecule_ids == rxn_mol_ids)
                existing = session.execute(stmt).scalars().first()

                if not existing:
                    component_orm = [
                        ReactionComponentORM(coefficient=coeff, molecule_id=mid) for coeff, mid in rxn_mols
                    ]

                    rxn_orm = ReactionRecordORM(
                        is_service=True,
                        specification_id=rxn_spec_id,
                        components=component_orm,
                        status=RecordStatusEnum.waiting,
                    )

                    self.create_service(rxn_orm, tag, priority)

                    session.add(rxn_orm)
                    session.flush()

                    rxn_ids.append(rxn_orm.id)
                    inserted_idx.append(idx)
                else:
                    rxn_ids.append(existing)
                    existing_idx.append(idx)

            meta = InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)
            return meta, rxn_ids

    def add(
        self,
        stoichiometries: Sequence[Iterable[Tuple[float, Union[int, Molecule]]]],
        rxn_spec: ReactionSpecification,
        tag: str,
        priority: PriorityEnum,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new reaction calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        stoichiometries
            Coefficient and molecules (objects or ids) to compute using the specification
        rxn_spec
            Specification for the reaction calculations
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority for the computation
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input stoichiometries
        """

        with self.root_socket.optional_session(session, False) as session:

            # First, add the specification
            spec_meta, spec_id = self.add_specification(rxn_spec, session=session)

            if not spec_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add specification: " + spec_meta.error_string
                    ),
                    [],
                )

            # Now the molecules
            new_mol = []
            for single_stoic in stoichiometries:
                mol_obj = [x[1] for x in single_stoic]
                mol_meta, mol_ids = self.root_socket.molecules.add_mixed(mol_obj, session=session)
                if not mol_meta.success:
                    return (
                        InsertMetadata(
                            error_description="Aborted - could not add all molecules: " + mol_meta.error_string
                        ),
                        [],
                    )

                new_mol.append([(x[0], y) for x, y in zip(single_stoic, mol_ids)])

            return self.add_internal(new_mol, spec_id, tag, priority, session=session)
