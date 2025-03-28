from __future__ import annotations

import logging
from typing import List, Dict, Tuple, Optional, Iterable, Sequence, Any, Union, TYPE_CHECKING

import tabulate
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import array_agg, aggregate_order_by
from sqlalchemy.orm import defer, undefer, joinedload, lazyload, selectinload

from qcfractal import __version__ as qcfractal_version
from qcfractal.components.optimization.record_db_models import OptimizationSpecificationORM
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.reaction import (
    ReactionSpecification,
    ReactionQueryFilters,
    ReactionInput,
    ReactionMultiInput,
)
from qcportal.record_models import PriorityEnum, RecordStatusEnum, OutputTypeEnum
from qcportal.utils import hash_dict, is_included
from .record_db_models import ReactionComponentORM, ReactionSpecificationORM, ReactionRecordORM
from ..record_socket import BaseRecordSocket
from ..record_utils import append_output

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket

# Meaningless, but unique to reaction
reaction_insert_lock_id = 14400
reaction_spec_insert_lock_id = 14401


class ReactionRecordSocket(BaseRecordSocket):
    """
    Socket for handling reaction computations
    """

    # Used by the base class
    record_orm = ReactionRecordORM
    record_input_type = ReactionInput
    record_multi_input_type = ReactionMultiInput

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        stmt = [
            select(
                ReactionComponentORM.reaction_id.label("parent_id"),
                ReactionComponentORM.singlepoint_id.label("child_id"),
            ),
            select(
                ReactionComponentORM.reaction_id.label("parent_id"),
                ReactionComponentORM.optimization_id.label("child_id"),
            ),
        ]
        return stmt

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

        append_output(session, rxn_orm, OutputTypeEnum.stdout, output)

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

        # What was already completed and/or submitted
        sub_opt_mols = {x.molecule_id for x in rxn_orm.components if x.optimization_id is not None}
        sub_sp_mols = {x.molecule_id for x in rxn_orm.components if x.singlepoint_id is not None}

        # What we need to compute
        opt_mols_to_compute = required_opt_mols - sub_opt_mols
        sp_mols_to_compute = required_sp_mols - sub_sp_mols

        # Singlepoint calculations must wait for optimizations
        sp_mols_to_compute -= opt_mols_to_compute

        # Convert to well-ordered lists
        opt_mols_to_compute = list(opt_mols_to_compute)
        sp_mols_to_compute = list(sp_mols_to_compute)

        service_orm.dependencies = []
        output = ""

        if opt_mols_to_compute:
            opt_spec_id = rxn_orm.specification.optimization_specification_id

            meta, opt_ids = self.root_socket.records.optimization.add_internal(
                opt_mols_to_compute,
                opt_spec_id,
                service_orm.compute_tag,
                service_orm.compute_priority,
                rxn_orm.owner_user_id,
                rxn_orm.owner_group_id,
                service_orm.find_existing,
                session=session,
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
                real_mols_to_compute = [
                    x.optimization_record.final_molecule_id
                    for x in rxn_orm.components
                    if x.molecule_id in sp_mols_to_compute
                ]
            else:
                real_mols_to_compute = sp_mols_to_compute

            qc_spec_id = rxn_orm.specification.singlepoint_specification_id
            meta, sp_ids = self.root_socket.records.singlepoint.add_internal(
                real_mols_to_compute,
                qc_spec_id,
                service_orm.compute_tag,
                service_orm.compute_priority,
                rxn_orm.owner_user_id,
                rxn_orm.owner_group_id,
                service_orm.find_existing,
                session=session,
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

        append_output(session, rxn_orm, OutputTypeEnum.stdout, output)

        return not (opt_mols_to_compute or sp_mols_to_compute)

    def add_specifications(
        self, rxn_specs: Sequence[ReactionSpecification], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds specifications for reaction services to the database, returning their IDs.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Specification IDs are returned in the same order as the input specifications

        Parameters
        ----------
        rxn_specs
            Sequence of specifications to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the IDs of the specifications.
        """

        to_add = []

        for rxn_spec in rxn_specs:
            kw_dict = rxn_spec.keywords.dict()

            rxn_spec_dict = {"program": rxn_spec.program, "keywords": kw_dict, "protocols": {}}
            rxn_spec_hash = hash_dict(rxn_spec_dict)

            rxn_spec_orm = ReactionSpecificationORM(
                program=rxn_spec.program,
                keywords=kw_dict,
                protocols=rxn_spec_dict["protocols"],
                specification_hash=rxn_spec_hash,
            )

            to_add.append(rxn_spec_orm)

        with self.root_socket.optional_session(session) as session:

            qc_specs_lst = [
                (idx, x.singlepoint_specification)
                for idx, x in enumerate(rxn_specs)
                if x.singlepoint_specification is not None
            ]
            opt_specs_lst = [
                (idx, x.optimization_specification)
                for idx, x in enumerate(rxn_specs)
                if x.optimization_specification is not None
            ]

            qc_specs_map = {}
            opt_specs_map = {}

            if qc_specs_lst:
                qc_specs = [x[1] for x in qc_specs_lst]
                meta, qc_spec_ids = self.root_socket.records.singlepoint.add_specifications(qc_specs, session=session)

                if not meta.success:
                    return (
                        InsertMetadata(
                            error_description="Unable to add singlepoint specifications: " + meta.error_string,
                        ),
                        [],
                    )

                qc_specs_map = {idx: qc_spec_id for (idx, _), qc_spec_id in zip(qc_specs_lst, qc_spec_ids)}

            if opt_specs_lst:
                opt_specs = [x[1] for x in opt_specs_lst]
                meta, opt_spec_ids = self.root_socket.records.optimization.add_specifications(
                    opt_specs, session=session
                )

                if not meta.success:
                    return (
                        InsertMetadata(
                            error_description="Unable to add optimization specifications: " + meta.error_string,
                        ),
                        [],
                    )

                opt_specs_map = {idx: opt_spec_id for (idx, _), opt_spec_id in zip(opt_specs_lst, opt_spec_ids)}

            # Unfortunately, we have to go one at a time due to the possibility of NULL fields
            # Lock for the rest of the transaction (since we have to query then add)
            session.execute(select(func.pg_advisory_xact_lock(reaction_spec_insert_lock_id))).scalar()

            inserted_idx = []
            existing_idx = []
            rxn_spec_ids = []

            for idx, rxn_spec_orm in enumerate(to_add):
                qc_spec_id = qc_specs_map.get(idx, None)
                opt_spec_id = opt_specs_map.get(idx, None)

                rxn_spec_orm.singlepoint_specification_id = qc_spec_id
                rxn_spec_orm.optimization_specification_id = opt_spec_id

                # Query first, due to behavior of NULL in postgres
                stmt = select(ReactionSpecificationORM.id).filter_by(specification_hash=rxn_spec_orm.specification_hash)

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
                    rxn_spec_ids.append(r)
                    existing_idx.append(idx)
                else:
                    session.add(rxn_spec_orm)
                    session.flush()
                    rxn_spec_ids.append(rxn_spec_orm.id)
                    inserted_idx.append(idx)

            return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx), rxn_spec_ids

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

        meta, ids = self.add_specifications([rxn_spec], session=session)

        if not ids:
            return meta, None

        return meta, ids[0]

    def get(
        self,
        record_ids: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[Dict[str, Any]]]:
        options = []

        if include:
            if is_included("components", include, exclude, False):
                options.append(
                    selectinload(ReactionRecordORM.components).options(selectinload(ReactionComponentORM.molecule))
                )

        with self.root_socket.optional_session(session, True) as session:
            return self.root_socket.records.get_base(
                orm_type=self.record_orm,
                record_ids=record_ids,
                include=include,
                exclude=exclude,
                missing_ok=missing_ok,
                additional_options=options,
                session=session,
            )

    def query(
        self,
        query_data: ReactionQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
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

        stmt = select(ReactionRecordORM.id)

        if need_spec_join or need_qc_spec_join or need_opt_spec_join:
            stmt = stmt.join(ReactionRecordORM.specification)

        if need_qc_spec_join:
            stmt = stmt.join(ReactionSpecificationORM.singlepoint_specification)

        if need_opt_spec_join:
            stmt = stmt.join(ReactionSpecificationORM.optimization_specification)

        if need_component_join:
            # Do not load components as part of the ORM, but join for the query
            stmt = stmt.join(ReactionRecordORM.components)

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
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
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
            order of the input molecules
        """

        compute_tag = compute_tag.lower()

        with self.root_socket.optional_session(session, False) as session:
            self.root_socket.users.assert_group_member(owner_user_id, owner_group_id, session=session)

            # Lock for the entire transaction
            session.execute(select(func.pg_advisory_xact_lock(reaction_insert_lock_id))).scalar()

            rxn_ids = []
            inserted_idx = []
            existing_idx = []

            if find_existing:
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
                            owner_user_id=owner_user_id,
                            owner_group_id=owner_group_id,
                        )

                        self.create_service(rxn_orm, compute_tag, compute_priority, find_existing)

                        session.add(rxn_orm)
                        session.flush()

                        rxn_ids.append(rxn_orm.id)
                        inserted_idx.append(idx)
                    else:
                        rxn_ids.append(existing)
                        existing_idx.append(idx)

            else:  # don't find existing - always add
                for idx, rxn_mols in enumerate(stoichiometries):
                    # sort molecule ids by increasing ids, and remove duplicates
                    rxn_mol_ids = sorted(set(x[1] for x in rxn_mols))

                    component_orm = [
                        ReactionComponentORM(coefficient=coeff, molecule_id=mid) for coeff, mid in rxn_mols
                    ]

                    rxn_orm = ReactionRecordORM(
                        is_service=True,
                        specification_id=rxn_spec_id,
                        components=component_orm,
                        status=RecordStatusEnum.waiting,
                        owner_user_id=owner_user_id,
                        owner_group_id=owner_group_id,
                    )

                    self.create_service(rxn_orm, compute_tag, compute_priority, find_existing)

                    session.add(rxn_orm)
                    session.flush()

                    rxn_ids.append(rxn_orm.id)
                    inserted_idx.append(idx)

            meta = InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx)
            return meta, rxn_ids

    def add(
        self,
        stoichiometries: Sequence[Iterable[Tuple[float, Union[int, Molecule]]]],
        rxn_spec: ReactionSpecification,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority for the computation
        owner_user
            Name or ID of the user who owns the record
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records
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
            owner_user_id, owner_group_id = self.root_socket.users.get_owner_ids(
                owner_user, owner_group, session=session
            )

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

            return self.add_internal(
                new_mol,
                spec_id,
                compute_tag,
                compute_priority,
                owner_user_id,
                owner_group_id,
                find_existing,
                session=session,
            )

    def add_from_input(
        self,
        record_input: ReactionInput,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertCountsMetadata, int]:

        assert isinstance(record_input, ReactionInput)

        meta, ids = self.add(
            [record_input.stoichiometries],
            record_input.specification,
            compute_tag,
            compute_priority,
            owner_user,
            owner_group,
            find_existing,
        )

        return InsertCountsMetadata.from_insert_metadata(meta), ids[0]

    ####################################################
    # Some stuff to be retrieved for reactions
    ####################################################

    def get_components(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(ReactionRecordORM.components).options(
                undefer("*"), joinedload(ReactionComponentORM.molecule).options(undefer("*"))
            ),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(ReactionRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")

            return [x.model_dict() for x in rec.components]
