from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcelemental.models import (
    OptimizationResult as QCEl_OptimizationResult,
)
from sqlalchemy import select, update
from sqlalchemy.orm import lazyload, joinedload, selectinload, defer, undefer, load_only

from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM
from qcfractal.db_socket.helpers import insert_general
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.optimization import (
    OptimizationSpecification,
    OptimizationQueryFilters,
    OptimizationInput,
    OptimizationMultiInput,
)
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.serialization import convert_numpy_recursive
from qcportal.singlepoint import QCSpecification
from qcportal.singlepoint import (
    SinglepointDriver,
)
from qcportal.utils import hash_dict, is_included
from .record_db_models import OptimizationSpecificationORM, OptimizationRecordORM, OptimizationTrajectoryORM
from ..record_socket import BaseRecordSocket

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union

# Meaningless, but unique to optimizations
optimization_insert_lock_id = 14100
optimization_spec_insert_lock_id = 14101


class OptimizationRecordSocket(BaseRecordSocket):
    """
    Socket for handling optimization computations
    """

    # Used by the base class
    record_orm = OptimizationRecordORM
    record_input_type = OptimizationInput
    record_multi_input_type = OptimizationMultiInput

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        stmt = select(
            OptimizationTrajectoryORM.optimization_id.label("parent_id"),
            OptimizationTrajectoryORM.singlepoint_id.label("child_id"),
        )
        return [stmt]

    def generate_task_specifications(self, session: Session, record_ids: Sequence[int]) -> List[Dict[str, Any]]:

        stmt = select(OptimizationRecordORM).filter(OptimizationRecordORM.id.in_(record_ids))
        stmt = stmt.options(load_only(OptimizationRecordORM.id, OptimizationRecordORM.extras))
        stmt = stmt.options(
            lazyload("*"),
            joinedload(OptimizationRecordORM.initial_molecule),
            selectinload(OptimizationRecordORM.specification),
        )

        record_orms = session.execute(stmt).scalars().all()

        task_specs = {}

        for record_orm in record_orms:
            specification = record_orm.specification
            initial_molecule = record_orm.initial_molecule.model_dict()

            model = {"method": specification.qc_specification.method}

            # Empty basis string should be None in the model
            if specification.qc_specification.basis:
                model["basis"] = specification.qc_specification.basis
            else:
                model["basis"] = None

            # Add the singlepoint program to the optimization keywords
            opt_keywords = specification.keywords.copy()
            opt_keywords["program"] = specification.qc_specification.program

            # driver = "gradient" is what the current schema uses
            qcschema_input = dict(
                schema_name="qcschema_optimization_input",
                schema_version=1,
                id=str(record_orm.id),  # str for compatibility
                input_specification=dict(
                    schema_name="qcschema_input",
                    schema_version=1,
                    model=model,
                    driver="gradient",
                    keywords=specification.qc_specification.keywords,
                ),
                initial_molecule=convert_numpy_recursive(
                    initial_molecule, flatten=True
                ),  # TODO - remove after all data is converted
                keywords=opt_keywords,
                protocols=specification.protocols,
                extras=record_orm.extras if record_orm.extras else {},
            )

            # Note that the 'program' that runs an optimization is
            # called a 'procedure' in QCEngine
            task_specs[record_orm.id] = {
                "function": "qcengine.compute_procedure",
                "function_kwargs": {
                    "input_data": qcschema_input,
                    "procedure": specification.program,
                },
            }

        if set(record_ids) != set(task_specs.keys()):
            raise RuntimeError("Did not generate all task specs for all optimizaiton records?")

        # Return in the input order
        return [task_specs[rid] for rid in record_ids]

    def update_completed_task(self, session: Session, record_id: int, result: QCEl_OptimizationResult) -> None:

        # Add the final molecule
        meta, final_mol_id = self.root_socket.molecules.add([result.final_molecule], session=session)
        if not meta.success:
            raise RuntimeError("Unable to add final molecule: " + meta.error_string)

        # Insert the trajectory
        traj_ids = self.root_socket.records.insert_complete_schema_v1(session, result.trajectory)

        for position, traj_id in enumerate(traj_ids):
            assoc_orm = OptimizationTrajectoryORM(singlepoint_id=traj_id)
            assoc_orm.optimization_id = record_id
            assoc_orm.position = position
            session.add(assoc_orm)

        # Update the fields themselves
        record_updates = {
            "final_molecule_id": final_mol_id[0],
            "energies": result.energies,
        }

        stmt = update(OptimizationRecordORM).where(OptimizationRecordORM.id == record_id).values(record_updates)
        session.execute(stmt)

    def insert_complete_schema_v1(
        self,
        session: Session,
        results: Sequence[QCEl_OptimizationResult],
    ) -> List[OptimizationRecordORM]:

        ret = []

        initial_mols = []
        final_mols = []
        opt_specs = []

        for result in results:
            initial_mols.append(result.initial_molecule)
            final_mols.append(result.final_molecule)

            # in v1 of the schema, the qc program is stored as a keyword
            qc_program = result.keywords.pop("program", "")

            qc_spec = QCSpecification(
                program=qc_program,
                driver=result.input_specification.driver,
                method=result.input_specification.model.method,
                basis=result.input_specification.model.basis,
                keywords=result.input_specification.keywords,
                # no protocols allowed in v1 of the schema
            )

            opt_spec = OptimizationSpecification(
                program=result.provenance.creator.lower(),
                qc_specification=qc_spec,
                keywords=result.keywords,
                protocols=result.protocols,
            )

            opt_specs.append(opt_spec)

        meta, spec_ids = self.root_socket.records.optimization.add_specifications(opt_specs, session=session)
        if not meta.success:
            raise RuntimeError("Aborted optimization insertion - could not add specifications: " + meta.error_string)

        meta, initial_mol_ids = self.root_socket.molecules.add(initial_mols, session=session)
        if not meta.success:
            raise RuntimeError("Aborted optimization insertion - could not add initial molecules: " + meta.error_string)

        meta, final_mol_ids = self.root_socket.molecules.add(final_mols, session=session)
        if not meta.success:
            raise RuntimeError("Aborted optimization insertion - could not add final molecules: " + meta.error_string)

        for result, initial_mol_id, final_mol_id, spec_id in zip(results, initial_mol_ids, final_mol_ids, spec_ids):
            record_orm = OptimizationRecordORM(
                specification_id=spec_id,
                initial_molecule_id=initial_mol_id,
                final_molecule_id=final_mol_id,
                energies=result.energies,
                status=RecordStatusEnum.complete,
            )

            if result.trajectory:
                trajectory_ids = self.root_socket.records.insert_complete_schema_v1(session, result.trajectory)
                opt_traj_orm = [
                    OptimizationTrajectoryORM(singlepoint_id=tid, position=idx)
                    for idx, tid in enumerate(trajectory_ids)
                ]
                record_orm.trajectory = opt_traj_orm

            ret.append(record_orm)

        return ret

    def add_specifications(
        self, opt_specs: Sequence[OptimizationSpecification], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds specification for optimization calculations to the database, returning their IDs.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Specification IDs are returned in the same order as the input specifications

        Parameters
        ----------
        opt_specs
            Sequence of specification to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the IDs of the specifications.
        """

        to_add = []

        for opt_spec in opt_specs:
            protocols_dict = opt_spec.protocols.dict(exclude_defaults=True)

            # Don't include lower specifications in the hash
            opt_spec_dict = opt_spec.dict(exclude={"protocols", "qc_specification"})
            opt_spec_dict["protocols"] = protocols_dict
            opt_spec_hash = hash_dict(opt_spec_dict)

            # Leave qc spec id for later
            opt_spec_orm = OptimizationSpecificationORM(
                program=opt_spec.program,
                keywords=opt_spec.keywords,
                protocols=protocols_dict,
                specification_hash=opt_spec_hash,
            )

            to_add.append(opt_spec_orm)

        with self.root_socket.optional_session(session, False) as session:
            qc_specs = [x.qc_specification for x in opt_specs]
            for qc_spec in qc_specs:
                # Make double sure the driver is deferred
                qc_spec.driver = SinglepointDriver.deferred

            meta, qc_spec_ids = self.root_socket.records.singlepoint.add_specifications(qc_specs, session=session)

            if not meta.success:
                return (
                    InsertMetadata(error_description="Unable to add single point specifications: " + meta.error_string),
                    [],
                )

            assert len(qc_spec_ids) == len(opt_specs)
            for opt_spec_orm, qc_spec_id in zip(to_add, qc_spec_ids):
                opt_spec_orm.qc_specification_id = qc_spec_id

            meta, ids = insert_general(
                session,
                to_add,
                (OptimizationSpecificationORM.specification_hash, OptimizationSpecificationORM.qc_specification_id),
                (OptimizationSpecificationORM.id,),
                optimization_spec_insert_lock_id,
            )

            return meta, [x[0] for x in ids]

    def add_specification(
        self, opt_spec: OptimizationSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        """
        Adds a specification for an optimization calculation to the database, returning its id.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Parameters
        ----------
        opt_spec
            Specification to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the id of the specification.
        """

        meta, ids = self.add_specifications([opt_spec], session=session)

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
            if is_included("initial_molecule", include, exclude, False):
                options.append(joinedload(OptimizationRecordORM.initial_molecule))
            if is_included("final_molecule", include, exclude, False):
                options.append(joinedload(OptimizationRecordORM.final_molecule))
            if is_included("trajectory", include, exclude, False):
                options.append(selectinload(OptimizationRecordORM.trajectory))

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
        query_data: OptimizationQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        and_query = []
        need_spspec_join = False
        need_optspec_join = False

        if query_data.program is not None:
            and_query.append(OptimizationSpecificationORM.program.in_(query_data.program))
            need_optspec_join = True
        if query_data.qc_program is not None:
            and_query.append(QCSpecificationORM.program.in_(query_data.qc_program))
            need_spspec_join = True
        if query_data.qc_method is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.qc_method))
            need_spspec_join = True
        if query_data.qc_basis is not None:
            and_query.append(QCSpecificationORM.basis.in_(query_data.qc_basis))
            need_spspec_join = True
        if query_data.initial_molecule_id is not None:
            and_query.append(OptimizationRecordORM.initial_molecule_id.in_(query_data.initial_molecule_id))
        if query_data.final_molecule_id is not None:
            and_query.append(OptimizationRecordORM.final_molecule_id.in_(query_data.final_molecule_id))

        stmt = select(OptimizationRecordORM.id)

        # If we need the singlepoint spec, we also need the optimization spec
        if need_optspec_join or need_spspec_join:
            stmt = stmt.join(OptimizationRecordORM.specification)

        if need_spspec_join:
            stmt = stmt.join(OptimizationSpecificationORM.qc_specification)

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=OptimizationRecordORM,
            query_data=query_data,
            session=session,
        )

    def add_internal(
        self,
        initial_molecule_ids: Sequence[int],
        opt_spec_id: int,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Internal function for adding new optimization computations

        This function expects that the molecules and specification are already added to the
        database and that the ids are known.

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        initial_molecule_ids
            IDs of the molecules to optimize. One record will be added per molecule.
        opt_spec_id
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

        with self.root_socket.optional_session(session) as session:
            self.root_socket.users.assert_group_member(owner_user_id, owner_group_id, session=session)

            # Get the spec orm. The full orm will be needed for create_task
            stmt = select(OptimizationSpecificationORM).where(OptimizationSpecificationORM.id == opt_spec_id)
            spec_orm = session.execute(stmt).scalar_one()

            all_orm = []
            all_molecules = self.root_socket.molecules.get(initial_molecule_ids, session=session)

            for mol_data in all_molecules:
                opt_orm = OptimizationRecordORM(
                    is_service=False,
                    specification=spec_orm,
                    specification_id=opt_spec_id,
                    initial_molecule_id=mol_data["id"],
                    status=RecordStatusEnum.waiting,
                    owner_user_id=owner_user_id,
                    owner_group_id=owner_group_id,
                )

                self.create_task(opt_orm, compute_tag, compute_priority)
                all_orm.append(opt_orm)

            if find_existing:
                meta, ids = insert_general(
                    session,
                    all_orm,
                    (OptimizationRecordORM.specification_id, OptimizationRecordORM.initial_molecule_id),
                    (OptimizationRecordORM.id,),
                    lock_id=optimization_insert_lock_id,
                )
                return meta, [x[0] for x in ids]
            else:
                session.add_all(all_orm)
                session.flush()
                meta = InsertMetadata(inserted_idx=list(range(len(all_orm))))

                return meta, [x.id for x in all_orm]

    def add(
        self,
        initial_molecules: Sequence[Union[int, Molecule]],
        opt_spec: OptimizationSpecification,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new optimization calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        initial_molecules
            Molecules to compute using the specification
        opt_spec
            Specification for the calculations
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
            order of the input molecules
        """

        with self.root_socket.optional_session(session, False) as session:
            owner_user_id, owner_group_id = self.root_socket.users.get_owner_ids(
                owner_user, owner_group, session=session
            )

            # First, add the specification
            spec_meta, spec_id = self.add_specification(opt_spec, session=session)
            if not spec_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add specification: " + spec_meta.error_string
                    ),
                    [],
                )

            # Now the molecules
            mol_meta, mol_ids = self.root_socket.molecules.add_mixed(initial_molecules, session=session)
            if not mol_meta.success:
                return (
                    InsertMetadata(error_description="Aborted - could not add all molecules: " + mol_meta.error_string),
                    [],
                )

            return self.add_internal(
                mol_ids,
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
        record_input: OptimizationInput,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertCountsMetadata, int]:

        assert isinstance(record_input, OptimizationInput)

        meta, ids = self.add(
            [record_input.initial_molecule],
            record_input.specification,
            compute_tag,
            compute_priority,
            owner_user,
            owner_group,
            find_existing,
        )

        return InsertCountsMetadata.from_insert_metadata(meta), ids[0]

    ####################################################
    # Some stuff to be retrieved for optimizations
    ####################################################

    def get_trajectory_ids(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        Retrieve the IDs of the singlepoint computations that form the trajectory
        """

        options = [
            lazyload("*"),
            defer("*"),
            joinedload(OptimizationRecordORM.trajectory).options(undefer(OptimizationTrajectoryORM.singlepoint_id)),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(OptimizationRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return [x.singlepoint_id for x in rec.trajectory]
