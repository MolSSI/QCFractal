from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from qcelemental.models import OptimizationInput, OptimizationResult
from qcelemental.models.procedures import QCInputSpecification

from sqlalchemy import select
from sqlalchemy.orm import contains_eager
from sqlalchemy.dialects.postgresql import insert

from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.db_socket.helpers import get_general, insert_general, get_general_multi
from qcfractal.portal.metadata_models import InsertMetadata, QueryMetadata
from qcfractal.portal.molecules import Molecule
from qcfractal.portal.records import PriorityEnum, RecordStatusEnum
from qcfractal.portal.records.singlepoint import (
    SinglePointDriver,
    WavefunctionProperties,
    SinglePointSpecification,
    SinglePointInputSpecification,
)
from qcfractal.portal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationSpecification,
    OptimizationProtocols,
    OptimizationQueryBody,
)


from .db_models import OptimizationSpecificationORM, OptimizationProcedureORM, OptimizationTrajectoryORM
from qcfractal.components.records.singlepoint.db_models import ResultORM, SinglePointSpecificationORM
from qcfractal.portal.keywords import KeywordSet

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from datetime import datetime
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union, Iterable

    OptimizationSpecificationDict = Dict[str, Any]
    OptimizationRecordDict = Dict[str, Any]


class OptimizationRecordSocket(BaseRecordSocket):
    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    def get_children_ids(self, session: Session, record_id: Iterable[int]) -> List[int]:
        stmt = select(OptimizationTrajectoryORM.singlepoint_record_id)
        stmt = stmt.where(OptimizationTrajectoryORM.optimization_record_id.in_(record_id))
        return session.execute(stmt).scalars().all()

    def get_specification(
        self, id: int, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[OptimizationSpecificationDict]:
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
            Keyword information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the keywords were missing
        """

        with self.root_socket.optional_session(session, True) as session:
            return get_general(
                session, OptimizationSpecificationORM, OptimizationSpecificationORM.id, [id], None, None, missing_ok
            )[0]

    def add_specification(
        self, opt_spec: OptimizationInputSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:

        protocols_dict = opt_spec.protocols.dict(exclude_defaults=True)

        with self.root_socket.optional_session(session, False) as session:
            # Add the singlepoint specification
            # Make double sure the driver is deferred
            opt_spec.singlepoint_specification.driver = SinglePointDriver.deferred
            meta, sp_spec_id = self.root_socket.records.singlepoint.add_specification(
                opt_spec.singlepoint_specification, session=session
            )
            if not meta.success:
                return (
                    InsertMetadata(
                        error_description="Unable to add single point specification: " + meta.error_string,
                    ),
                    None,
                )

            stmt = (
                insert(OptimizationSpecificationORM)
                .values(
                    program=opt_spec.program,
                    keywords=opt_spec.keywords,
                    singlepoint_specification_id=sp_spec_id,
                    protocols=protocols_dict,
                )
                .on_conflict_do_nothing()
                .returning(OptimizationSpecificationORM.id)
            )

            r = session.execute(stmt).scalar_one_or_none()
            if r is not None:
                return InsertMetadata(inserted_idx=[0]), r
            else:
                # Specification was already existing
                stmt = select(OptimizationSpecificationORM.id).filter_by(
                    program=opt_spec.program,
                    keywords=opt_spec.keywords,
                    singlepoint_specification_id=sp_spec_id,
                    protocols=protocols_dict,
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
    ) -> List[Optional[OptimizationRecordDict]]:
        """
        Obtain an optimization record with specified IDs

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
            OptimizationProcedureORM, record_id, include, exclude, missing_ok, session=session
        )

    def get_trajectory(
        self,
        record_id: int,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ):

        with self.root_socket.optional_session(session, True) as session:
            traj = get_general_multi(
                session,
                OptimizationTrajectoryORM,
                OptimizationTrajectoryORM.optimization_record_id,
                [record_id],
                include,
                exclude,
                missing_ok,
            )
            return sorted(traj[0], key=lambda x: x["position"])

    def query(
        self,
        query_data: OptimizationQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[OptimizationRecordDict]]:

        and_query = []
        need_spspec_join = False
        need_optspec_join = False

        if query_data.program is not None:
            and_query.append(OptimizationSpecificationORM.program.in_(query_data.program))
            need_optspec_join = True
        if query_data.singlepoint_program is not None:
            and_query.append(SinglePointSpecificationORM.program.in_(query_data.singlepoint_program))
            need_spspec_join = True
        if query_data.singlepoint_method is not None:
            and_query.append(SinglePointSpecificationORM.method.in_(query_data.singlepoint_method))
            need_spspec_join = True
        if query_data.singlepoint_basis is not None:
            and_query.append(SinglePointSpecificationORM.basis.in_(query_data.singlepoint_basis))
            need_spspec_join = True
        if query_data.singlepoint_keywords_id is not None:
            and_query.append(SinglePointSpecificationORM.keywords_id.in_(query_data.singlepoint_keywords_id))
            need_spspec_join = True
        if query_data.initial_molecule_id is not None:
            and_query.append(OptimizationProcedureORM.initial_molecule_id.in_(query_data.initial_molecule_id))
        if query_data.final_molecule_id is not None:
            and_query.append(OptimizationProcedureORM.final_molecule_id.in_(query_data.final_molecule_id))

        stmt = select(OptimizationProcedureORM)

        # If we need the singlepoint spec, we also need the optimization spec
        if need_optspec_join or need_spspec_join:
            stmt = stmt.join(OptimizationProcedureORM.specification).options(
                contains_eager(OptimizationProcedureORM.specification)
            )

        if need_spspec_join:
            stmt = stmt.join(OptimizationSpecificationORM.singlepoint_specification).options(
                contains_eager(
                    OptimizationProcedureORM.specification, OptimizationSpecificationORM.singlepoint_specification
                )
            )

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=OptimizationProcedureORM,
            query_data=query_data,
            session=session,
        )

    def add(
        self,
        opt_spec: OptimizationInputSpecification,
        initial_molecules: Sequence[Union[int, Molecule]],
        tag: Optional[str] = None,
        priority: PriorityEnum = PriorityEnum.normal,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new optimization calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        optspec
            Specification for the calculations
        initial_molecules
            Molecules to compute using the specification
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

        # All will have the same required programs
        required_programs = {opt_spec.program: None, opt_spec.singlepoint_specification.program: None}

        with self.root_socket.optional_session(session, False) as session:

            # First, add the specification
            spec_meta, spec_id = self.add_specification(opt_spec, session=session)
            if not spec_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add specification: " + spec_meta.error_description
                    ),
                    [],
                )

            # Now the molecules
            mol_meta, mol_ids = self.root_socket.molecules.add_mixed(initial_molecules, session=session)
            if not mol_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add all molecules: " + mol_meta.error_description
                    ),
                    [],
                )

            all_orm = []
            all_molecules = self.root_socket.molecules.get(mol_ids, session=session)

            # Load the spec as is from the db
            # May be different due to normalization, or because keywords were passed by
            # ID where we need the full keywords
            real_spec = self.get_specification(spec_id, session=session)

            model = {"method": real_spec["singlepoint_specification"]["method"]}
            if real_spec["singlepoint_specification"]["basis"]:
                model["basis"] = real_spec["singlepoint_specification"]["basis"]

            # Add the singlepoint program to the optimization keywords
            opt_keywords = real_spec["keywords"].copy()
            opt_keywords["program"] = real_spec["singlepoint_specification"]["program"]

            for mol_data in all_molecules:

                qcschema_input = OptimizationInput(
                    input_specification=QCInputSpecification(
                        model=model, keywords=real_spec["singlepoint_specification"]["keywords"]["values"]
                    ),
                    initial_molecule=mol_data,
                    keywords=opt_keywords,
                    protocols=real_spec["protocols"],
                )

                task_orm = TaskQueueORM(
                    tag=tag,
                    priority=priority,
                    required_programs=required_programs,
                    spec={
                        "function": "qcengine.compute_procedure",
                        "args": [qcschema_input.dict(), real_spec["program"]],
                        "kwargs": {},
                    },
                )

                opt_orm = OptimizationProcedureORM(
                    specification_id=spec_id,
                    initial_molecule_id=mol_data["id"],
                    status=RecordStatusEnum.waiting,
                    task=task_orm,
                )

                all_orm.append(opt_orm)

            meta, ids = insert_general(
                session,
                all_orm,
                (OptimizationProcedureORM.specification_id, OptimizationProcedureORM.initial_molecule_id),
                (OptimizationProcedureORM.id,),
            )
            return meta, [x[0] for x in ids]

    def update_completed(
        self, session: Session, record_orm: OptimizationProcedureORM, result: OptimizationResult, manager_name: str
    ) -> None:

        # Add the final molecule
        meta, final_mol_id = self.root_socket.molecules.add([result.final_molecule])
        if not meta.success:
            raise RuntimeError("Unable to add final molecule: " + meta.error_string)

        # Insert the trajectory
        record_orm.trajectory = []
        for position, traj_result in enumerate(result.trajectory):
            traj_orm = self.root_socket.records.singlepoint.insert_completed(session, traj_result)
            assoc_orm = OptimizationTrajectoryORM(singlepoint_record=traj_orm)
            record_orm.trajectory.append(assoc_orm)

        # Update the fields themselves
        record_orm.final_molecule_id = final_mol_id[0]
        record_orm.energies = result.energies
        record_orm.extras = result.extras

    def insert_completed(
        self,
        session: Session,
        result: OptimizationResult,
    ) -> ResultORM:

        raise RuntimeError("Not yet implemented")

    def recreate_task(
        self,
        record_orm: OptimizationProcedureORM,
        tag: Optional[str] = None,
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> None:

        opt_spec = record_orm.specification
        sp_spec = opt_spec.singlepoint_specification
        init_molecule = Molecule(**record_orm.initial_molecule.dict())
        required_programs = {opt_spec.program: None, opt_spec.singlepoint_specification.program: None}

        model = {"method": sp_spec.method}
        if sp_spec.basis:
            model["basis"] = sp_spec.basis

        # Add the singlepoint program to the optimization keywords
        opt_keywords = opt_spec.keywords.copy()
        opt_keywords["program"] = sp_spec.program

        qcschema_input = OptimizationInput(
            input_specification=QCInputSpecification(model=model, keywords=sp_spec.keywords.values),
            initial_molecule=init_molecule,
            keywords=opt_keywords,
            protocols=opt_spec.protocols,
        )

        task_orm = TaskQueueORM(
            tag=tag,
            priority=priority,
            required_programs=required_programs,
            spec={
                "function": "qcengine.compute_procedure",
                "args": [qcschema_input.dict(), record_orm.specification.program],
                "kwargs": {},
            },
        )

        record_orm.task = task_orm
