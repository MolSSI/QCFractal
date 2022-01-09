from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcelemental.models import OptimizationInput, OptimizationResult
from qcelemental.models.procedures import QCInputSpecification
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import contains_eager

from qcfractal.components.records.singlepoint.db_models import SinglepointRecordORM, SinglepointSpecificationORM
from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.db_socket.helpers import get_general, insert_general, get_general_multi
from qcportal.metadata_models import InsertMetadata, QueryMetadata
from qcportal.molecules import Molecule
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQueryBody,
)
from qcportal.records.singlepoint import (
    SinglepointDriver,
)
from .db_models import OptimizationSpecificationORM, OptimizationRecordORM, OptimizationTrajectoryORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union

    OptimizationSpecificationDict = Dict[str, Any]
    OptimizationRecordDict = Dict[str, Any]


class OptimizationRecordSocket(BaseRecordSocket):
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

    def get_specification(
        self, spec_id: int, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[OptimizationSpecificationDict]:
        """
        Obtain a specification with the specified ID

        If missing_ok is False, then any ids that are missing in the database will raise an exception.
        Otherwise, the returned id will be None

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
        spec_id
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
                session,
                OptimizationSpecificationORM,
                OptimizationSpecificationORM.id,
                [spec_id],
                None,
                None,
                missing_ok,
            )[0]

    def add_specification(
        self, opt_spec: OptimizationInputSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:

        protocols_dict = opt_spec.protocols.dict(exclude_defaults=True)

        with self.root_socket.optional_session(session, False) as session:
            # Add the singlepoint specification
            # Make double sure the driver is deferred
            opt_spec.singlepoint_specification.driver = SinglepointDriver.deferred
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
            OptimizationRecordORM, record_id, include, exclude, missing_ok, session=session
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
                OptimizationTrajectoryORM.optimization_id,
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

    def generate_task_specification(self, record_orm: OptimizationRecordORM) -> Dict[str, Any]:

        specification = record_orm.specification
        initial_molecule = record_orm.initial_molecule.dict()

        model = {"method": specification.singlepoint_specification.method}
        if specification.singlepoint_specification.basis:
            model["basis"] = specification.singlepoint_specification.basis

        # Add the singlepoint program to the optimization keywords
        opt_keywords = specification.keywords.copy()
        opt_keywords["program"] = specification.singlepoint_specification.program

        qcschema_input = OptimizationInput(
            input_specification=QCInputSpecification(
                model=model, keywords=specification.singlepoint_specification.keywords.values
            ),
            initial_molecule=initial_molecule,
            keywords=opt_keywords,
            protocols=specification.protocols,
        )

        return {
            "function": "qcengine.compute_procedure",
            "args": [qcschema_input.dict(), specification.program],
            "kwargs": {},
        }

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
        opt_spec
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

            # Get the spec orm. The full orm will be needed for create_task
            stmt = select(OptimizationSpecificationORM).where(OptimizationSpecificationORM.id == spec_id)
            spec_orm = session.execute(stmt).scalar_one()

            all_orm = []
            all_molecules = self.root_socket.molecules.get(mol_ids, session=session)

            for mol_data in all_molecules:

                opt_orm = OptimizationRecordORM(
                    is_service=False,
                    specification=spec_orm,
                    specification_id=spec_id,
                    initial_molecule_id=mol_data["id"],
                    status=RecordStatusEnum.waiting,
                )

                self.create_task(opt_orm, tag, priority)
                all_orm.append(opt_orm)

            meta, ids = insert_general(
                session,
                all_orm,
                (OptimizationRecordORM.specification_id, OptimizationRecordORM.initial_molecule_id),
                (OptimizationRecordORM.id,),
            )
            return meta, [x[0] for x in ids]

    def update_completed_task(
        self, session: Session, record_orm: OptimizationRecordORM, result: OptimizationResult, manager_name: str
    ) -> None:

        # Add the final molecule
        meta, final_mol_id = self.root_socket.molecules.add([result.final_molecule])
        if not meta.success:
            raise RuntimeError("Unable to add final molecule: " + meta.error_string)

        # Insert the trajectory
        record_orm.trajectory = []
        for position, traj_result in enumerate(result.trajectory):
            traj_orm = self.root_socket.records.singlepoint.insert_complete_record(session, traj_result)
            assoc_orm = OptimizationTrajectoryORM(singlepoint_record=traj_orm)
            record_orm.trajectory.append(assoc_orm)

        # Update the fields themselves
        record_orm.final_molecule_id = final_mol_id[0]
        record_orm.energies = result.energies
        record_orm.extras = result.extras

    def insert_complete_record(
        self,
        session: Session,
        result: OptimizationResult,
    ) -> SinglepointRecordORM:

        raise RuntimeError("Not yet implemented")
