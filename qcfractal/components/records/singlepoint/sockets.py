from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from qcelemental.models import AtomicInput, AtomicResult, Molecule
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from qcfractal.components.records.singlepoint.db_models import SinglePointSpecificationORM, ResultORM
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.db_socket.helpers import get_general, insert_general
from qcfractal.interface.models import RecordStatusEnum, PriorityEnum
from qcfractal.portal.components.records.singlepoint import (
    SinglePointSpecification,
)
from qcfractal.portal.metadata_models import InsertMetadata
from ..helpers import create_compute_history_entry, wavefunction_helper

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union

    SinglePointSpecificationDict = Dict[str, Any]
    SinglePointRecordDict = Dict[str, Any]


class SinglePointRecordSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.record

    def get_specification(
        self, id: int, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[SinglePointSpecificationDict]:
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
                session, SinglePointSpecificationORM, SinglePointSpecificationORM.id, [id], None, None, None, missing_ok
            )[0]

    def add_specification(
        self, sp_spec: SinglePointSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        protocols_dict = sp_spec.protocols.dict(exclude_defaults=True)
        basis = "" if sp_spec.basis is None else sp_spec.basis

        with self.root_socket.optional_session(session, False) as session:
            # Add the keywords
            meta, kw_ids = self.root_socket.keywords.add_mixed([sp_spec.keywords], session=session)
            if not meta.success:
                return (
                    InsertMetadata(
                        error_description="Unable to add keywords: " + meta.error_string,
                    ),
                    None,
                )

            stmt = (
                insert(SinglePointSpecificationORM)
                .values(
                    program=sp_spec.program,
                    driver=sp_spec.driver,
                    method=sp_spec.method,
                    basis=basis,
                    keywords_id=kw_ids[0],
                    protocols=protocols_dict,
                )
                .on_conflict_do_nothing()
                .returning(SinglePointSpecificationORM.id)
            )

            r = session.execute(stmt).scalar_one_or_none()
            if r is not None:
                return InsertMetadata(inserted_idx=[0]), r
            else:
                # Specification was already existing
                stmt = select(SinglePointSpecificationORM.id).filter_by(
                    program=sp_spec.program,
                    driver=sp_spec.driver,
                    method=sp_spec.method,
                    basis=basis,
                    keywords_id=kw_ids[0],
                    protocols=protocols_dict,
                )

                r = session.execute(stmt).scalar_one()
                return InsertMetadata(existing_idx=[0]), r

    def add(
        self,
        sp_spec: SinglePointSpecification,
        molecules: Sequence[Union[int, Molecule]],
        tag: Optional[str] = None,
        priority: PriorityEnum = PriorityEnum.normal,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new singlepoint calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        sp_spec
            Specification for the single point calculations
        molecules
            Molecules to compute using the specification
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules in the SinglePointInput.
        """

        # All will have the same required programs
        required_programs = {sp_spec.program: None}

        with self.root_socket.optional_session(session, False) as session:

            # First, add the specification
            spec_meta, spec_id = self.add_specification(sp_spec, session=session)
            if not spec_meta.success:
                return spec_meta, []

            # Now the molecules
            mol_meta, mol_ids = self.root_socket.molecules.add_mixed(molecules, session=session)
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

            model = {"method": real_spec["method"]}
            if real_spec["basis"]:
                model["basis"] = real_spec["basis"]

            for mol_data in all_molecules:

                qcschema_input = AtomicInput(
                    driver=real_spec["driver"],
                    model=model,
                    molecule=mol_data,
                    keywords=real_spec["keywords"]["values"],
                    protocols=real_spec["protocols"],
                )

                task_orm = TaskQueueORM(
                    tag=tag,
                    priority=priority,
                    required_programs=required_programs,
                    spec={
                        "function": "qcengine.compute",
                        "args": [qcschema_input.dict(), real_spec["program"]],
                        "kwargs": {},
                    },
                )

                sp_orm = ResultORM(
                    specification_id=spec_id,
                    molecule_id=mol_data["id"],
                    status=RecordStatusEnum.waiting,
                    task=task_orm,
                    protocols={},  # TODO - remove me
                )

                all_orm.append(sp_orm)

            meta, ids = insert_general(
                session, all_orm, (ResultORM.specification_id, ResultORM.molecule_id), (ResultORM.id,)
            )
            return meta, [x[0] for x in ids]

    def update_completed(self, session: Session, record_orm: ResultORM, result: AtomicResult, manager_name: str):
        # Get the outputs & status, storing in the history orm
        history_orm = create_compute_history_entry(result)

        history_orm.manager_name = manager_name

        record_orm.compute_history.insert(0, history_orm)

        # Update the fields themselves
        record_orm.return_result = record_orm.return_result
        record_orm.properties = result.properties.dict(encoding="json")
        record_orm.extras = result.extras

        record_orm.status = history_orm.status
        record_orm.manager_name = manager_name
        record_orm.modified_on = datetime.utcnow()
        record_orm.wavefunction = wavefunction_helper(result.wavefunction)

        # We have to flush to prevent a circular dependency when
        # adding the history_orm to the "latest" field
        session.flush()
        record_orm.compute_history_latest = history_orm
