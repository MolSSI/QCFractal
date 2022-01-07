from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from qcelemental.models import AtomicInput, AtomicResult
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import contains_eager

from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.db_socket.helpers import get_general, insert_general
from qcportal.keywords import KeywordSet
from qcportal.metadata_models import InsertMetadata, QueryMetadata
from qcportal.molecules import Molecule
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.singlepoint import (
    WavefunctionProperties,
    SinglepointSpecification,
    SinglepointInputSpecification,
    SinglepointQueryBody,
)
from .db_models import SinglepointSpecificationORM, SinglepointRecordORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union, Iterable

    SinglepointSpecificationDict = Dict[str, Any]
    SinglepointRecordDict = Dict[str, Any]


def wavefunction_helper(wavefunction: Optional[WavefunctionProperties]) -> Optional[WavefunctionStoreORM]:
    _wfn_all_fields = set(WavefunctionProperties.__fields__.keys())
    logger = logging.getLogger(__name__)

    if wavefunction is None:
        return None

    wfn_dict = wavefunction.dict()
    available_keys = set(wfn_dict.keys())

    # Extra fields are trimmed as we have a column *per* wavefunction structure.
    extra_fields = available_keys - _wfn_all_fields
    if extra_fields:
        logger.warning(f"Too much wavefunction data for result, removing extra data: {extra_fields}")
        available_keys &= _wfn_all_fields

    wavefunction_save = {k: wfn_dict[k] for k in available_keys}
    wfn_prop = WavefunctionProperties(**wavefunction_save)
    return WavefunctionStoreORM.from_model(wfn_prop)


class SinglepointRecordSocket(BaseRecordSocket):
    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        return []

    def get_specification(
        self, spec_id: int, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[SinglepointSpecificationDict]:
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
                session, SinglepointSpecificationORM, SinglepointSpecificationORM.id, [spec_id], None, None, missing_ok
            )[0]

    def add_specification(
        self, sp_spec: SinglepointInputSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        protocols_dict = sp_spec.protocols.dict(exclude_defaults=True)

        # TODO - if error_correction is manually specified as the default, then it will be an empty dict
        if "error_correction" in protocols_dict:
            erc = protocols_dict["error_correction"]
            pol = erc.get("policies", dict())
            if len(pol) == 0:
                erc.pop("policies", None)
            if len(erc) == 0:
                protocols_dict.pop("error_correction")

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
                insert(SinglepointSpecificationORM)
                .values(
                    program=sp_spec.program,
                    driver=sp_spec.driver,
                    method=sp_spec.method,
                    basis=basis,
                    keywords_id=kw_ids[0],
                    protocols=protocols_dict,
                )
                .on_conflict_do_nothing()
                .returning(SinglepointSpecificationORM.id)
            )

            r = session.execute(stmt).scalar_one_or_none()
            if r is not None:
                return InsertMetadata(inserted_idx=[0]), r
            else:
                # Specification was already existing
                stmt = select(SinglepointSpecificationORM.id).filter_by(
                    program=sp_spec.program,
                    driver=sp_spec.driver,
                    method=sp_spec.method,
                    basis=basis,
                    keywords_id=kw_ids[0],
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
    ) -> List[Optional[SinglepointRecordDict]]:
        """
        Obtain a singlepoint record with specified IDs

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
            SinglepointRecordORM, record_id, include, exclude, missing_ok, session=session
        )

    def query(
        self,
        query_data: SinglepointQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[SinglepointRecordDict]]:

        and_query = []
        need_join = False

        if query_data.program is not None:
            and_query.append(SinglepointSpecificationORM.program.in_(query_data.program))
            need_join = True
        if query_data.driver is not None:
            and_query.append(SinglepointSpecificationORM.driver.in_(query_data.driver))
            need_join = True
        if query_data.method is not None:
            and_query.append(SinglepointSpecificationORM.method.in_(query_data.method))
            need_join = True
        if query_data.basis is not None:
            and_query.append(SinglepointSpecificationORM.basis.in_(query_data.basis))
            need_join = True
        if query_data.keywords_id is not None:
            and_query.append(SinglepointSpecificationORM.keywords_id.in_(query_data.keywords_id))
            need_join = True
        if query_data.molecule_id is not None:
            and_query.append(SinglepointRecordORM.molecule_id.in_(query_data.molecule_id))

        stmt = select(SinglepointRecordORM)

        if need_join:
            stmt = stmt.join(SinglepointRecordORM.specification).options(
                contains_eager(SinglepointRecordORM.specification)
            )

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=SinglepointRecordORM,
            query_data=query_data,
            session=session,
        )

    def generate_task_specification(self, record_orm: SinglepointRecordORM) -> Dict[str, Any]:

        specification = record_orm.specification
        molecule = record_orm.molecule.dict()

        model = {"method": specification.method}
        if specification.basis:
            model["basis"] = specification.basis

        qcschema_input = AtomicInput(
            driver=specification.driver,
            model=model,
            molecule=molecule,
            keywords=specification.keywords.values,
            protocols=specification.protocols,
        )

        return {
            "function": "qcengine.compute",
            "args": [qcschema_input.dict(), specification.program],
            "kwargs": {},
        }

    def add(
        self,
        sp_spec: SinglepointSpecification,
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
            order of the input molecules
        """

        # tags should be lowercase
        if tag is not None:
            tag = tag.lower()

        with self.root_socket.optional_session(session, False) as session:

            # First, add the specification
            spec_meta, spec_id = self.add_specification(sp_spec, session=session)
            if not spec_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add specification: " + spec_meta.error_description
                    ),
                    [],
                )

            # Now the molecules
            mol_meta, mol_ids = self.root_socket.molecules.add_mixed(molecules, session=session)
            if not mol_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add all molecules: " + mol_meta.error_description
                    ),
                    [],
                )

            # Get the spec orm. The full orm will be needed for create_task
            stmt = select(SinglepointSpecificationORM).where(SinglepointSpecificationORM.id == spec_id)
            spec_orm = session.execute(stmt).scalar_one()

            all_orm = []

            for mid in mol_ids:
                sp_orm = SinglepointRecordORM(
                    is_service=False,
                    specification=spec_orm,
                    specification_id=spec_id,
                    molecule_id=mid,
                    status=RecordStatusEnum.waiting,
                )

                self.create_task(sp_orm, tag, priority)
                all_orm.append(sp_orm)

            meta, ids = insert_general(
                session,
                all_orm,
                (SinglepointRecordORM.specification_id, SinglepointRecordORM.molecule_id),
                (SinglepointRecordORM.id,),
            )
            return meta, [x[0] for x in ids]

    def update_completed_task(
        self, session: Session, record_orm: SinglepointRecordORM, result: AtomicResult, manager_name: str
    ) -> None:
        # Update the fields themselves
        record_orm.return_result = result.return_result
        record_orm.properties = result.properties.dict(encoding="json")
        record_orm.wavefunction = wavefunction_helper(result.wavefunction)
        record_orm.extras = result.extras

    def insert_complete_record(
        self,
        session: Session,
        result: AtomicResult,
    ) -> SinglepointRecordORM:

        sp_spec = SinglepointInputSpecification(
            program=result.provenance.creator.lower(),
            driver=result.driver,
            method=result.model.method,
            basis=result.model.basis,
            keywords=KeywordSet(values=result.keywords),
            protocols=result.protocols,
        )

        spec_meta, spec_id = self.add_specification(sp_spec, session=session)
        if not spec_meta.success:
            raise RuntimeError(
                "Aborted single point insertion - could not add specification: " + spec_meta.error_description
            )

        mol_meta, mol_ids = self.root_socket.molecules.add([result.molecule], session=session)
        if not mol_meta.success:
            raise RuntimeError(
                "Aborted single point insertion - could not add molecule: " + spec_meta.error_description
            )

        record_orm = SinglepointRecordORM()
        record_orm.is_service = False
        record_orm.specification_id = spec_id
        record_orm.molecule_id = mol_ids[0]
        record_orm.status = RecordStatusEnum.complete
        record_orm.return_result = result.return_result
        record_orm.properties = result.properties.dict(encoding="json")
        record_orm.wavefunction = wavefunction_helper(result.wavefunction)
        record_orm.extras = result.extras

        record_orm.protocols = {}  # TODO - REMOVE ME

        session.add(record_orm)
        session.flush()
        return record_orm
