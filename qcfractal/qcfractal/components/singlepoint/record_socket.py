from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcelemental.models import AtomicResult as QCEl_AtomicResult
from sqlalchemy import select, or_
from sqlalchemy.orm import lazyload, joinedload, defer, undefer, defaultload, load_only, selectinload

from qcfractal.db_socket.helpers import insert_general
from qcportal.compression import CompressionEnum, compress
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.serialization import convert_numpy_recursive
from qcportal.singlepoint import (
    QCSpecification,
    WavefunctionProperties,
    SinglepointQueryFilters,
    SinglepointInput,
    SinglepointMultiInput,
)
from qcportal.utils import hash_dict, is_included
from .record_db_models import QCSpecificationORM, SinglepointRecordORM, WavefunctionORM
from ..record_socket import BaseRecordSocket

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Union

# Meaningless, but unique to singlepoints
singlepoint_insert_lock_id = 14000
singlepoint_spec_insert_lock_id = 14001


class SinglepointRecordSocket(BaseRecordSocket):
    """
    Socket for handling singlepoint computations
    """

    # Used by the base class
    record_orm = SinglepointRecordORM
    record_input_type = SinglepointInput
    record_multi_input_type = SinglepointMultiInput

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        return []

    def generate_task_specifications(self, session: Session, record_ids: Sequence[int]) -> List[Dict[str, Any]]:
        stmt = select(SinglepointRecordORM).filter(SinglepointRecordORM.id.in_(record_ids))
        stmt = stmt.options(load_only(SinglepointRecordORM.id, SinglepointRecordORM.extras))
        stmt = stmt.options(
            lazyload("*"), joinedload(SinglepointRecordORM.molecule), selectinload(SinglepointRecordORM.specification)
        )

        record_orms = session.execute(stmt).scalars().all()

        task_specs = {}

        for record_orm in record_orms:
            specification = record_orm.specification
            molecule = record_orm.molecule.model_dict()

            model = {"method": specification.method}

            # Empty basis string should be None in the model
            if specification.basis:
                model["basis"] = specification.basis
            else:
                model["basis"] = None

            qcschema_input = dict(
                schema_name="qcschema_input",
                schema_version=1,
                id=str(record_orm.id),  # str for compatibility
                driver=specification.driver,
                model=model,
                molecule=convert_numpy_recursive(molecule, flatten=True),  # TODO - remove after all data is converted
                keywords=specification.keywords,
                protocols=specification.protocols,
                extras=record_orm.extras if record_orm.extras else {},
            )

            task_specs[record_orm.id] = {
                "function": "qcengine.compute",
                "function_kwargs": {
                    "input_data": qcschema_input,
                    "program": specification.program,
                },
            }

        if set(record_ids) != set(task_specs.keys()):
            raise RuntimeError("Did not generate all task specs for all singlepoint records?")

        # Return in the input order
        return [task_specs[rid] for rid in record_ids]

    def create_wavefunction_orm(self, wavefunction: WavefunctionProperties) -> WavefunctionORM:
        """
        Convert a QCElemental wavefunction into a wavefunction ORM
        """

        wfn_dict = wavefunction.dict(encoding="json")
        cdata, ctype, clevel = compress(wfn_dict, CompressionEnum.zstd)

        return WavefunctionORM(compression_type=ctype, compression_level=clevel, data=cdata)

    def update_completed_task(self, session: Session, record_id: int, result: QCEl_AtomicResult) -> None:
        # Update the fields themselves
        if result.wavefunction:
            wavefunction_orm = self.create_wavefunction_orm(result.wavefunction)
            wavefunction_orm.record_id = record_id
            session.add(wavefunction_orm)

    def insert_complete_schema_v1(
        self,
        session: Session,
        results: Sequence[QCEl_AtomicResult],
    ) -> List[SinglepointRecordORM]:

        ret = []

        mols = []
        qc_specs = []

        for result in results:
            mols.append(result.molecule)

            qc_spec = QCSpecification(
                program=result.provenance.creator.lower(),
                driver=result.driver,
                method=result.model.method,
                basis=result.model.basis,
                keywords=result.keywords,
                protocols=result.protocols,
            )
            qc_specs.append(qc_spec)

        meta, spec_ids = self.root_socket.records.singlepoint.add_specifications(qc_specs, session=session)
        if not meta.success:
            raise RuntimeError("Aborted single point insertion - could not add specifications: " + meta.error_string)

        meta, mol_ids = self.root_socket.molecules.add(mols, session=session)
        if not meta.success:
            raise RuntimeError("Aborted single point insertion - could not add molecules: " + meta.error_string)

        for result, mol_id, spec_id in zip(results, mol_ids, spec_ids):
            record_orm = SinglepointRecordORM(
                specification_id=spec_id,
                molecule_id=mol_id,
                status=RecordStatusEnum.complete,
            )

            if result.wavefunction:
                record_orm.wavefunction = self.create_wavefunction_orm(result.wavefunction)

            ret.append(record_orm)

        return ret

    def add_specifications(
        self, qc_specs: Sequence[QCSpecification], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds specifications for singlepoint calculations to the database, returning their IDs.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Specification IDs are returned in the same order as the input specifications

        Parameters
        ----------
        qc_specs
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

        for qc_spec in qc_specs:
            protocols_dict = qc_spec.protocols.dict(exclude_defaults=True)

            # TODO - if error_correction is manually specified as the default, then it will be an empty dict
            if "error_correction" in protocols_dict:
                erc = protocols_dict["error_correction"]
                pol = erc.get("policies", dict())
                if len(pol) == 0:
                    erc.pop("policies", None)
                if len(erc) == 0:
                    protocols_dict.pop("error_correction")

            qc_spec_dict = qc_spec.dict(exclude={"basis", "protocols"})
            qc_spec_dict["basis"] = "" if qc_spec.basis is None else qc_spec.basis
            qc_spec_dict["protocols"] = protocols_dict
            qc_spec_hash = hash_dict(qc_spec_dict)

            qc_spec_orm = QCSpecificationORM(
                specification_hash=qc_spec_hash,
                program=qc_spec_dict["program"],
                driver=qc_spec_dict["driver"],
                method=qc_spec_dict["method"],
                basis=qc_spec_dict["basis"],
                keywords=qc_spec_dict["keywords"],
                protocols=qc_spec_dict["protocols"],
            )

            to_add.append(qc_spec_orm)

        with self.root_socket.optional_session(session, False) as session:
            meta, ids = insert_general(
                session,
                to_add,
                (QCSpecificationORM.specification_hash,),
                (QCSpecificationORM.id,),
                singlepoint_spec_insert_lock_id,
            )

            return meta, [x[0] for x in ids]

    def add_specification(
        self, qc_spec: QCSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        """
        Adds a single specification for a singlepoint calculation to the database, returning its id.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Parameters
        ----------
        qc_spec
            Specification to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the id of the specification.
        """

        meta, ids = self.add_specifications([qc_spec], session=session)

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
            if is_included("molecule", include, exclude, False):
                options.append(joinedload(SinglepointRecordORM.molecule))
            if is_included("wavefunction", include, exclude, False):
                options.append(joinedload(SinglepointRecordORM.wavefunction).undefer(WavefunctionORM.data))

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
        query_data: SinglepointQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        and_query = []
        need_join = False

        if query_data.program is not None:
            and_query.append(QCSpecificationORM.program.in_(query_data.program))
            need_join = True
        if query_data.driver is not None:
            and_query.append(QCSpecificationORM.driver.in_(query_data.driver))
            need_join = True
        if query_data.method is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.method))
            need_join = True
        if query_data.basis is not None:
            and_query.append(QCSpecificationORM.basis.in_(query_data.basis))
            need_join = True
        if query_data.molecule_id is not None:
            and_query.append(SinglepointRecordORM.molecule_id.in_(query_data.molecule_id))
        if query_data.keywords is not None:
            or_query = []
            for d in query_data.keywords:
                or_query.append(QCSpecificationORM.keywords.comparator.contains(d))
            and_query.append(or_(*or_query))
            need_join = True

        stmt = select(SinglepointRecordORM.id)

        if need_join:
            stmt = stmt.join(SinglepointRecordORM.specification)

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=SinglepointRecordORM,
            query_data=query_data,
            session=session,
        )

    def add_internal(
        self,
        molecule_ids: Sequence[int],
        qc_spec_id: int,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Internal function for adding new singlepoint computations

        This function expects that the molecules and specification are already added to the
        database and that the ids are known.

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        molecule_ids
            IDs of the molecules to run the computation with. One record will be added per molecule.
        qc_spec_id
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

            # Get the spec orm. The full orm will be needed for create_task
            stmt = select(QCSpecificationORM).where(QCSpecificationORM.id == qc_spec_id)
            spec_orm = session.execute(stmt).scalar_one()

            all_orm = []

            for mid in molecule_ids:
                sp_orm = SinglepointRecordORM(
                    is_service=False,
                    specification=spec_orm,
                    specification_id=qc_spec_id,
                    molecule_id=mid,
                    status=RecordStatusEnum.waiting,
                    owner_user_id=owner_user_id,
                    owner_group_id=owner_group_id,
                )

                self.create_task(sp_orm, compute_tag, compute_priority)
                all_orm.append(sp_orm)

            if find_existing:
                meta, ids = insert_general(
                    session,
                    all_orm,
                    (SinglepointRecordORM.specification_id, SinglepointRecordORM.molecule_id),
                    (SinglepointRecordORM.id,),
                    lock_id=singlepoint_insert_lock_id,
                )
                return meta, [x[0] for x in ids]
            else:
                session.add_all(all_orm)
                session.flush()
                meta = InsertMetadata(inserted_idx=list(range(len(all_orm))))

                return meta, [x.id for x in all_orm]

    def add(
        self,
        molecules: Sequence[Union[int, Molecule]],
        qc_spec: QCSpecification,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new singlepoint calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        molecules
            Molecules to compute using the specification
        qc_spec
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
            spec_meta, spec_id = self.add_specification(qc_spec, session=session)
            if not spec_meta.success:
                return (
                    InsertMetadata(
                        error_description="Aborted - could not add specification: " + spec_meta.error_string
                    ),
                    [],
                )

            # Now the molecules
            mol_meta, mol_ids = self.root_socket.molecules.add_mixed(molecules, session=session)
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
        record_input: SinglepointInput,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertCountsMetadata, int]:

        assert isinstance(record_input, SinglepointInput)

        meta, ids = self.add(
            [record_input.molecule],
            record_input.specification,
            compute_tag,
            compute_priority,
            owner_user,
            owner_group,
            find_existing,
        )

        return InsertCountsMetadata.from_insert_metadata(meta), ids[0]

    ####################################################
    # Some stuff to be retrieved for singlepoints
    ####################################################

    def get_wavefunction_metadata(
        self, record_id: int, *, session: Optional[Session] = None
    ) -> Optional[Dict[str, Any]]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(SinglepointRecordORM.wavefunction).options(
                undefer("*"), defaultload("*"), defer(WavefunctionORM.data)
            ),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(SinglepointRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            if rec.wavefunction is None:
                return None
            return rec.wavefunction.model_dict()

    def get_wavefunction_rawdata(
        self, record_id: int, *, session: Optional[Session] = None
    ) -> Tuple[bytes, CompressionEnum]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(SinglepointRecordORM.wavefunction).options(
                undefer(WavefunctionORM.data), undefer(WavefunctionORM.compression_type)
            ),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(SinglepointRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return rec.wavefunction.data, rec.wavefunction.compression_type
