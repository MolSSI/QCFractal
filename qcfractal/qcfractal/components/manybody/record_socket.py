from __future__ import annotations

import contextlib
import importlib
import io
import logging
import textwrap
from typing import List, Dict, Tuple, Optional, Sequence, Any, Union, TYPE_CHECKING

import tabulate
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import array_agg, aggregate_order_by
from sqlalchemy.orm import defer, undefer, lazyload, joinedload, selectinload

from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM
from qcfractal.db_socket.helpers import insert_general
from qcportal.exceptions import MissingDataError
from qcportal.manybody import (
    ManybodySpecification,
    ManybodyQueryFilters,
    ManybodyInput,
    ManybodyMultiInput,
)
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum, RecordStatusEnum, OutputTypeEnum
from qcportal.utils import chunk_iterable, hash_dict
from .record_db_models import (
    ManybodyClusterORM,
    ManybodyRecordORM,
    ManybodySpecificationORM,
    ManybodySpecificationLevelsORM,
)
from ..record_socket import BaseRecordSocket
from ..record_utils import append_output

_qcm_spec = importlib.util.find_spec("qcmanybody")

if _qcm_spec is not None:
    qcmanybody = importlib.util.module_from_spec(_qcm_spec)
    _qcm_spec.loader.exec_module(qcmanybody)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket


# Meaningless, but unique to manybody
manybody_insert_lock_id = 14500
manybody_insert_spec_lock_id = 14501


def _get_qcmanybody_core(
    mb_orm: ManybodyRecordORM,
) -> Tuple[qcmanybody.ManyBodyCore, Dict[str, ManybodySpecificationLevelsORM]]:
    init_mol: Molecule = mb_orm.initial_molecule.to_model(Molecule)

    qcm_levels = {}
    level_spec_map = {}
    sp_id_map = {}

    for nb, lvl in sorted(mb_orm.specification.levels.items()):
        if nb == -1:
            nb = "supersystem"

        sp_spec = lvl.singlepoint_specification
        if sp_spec.id in sp_id_map:
            sp_name = sp_id_map[sp_spec.id]
        else:
            test_name = f"{sp_spec.program}/{sp_spec.method}/{sp_spec.basis}"
            sp_name = test_name

            # duplicates
            i = 0
            while sp_name in qcm_levels:
                i += 1
                sp_name = f"{test_name}_{i}"

            sp_id_map[sp_spec.id] = sp_name

        qcm_levels[nb] = sp_name
        level_spec_map[sp_name] = lvl

    qcm = qcmanybody.ManyBodyCore(
        molecule=init_mol,
        levels=qcm_levels,
        bsse_type=[qcmanybody.BsseEnum[x] for x in mb_orm.specification.bsse_correction],
        return_total_data=mb_orm.specification.keywords.get("return_total_data", False),
        supersystem_ie_only=mb_orm.specification.keywords.get("supersystem_ie_only", False),
        embedding_charges=None,
    )

    return qcm, level_spec_map


class ManybodyRecordSocket(BaseRecordSocket):
    """
    Socket for handling manybody computations
    """

    # Used by the base class
    record_orm = ManybodyRecordORM
    record_input_type = ManybodyInput
    record_multi_input_type = ManybodyMultiInput

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    def available(self) -> bool:
        return _qcm_spec is not None

    @staticmethod
    def get_children_select() -> List[Any]:
        stmt = select(
            ManybodyClusterORM.manybody_id.label("parent_id"),
            ManybodyClusterORM.singlepoint_id.label("child_id"),
        )
        return [stmt]

    def initialize_service(self, session: Session, service_orm: ServiceQueueORM) -> None:
        mb_orm: ManybodyRecordORM = service_orm.record

        output = "\n\nCreated manybody calculation\n"
        output += "qcmanybody version: " + qcmanybody.__version__ + "\n\n"

        output += "-" * 80 + "\nSpecification:\n\n"

        table_rows = []

        for k, v in mb_orm.specification.keywords.items():
            table_rows.append((k, v))

        if mb_orm.specification.program != "qcmanybody":
            raise RuntimeError(f"Unknown program: {mb_orm.specification.program}")

        table_rows.append(("bsse_correction", str(mb_orm.specification.bsse_correction)))
        output += tabulate.tabulate(table_rows, tablefmt="plain")
        output += "\n\n"

        init_mol: Molecule = mb_orm.initial_molecule.to_model(Molecule)
        output += f"Initial molecule: formula={init_mol.get_molecular_formula()} id={mb_orm.initial_molecule_id}\n"
        output += f"Initial molecule has {len(init_mol.fragments)} fragments\n"

        # Create a computer instance to get what calculations we need
        qcm, spec_map = _get_qcmanybody_core(mb_orm)

        output += "\n\n" + "-" * 80 + "\nModel Chemistries/Specifications:\n\n"
        for name, lvl_spec in spec_map.items():
            output += f"{name}:\n"
            output += textwrap.indent(
                tabulate.tabulate(lvl_spec.singlepoint_specification.model_dict().items(), tablefmt="plain"), "    "
            )
            output += "\n"

        output += "\n\n" + "-" * 80 + "\nLevels:\n\n"
        for level, mc_name in qcm.levels.items():
            output += f" {level:>13}: {mc_name}\n"
        output += "\n\n"

        output += "\n\n" + "-" * 80 + "\nComputation count:\n\n"
        table_rows = []
        for mc, compute_dict in qcm.compute_map.items():
            for nb, frags in compute_dict["all"].items():
                table_rows.append((f"{mc} {nb}-mer", len(frags)))
        output += tabulate.tabulate(table_rows, headers=["n-body", "count"])

        # Add what we need to compute to the database
        table_rows = []

        for mol_batch in chunk_iterable(qcm.iterate_molecules(), 400):
            to_add = [x[2] for x in mol_batch]
            meta, mol_ids = self.root_socket.molecules.add(to_add, session=session)
            if not meta.success:
                raise RuntimeError("Unable to add molecules to the database: " + meta.error_string)

            for (mc_level, label, molecule), mol_id in zip(mol_batch, mol_ids):
                # Decode the label given by qcmanybody
                _, frag, bas = qcmanybody.delabeler(label)

                mb_cluster_orm = ManybodyClusterORM(
                    mc_level=mc_level,
                    fragments=frag,
                    basis=bas,
                    molecule_id=mol_id,
                )
                table_rows.append((mc_level, frag, bas, mol_id, molecule.get_molecular_formula(), molecule.get_hash()))
                mb_orm.clusters.append(mb_cluster_orm)

        output += "\n\nMolecules to compute\n\n"
        output += tabulate.tabulate(
            table_rows, headers=["model chemistry", "fragments", "basis", "molecule_id", "formula", "hash"]
        )

        append_output(session, mb_orm, OutputTypeEnum.stdout, output)

    def iterate_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ):
        mb_orm: ManybodyRecordORM = service_orm.record

        # Always update with the current provenance
        mb_orm.compute_history[-1].provenance = {
            "creator": "qcmanybody",
            "version": qcmanybody.__version__,
            "routine": "qcmanybody",
        }

        service_orm.dependencies = []

        submitted = []

        qcm, spec_map = _get_qcmanybody_core(mb_orm)
        done_sp_ids = set(c.singlepoint_id for c in mb_orm.clusters if c.singlepoint_id is not None)

        # what we need to submit, mapped by single spec id

        clusters_to_submit = {}
        for c in mb_orm.clusters:
            if c.singlepoint_id is not None:
                continue
            clusters_to_submit.setdefault(c.mc_level, [])
            clusters_to_submit[c.mc_level].append(c)

        for mc_level, clusters in clusters_to_submit.items():
            mol_ids = [c.molecule_id for c in clusters]
            meta, sp_ids = self.root_socket.records.singlepoint.add_internal(
                mol_ids,
                spec_map[mc_level].singlepoint_specification_id,
                service_orm.compute_tag,
                service_orm.compute_priority,
                mb_orm.owner_user_id,
                mb_orm.owner_group_id,
                service_orm.find_existing,
                session=session,
            )

            for cluster, sp_id in zip(clusters, sp_ids):
                cluster.singlepoint_id = sp_id
                submitted.append((cluster, sp_id))

                # Add as a dependency to the service, but only if it's not done yet
                if sp_id not in done_sp_ids:
                    svc_dep = ServiceDependencyORM(record_id=sp_id, extras={})
                    service_orm.dependencies.append(svc_dep)
                    done_sp_ids.add(sp_id)

        if len(submitted) != 0:
            output = f"\nSubmitted {len(submitted)} singlepoint calculations "
            append_output(session, mb_orm, OutputTypeEnum.stdout, output)
            return False

        output = "\n\n" + "*" * 80 + "\n"
        output += "All manybody singlepoint computations are complete!\n\n"

        output += "=" * 20 + "\nSinglepoint results\n" + "=" * 20 + "\n\n"

        # Make a nice output table
        table_rows = []
        for cluster in mb_orm.clusters:
            mol_id = cluster.molecule_id

            energy = cluster.singlepoint_record.properties["return_energy"]
            table_row = [cluster.mc_level, cluster.fragments, cluster.basis, energy, mol_id, cluster.singlepoint_id]
            table_rows.append(table_row)

        output += tabulate.tabulate(
            table_rows,
            headers=["model chemistry", "fragments", "basis", "energy (hartree)", "molecule id", "singlepoint id"],
            floatfmt=".10f",
        )

        # Analyze the actual results
        component_results = {}
        for cluster in mb_orm.clusters:
            mc_level = cluster.mc_level
            label = qcmanybody.labeler(mc_level, cluster.fragments, cluster.basis)
            energy = cluster.singlepoint_record.properties["return_energy"]

            component_results.setdefault(label, {})
            component_results[label]["energy"] = energy

        # Swallow any output
        qcmb_stdout = io.StringIO()

        with contextlib.redirect_stdout(qcmb_stdout):
            mb_orm.properties = qcm.analyze(component_results)

        output += "\n\n" + "=" * 40 + "\nManybody expansion results\n" + "=" * 40 + "\n"
        output += mb_orm.properties.pop("stdout")
        append_output(session, mb_orm, OutputTypeEnum.stdout, output)

        # We are done!
        return True

    def add_specifications(
        self, mb_specs: Sequence[ManybodySpecification], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds a specification for a manybody service to the database, returning its id.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Parameters
        ----------
        mb_specs
            Sequence of specifications to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the IDs of the specification.
        """

        # Because of how we handle levels, we do this the opposite of other record types - we add one at a time

        all_metadata = []
        all_ids = []

        with self.root_socket.optional_session(session) as session:
            for mb_spec in mb_specs:
                meta, spec_id = self.add_specification(mb_spec, session=session)

                if not meta.success:
                    return (
                        InsertMetadata(error_description="Unable to add manybody specification: " + meta.error_string),
                        [],
                    )

                all_metadata.append(meta)
                all_ids.append(spec_id)

        return InsertMetadata.merge(all_metadata), all_ids

    def add_specification(
        self, mb_spec: ManybodySpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        """
        Adds a specification for a manybody service to the database, returning its id.

        If an identical specification exists, then no insertion takes place and the id of the existing
        specification is returned.

        Parameters
        ----------
        mb_spec
            Specification to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the id of the specification.
        """

        mb_kw_dict = mb_spec.keywords.dict()

        mb_spec_dict = {
            "program": mb_spec.program,
            "bsse_correction": sorted(mb_spec.bsse_correction),
            "keywords": mb_kw_dict,
            "protocols": {},
        }
        mb_spec_hash = hash_dict(mb_spec_dict)

        # Map 'supersystem' to -1
        # The reverse (mapping -1 to 'supersystem') happens in the specification orm model_dict function
        levels = mb_spec.levels.copy()
        if "supersystem" in levels:
            levels[-1] = levels.pop("supersystem")

        with self.root_socket.optional_session(session) as session:
            # add all singlepoint specifications

            # Level to singlepoint spec id
            level_spec_id_map: Dict[int, int] = {}
            for k, v in levels.items():
                meta, sp_spec_id = self.root_socket.records.singlepoint.add_specification(qc_spec=v, session=session)

                if not meta.success:
                    return (
                        InsertMetadata(
                            error_description="Unable to add singlepoint specification: " + meta.error_string,
                        ),
                        None,
                    )

                level_spec_id_map[k] = sp_spec_id

            # Now the full manybody specification. Lock due to query + insert
            session.execute(select(func.pg_advisory_xact_lock(manybody_insert_spec_lock_id))).scalar()

            # Create a cte with the specification + levels
            mb_spec_cte = (
                select(
                    ManybodySpecificationORM.id,
                    ManybodySpecificationORM.specification_hash,
                    array_agg(
                        aggregate_order_by(
                            ManybodySpecificationLevelsORM.singlepoint_specification_id,
                            ManybodySpecificationLevelsORM.singlepoint_specification_id.asc(),
                        )
                    ).label("singlepoint_ids"),
                    array_agg(
                        aggregate_order_by(
                            ManybodySpecificationLevelsORM.level,
                            ManybodySpecificationLevelsORM.level.asc(),
                        )
                    ).label("levels"),
                )
                .join(
                    ManybodySpecificationLevelsORM,
                    ManybodySpecificationLevelsORM.manybody_specification_id == ManybodySpecificationORM.id,
                )
                .group_by(ManybodySpecificationORM.id)
                .cte()
            )

            stmt = select(mb_spec_cte.c.id)
            stmt = stmt.where(mb_spec_cte.c.specification_hash == mb_spec_hash)
            stmt = stmt.where(mb_spec_cte.c.levels == sorted(level_spec_id_map.keys()))
            stmt = stmt.where(mb_spec_cte.c.singlepoint_ids == sorted(level_spec_id_map.values()))

            existing_id = session.execute(stmt).scalar_one_or_none()

            if existing_id is not None:
                return InsertMetadata(existing_idx=[0]), existing_id

            # Does not exist. Insert new
            mb_levels_orms = {}
            for level, sp_spec_id in level_spec_id_map.items():
                mb_levels_orms[level] = ManybodySpecificationLevelsORM(
                    level=level, singlepoint_specification_id=sp_spec_id
                )

            new_orm = ManybodySpecificationORM(
                program=mb_spec.program,
                bsse_correction=mb_spec.bsse_correction,
                keywords=mb_kw_dict,
                specification_hash=mb_spec_hash,
                levels=mb_levels_orms,
                protocols={},
            )

            session.add(new_orm)
            session.flush()
            return InsertMetadata(inserted_idx=[0]), new_orm.id

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
            if "**" in include or "initial_molecule" in include:
                options.append(joinedload(ManybodyRecordORM.initial_molecule))
            if "**" in include or "clusters" in include:
                options.append(selectinload(ManybodyRecordORM.clusters))

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
        query_data: ManybodyQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        and_query = []
        need_spec_join = False
        need_qcspec_join = False

        if query_data.program is not None:
            and_query.append(ManybodySpecificationORM.program.in_(query_data.program))
            need_spec_join = True
        if query_data.qc_program is not None:
            and_query.append(QCSpecificationORM.program.in_(query_data.qc_program))
            need_qcspec_join = True
        if query_data.qc_method is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.qc_method))
            need_qcspec_join = True
        if query_data.qc_basis is not None:
            and_query.append(QCSpecificationORM.basis.in_(query_data.qc_basis))
            need_qcspec_join = True
        if query_data.initial_molecule_id is not None:
            and_query.append(ManybodyRecordORM.initial_molecule_id.in_(query_data.initial_molecule_id))

        stmt = select(ManybodyRecordORM.id)

        if need_spec_join or need_qcspec_join:
            stmt = stmt.join(ManybodyRecordORM.specification)

        if need_qcspec_join:
            stmt = stmt.join(ManybodySpecificationORM.levels).join(
                ManybodySpecificationLevelsORM.singlepoint_specification
            )

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=ManybodyRecordORM,
            query_data=query_data,
            session=session,
        )

    def add_internal(
        self,
        initial_molecule_ids: Sequence[int],
        mb_spec_id: int,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Internal function for adding new manybody computations

        This function expects that the molecules and specification are already added to the
        database and that the ids are known.

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        initial_molecule_ids
            IDs of the initial molecules
        mb_spec_id
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

            all_orm = []

            for mid in initial_molecule_ids:
                mb_orm = ManybodyRecordORM(
                    is_service=True,
                    specification_id=mb_spec_id,
                    initial_molecule_id=mid,
                    status=RecordStatusEnum.waiting,
                    owner_user_id=owner_user_id,
                    owner_group_id=owner_group_id,
                )

                self.create_service(mb_orm, compute_tag, compute_priority, find_existing)
                all_orm.append(mb_orm)

            if find_existing:
                meta, ids = insert_general(
                    session,
                    all_orm,
                    (ManybodyRecordORM.specification_id, ManybodyRecordORM.initial_molecule_id),
                    (ManybodyRecordORM.id,),
                    lock_id=manybody_insert_lock_id,
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
        mb_spec: ManybodySpecification,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new manybody calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        Parameters
        ----------
        initial_molecules
            Initial molecules (objects or ids) to compute using the specification
        mb_spec
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
            spec_meta, spec_id = self.add_specification(mb_spec, session=session)
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
        record_input: ManybodyInput,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertCountsMetadata, int]:

        assert isinstance(record_input, ManybodyInput)

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
    # Some stuff to be retrieved for manybodys
    ####################################################

    def get_clusters(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(ManybodyRecordORM.clusters).options(
                undefer("*"), joinedload(ManybodyClusterORM.molecule).options(undefer("*"))
            ),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(ManybodyRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return [x.model_dict() for x in rec.clusters]
