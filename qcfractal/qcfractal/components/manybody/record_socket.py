from __future__ import annotations

import itertools
import logging
import math
from typing import List, Dict, Tuple, Optional, Sequence, Any, Union, Set, TYPE_CHECKING

import tabulate
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import defer, undefer, lazyload, joinedload

from qcfractal import __version__ as qcfractal_version
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM
from qcfractal.db_socket.helpers import insert_general
from qcportal.exceptions import MissingDataError
from qcportal.manybody import (
    BSSECorrectionEnum,
    ManybodyKeywords,
    ManybodySpecification,
    ManybodyQueryFilters,
)
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum, RecordStatusEnum, OutputTypeEnum
from qcportal.utils import hash_dict
from .record_db_models import ManybodyClusterORM, ManybodyRecordORM, ManybodySpecificationORM
from ..record_socket import BaseRecordSocket

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket


# Meaningless, but unique to manybody
manybody_insert_lock_id = 14500


def nCr(n: int, r: int) -> int:
    """
    Compute the binomial coefficient n! / (k! * (n-k)!)
    """

    # TODO: available in python 3.8 as math.comb
    return math.factorial(n) // (math.factorial(r) * math.factorial(n - r))


def analyze_results(mb_orm: ManybodyRecordORM):
    keywords = ManybodyKeywords(**mb_orm.specification.keywords)

    # Total number of fragments present on the molecule
    total_frag = len(mb_orm.initial_molecule.fragments)

    # Group clusters by nbody
    # For CP, this only includes the calculations done in the full basis
    clusters = {}
    for c in mb_orm.clusters:
        if keywords.bsse_correction == BSSECorrectionEnum.none:
            nbody = len(c.fragments)
            clusters.setdefault(nbody, [])
            clusters[nbody].append(c)
        elif keywords.bsse_correction == BSSECorrectionEnum.cp and len(c.basis) > 1:
            nbody = len(c.fragments)
            clusters.setdefault(nbody, [])
            clusters[nbody].append(c)

    # Total energy for each nbody cluster. This is the energy calculated
    # by the singlepoint multiplied by its degeneracy
    cluster_energy: Dict[int, float] = {}

    for nbody, v in clusters.items():
        cluster_energy[nbody] = sum(c.degeneracy * c.singlepoint_record.properties["return_energy"] for c in v)

    # Calculate CP correction
    bsse = 0.0
    if keywords.bsse_correction == BSSECorrectionEnum.cp:
        monomer_clusters = [c for c in mb_orm.clusters if len(c.fragments) == 1 and len(c.basis) == 1]
        monomer_energy = sum(c.degeneracy * c.singlepoint_record.properties["return_energy"] for c in monomer_clusters)
        bsse = cluster_energy[1] - monomer_energy

    # Total energies
    total_energy_through = {}

    for n in cluster_energy.keys():
        # If entire molecule was calculated, then add that
        if n == total_frag:
            total_energy_through[n] = cluster_energy[n]
        elif n == 1:
            total_energy_through[n] = cluster_energy[n]
        else:
            total_energy_through[n] = 0.0
            for nbody in range(1, n + 1):
                sign = (-1) ** (n - nbody)
                take_nk = nCr(total_frag - nbody - 1, n - nbody)
                total_energy_through[n] += take_nk * sign * cluster_energy[nbody]

    # Apply CP correction
    if keywords.bsse_correction == BSSECorrectionEnum.cp:
        total_energy_through = {k: v - bsse for k, v in total_energy_through.items()}

    # Contributions to interaction energy
    energy_contrib = {}
    energy_contrib[1] = 0.0
    for n in total_energy_through:
        if n != 1:
            energy_contrib[n] = total_energy_through[n] - total_energy_through[n - 1]

    # Interaction energy
    interaction_energy = {}
    for n in total_energy_through:
        interaction_energy[n] = total_energy_through[n] - total_energy_through[1]

    results = {
        "cluster_energy": cluster_energy,
        "total_energy_through": total_energy_through,
        "interaction_energy": interaction_energy,
        "energy_contrib": energy_contrib,
    }

    mb_orm.results = results


def build_mbe_clusters(mol: Molecule, keywords: ManybodyKeywords) -> List[Tuple[Set[int], Set[int], Molecule]]:
    """
    Fragments a larger molecule into clusters

    Parameters
    ----------
    mol
        Molecule to fragment
    keywords
        Keywords that control the fragmenting

    Returns
    -------
    :
        A list of tuples with three elements -
        (1) Set of fragment indices (2) Set of basis indices (3) Fragment molecule
    """

    # List: (fragments, basis, Molecule)
    # fragments and basis are sequences
    ret: List[Tuple[Set[int], Set[int], Molecule]] = []

    if len(mol.fragments) < 2:
        raise RuntimeError("manybody service: Molecule must have at least two fragments")

    max_nbody = keywords.max_nbody

    if max_nbody is None:
        max_nbody = len(mol.fragments)
    else:
        max_nbody = min(max_nbody, len(mol.fragments))

    # Build some info
    allfrag = set(range(max_nbody))

    # Loop over the nbody (the number of bodies to include. 1 = monomers, 2 = dimers)
    for nbody in range(1, max_nbody):
        for frag_idx in itertools.combinations(allfrag, nbody):
            frag_idx = set(frag_idx)
            if keywords.bsse_correction == BSSECorrectionEnum.none:
                frag_mol = mol.get_fragment(frag_idx, orient=True, group_fragments=True)
                ret.append((frag_idx, frag_idx, frag_mol))
            elif keywords.bsse_correction == BSSECorrectionEnum.cp:
                ghost = list(set(allfrag) - set(frag_idx))
                frag_mol = mol.get_fragment(frag_idx, ghost, orient=True, group_fragments=True)
                ret.append((frag_idx, allfrag, frag_mol))
            else:
                raise RuntimeError(f"Unknown BSSE correction method: {keywords.bsse_correction}")

    # Include full molecule as well
    if max_nbody >= len(mol.fragments):
        ret.append((allfrag, allfrag, mol))

    # Always include monomer in monomer basis for CP
    if keywords.bsse_correction == BSSECorrectionEnum.cp:
        for frag_idx in allfrag:
            frag_mol = mol.get_fragment([frag_idx], orient=True, group_fragments=True)
            ret.append(({frag_idx}, {frag_idx}, frag_mol))

    return ret


class ManybodyRecordSocket(BaseRecordSocket):
    """
    Socket for handling manybody computations
    """

    # Used by the base class
    record_orm = ManybodyRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

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

        output += "-" * 80 + "\nManybody Keywords:\n\n"
        spec: ManybodySpecification = mb_orm.specification.to_model(ManybodySpecification)
        table_rows = sorted(spec.keywords.dict().items())
        output += tabulate.tabulate(table_rows, headers=["keyword", "value"])
        output += "\n\n" + "-" * 80 + "\nQC Specification:\n\n"
        table_rows = sorted(spec.singlepoint_specification.dict().items())
        output += tabulate.tabulate(table_rows, headers=["keyword", "value"])
        output += "\n\n"

        init_mol: Molecule = mb_orm.initial_molecule.to_model(Molecule)

        output += f"Initial molecule: formula={init_mol.get_molecular_formula()} id={mb_orm.initial_molecule_id}\n"
        output += f"Initial molecule has {len(init_mol.fragments)} fragments\n"

        # Fragment the initial molecule into clusters
        keywords = ManybodyKeywords(**mb_orm.specification.keywords)
        mol_clusters = build_mbe_clusters(init_mol, keywords)

        output += f"Molecule is split into into {len(mol_clusters)} separate clusters:\n\n"

        # Group by nbody and count for output
        mol_clusters_nbody = {}
        for mc in mol_clusters:
            nbody = len(mc[0])
            mol_clusters_nbody.setdefault(nbody, 0)
            mol_clusters_nbody[nbody] += 1

        table_rows = [(k, v) for k, v in sorted(mol_clusters_nbody.items())]
        output += tabulate.tabulate(table_rows, headers=["n-body", "count"])
        output += "\n\n"

        # Add the manybody molecules/clusters to the db
        nbody_mols = [x[2] for x in mol_clusters]
        meta, mol_ids = self.root_socket.molecules.add(nbody_mols)

        if not meta.success:
            raise RuntimeError("Unable to add molecules to the database: " + meta.error_string)

        # We do unique ids only
        # Some manybody calculations will have identical molecules
        # Think of single-atom dimers or something. There will only be one monomer
        done_ids = set()

        table_rows = []
        for (frag_idx, basis_idx, frag_mol), mol_id in zip(mol_clusters, mol_ids):
            if mol_id in done_ids:
                continue

            done_ids.add(mol_id)
            degen = mol_ids.count(mol_id)
            frag_idx = sorted(frag_idx)
            basis_idx = sorted(basis_idx)

            new_mb_orm = ManybodyClusterORM(fragments=frag_idx, basis=basis_idx, molecule_id=mol_id, degeneracy=degen)

            mb_orm.clusters.append(new_mb_orm)

            table_rows.append((degen, frag_mol.get_molecular_formula(), mol_id, frag_idx, basis_idx))

        # Sort rows by nbody (# of fragments), the degeneracy descending, then molecule id
        table_rows = sorted(table_rows, key=lambda x: (len(x[3]), -x[0], x[2]))
        output += tabulate.tabulate(table_rows, headers=["degeneracy", "molecule", "molecule id", "fragments", "basis"])
        output += "\n\n"

        self.root_socket.records.append_output(session, mb_orm, OutputTypeEnum.stdout, output)

    def iterate_service(
        self,
        session: Session,
        service_orm: ServiceQueueORM,
    ):
        mb_orm: ManybodyRecordORM = service_orm.record

        # Always update with the current provenance
        mb_orm.compute_history[-1].provenance = {
            "creator": "qcfractal",
            "version": qcfractal_version,
            "routine": "qcfractal.services.manybody",
        }

        # Grab all the clusters for the computation and them map them to molecule ID
        clusters = mb_orm.clusters
        clusters_by_mol = {c.molecule_id: c for c in clusters}

        service_orm.dependencies = []

        # What molecules/clusters we still have to do
        mols_to_compute = [c.molecule_id for c in clusters if c.singlepoint_id is None]

        if mols_to_compute:
            sp_spec_id = mb_orm.specification.singlepoint_specification_id
            meta, sp_ids = self.root_socket.records.singlepoint.add_internal(
                mols_to_compute,
                sp_spec_id,
                service_orm.tag,
                service_orm.priority,
                mb_orm.owner_user_id,
                mb_orm.owner_group_id,
                service_orm.find_existing,
                session=session,
            )

            output = f"\nSubmitted {len(sp_ids)} singlepoint calculations "
            output += f"({meta.n_inserted} new, {meta.n_existing} existing):\n\n"

            for mol_id, sp_id in zip(mols_to_compute, sp_ids):
                svc_dep = ServiceDependencyORM(record_id=sp_id, extras={})

                cluster_orm = clusters_by_mol[mol_id]

                # Assign the singlepoint id to the cluster
                assert cluster_orm.singlepoint_id is None
                cluster_orm.singlepoint_id = sp_id

                service_orm.dependencies.append(svc_dep)

            table_rows = sorted(zip(mols_to_compute, sp_ids))
            output += tabulate.tabulate(table_rows, headers=["molecule id", "singlepoint id"])

        else:
            output = "\n\n" + "*" * 80 + "\n"
            output += "All manybody singlepoint computations are complete!\n\n"

            output += "Singlepoint results:\n"

            # Map molecule_id -> singlepoint record
            result_map = {c.molecule_id: c.singlepoint_record for c in clusters}

            # Make a nice output table
            table_rows = []
            for component in mb_orm.clusters:
                mol_id = component.molecule_id
                mol_form = component.molecule.identifiers["molecular_formula"]

                energy = component.singlepoint_record.properties["return_energy"]
                table_row = [mol_id, component.singlepoint_id, mol_form, energy]
                table_rows.append(table_row)

                result_map[mol_id] = component.singlepoint_record

            output += tabulate.tabulate(
                table_rows, headers=["molecule id", "singlepoint id", "molecule", "energy (hartree)"], floatfmt=".10f"
            )

            # Create the results of the manybody calculation
            analyze_results(mb_orm)

            # Make a results table
            r = mb_orm.results
            nb_keys = sorted(r["total_energy_through"].keys())
            table_rows = [
                (
                    nbody,
                    r["total_energy_through"][nbody],
                    r["interaction_energy"][nbody],
                    r["energy_contrib"][nbody],
                )
                for nbody in nb_keys
            ]

            output += "\n\n\n\n" + "=" * 80 + "\n"
            output += "Final energy results (in hartrees)\n" + "=" * 80 + "\n\n"
            output += tabulate.tabulate(
                table_rows,
                headers=["\nnbody", "Total Energy  \nthrough n-body", "\nInteraction Energy", "\nContrib to IE"],
                floatfmt="6.10f",
            )

        self.root_socket.records.append_output(session, mb_orm, OutputTypeEnum.stdout, output)

        return len(mols_to_compute) == 0

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

        kw_dict = mb_spec.keywords.dict()
        kw_hash = hash_dict(kw_dict)

        with self.root_socket.optional_session(session) as session:
            meta, sp_spec_id = self.root_socket.records.singlepoint.add_specification(
                qc_spec=mb_spec.singlepoint_specification, session=session
            )

            if not meta.success:
                return (
                    InsertMetadata(
                        error_description="Unable to add singlepoint specification: " + meta.error_string,
                    ),
                    None,
                )

            stmt = (
                insert(ManybodySpecificationORM)
                .values(
                    program=mb_spec.program,
                    singlepoint_specification_id=sp_spec_id,
                    keywords=kw_dict,
                    keywords_hash=kw_hash,
                )
                .on_conflict_do_nothing()
                .returning(ManybodySpecificationORM.id)
            )

            r = session.execute(stmt).scalar_one_or_none()
            if r is not None:
                return InsertMetadata(inserted_idx=[0]), r
            else:
                # Specification was already existing
                stmt = select(ManybodySpecificationORM.id).filter_by(
                    program=mb_spec.program,
                    singlepoint_specification_id=sp_spec_id,
                    keywords_hash=kw_hash,
                )

                r = session.execute(stmt).scalar_one()
                return InsertMetadata(existing_idx=[0]), r

    def query(
        self,
        query_data: ManybodyQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        Query manybody records

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
            A list of record ids that were found in the database.
        """

        and_query = []
        need_spec_join = False
        need_qcspec_join = False

        if query_data.program is not None:
            and_query.append(ManybodySpecificationORM.program.in_(query_data.program))
            need_spec_join = True
        if query_data.qc_program is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.qc_program))
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
            stmt = stmt.join(ManybodySpecificationORM.singlepoint_specification)

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
        tag: str,
        priority: PriorityEnum,
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
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
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

        tag = tag.lower()

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

                self.create_service(mb_orm, tag, priority, find_existing)
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
        tag: str,
        priority: PriorityEnum,
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
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
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
                mol_ids, spec_id, tag, priority, owner_user_id, owner_group_id, find_existing, session=session
            )

    ####################################################
    # Some stuff to be retrieved for manybodys
    ####################################################

    def get_clusters(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        options = [lazyload("*"), defer("*"), joinedload(ManybodyRecordORM.clusters).options(undefer("*"))]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(ManybodyRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return [x.model_dict() for x in rec.clusters]
