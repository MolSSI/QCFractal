from __future__ import annotations

import logging
from typing import List, Dict, Tuple, Optional, Iterable, Sequence, Any, Union, TYPE_CHECKING

import tabulate
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import array_agg, aggregate_order_by
from sqlalchemy.orm import contains_eager

from qcfractal import __version__ as qcfractal_version
from qcfractal.components.records.singlepoint.db_models import QCSpecificationORM, SinglepointRecordORM
from qcfractal.components.records.sockets import BaseRecordSocket
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcportal.metadata_models import InsertMetadata, QueryMetadata
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputTypeEnum
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.reaction import (
    ReactionQCSpecification,
    ReactionQueryBody,
)
from .db_models import ReactionStoichiometryORM, ReactionComponentORM, ReactionRecordORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket

    QCSpecificationDict = Dict[str, Any]
    ReactionRecordDict = Dict[str, Any]


class ReactionRecordSocket(BaseRecordSocket):

    # Used by the base class
    record_orm = ReactionRecordORM
    specification_orm = QCSpecificationORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        stmt = select(
            ReactionComponentORM.reaction_id.label("parent_id"),
            ReactionComponentORM.singlepoint_id.label("child_id"),
        )
        return [stmt]

    def add_specification(
        self, qc_spec: ReactionQCSpecification, *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.singlepoint.add_specification(qc_spec=qc_spec, session=session)

    def query(
        self,
        query_data: ReactionQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ReactionRecordDict]]:

        and_query = []
        need_spec_join = False
        need_stoic_join = False

        if query_data.program is not None:
            and_query.append(QCSpecificationORM.program.in_(query_data.program))
            need_spec_join = True
        if query_data.driver is not None:
            and_query.append(QCSpecificationORM.driver.in_(query_data.driver))
            need_spec_join = True
        if query_data.method is not None:
            and_query.append(QCSpecificationORM.method.in_(query_data.method))
            need_spec_join = True
        if query_data.basis is not None:
            and_query.append(QCSpecificationORM.basis.in_(query_data.basis))
            need_spec_join = True
        if query_data.molecule_id is not None:
            and_query.append(ReactionStoichiometryORM.molecule_id.in_(query_data.molecule_id))
            need_stoic_join = True

        stmt = select(ReactionRecordORM)

        if need_spec_join:
            stmt = stmt.join(ReactionRecordORM.specification).options(contains_eager(ReactionRecordORM.specification))

        if need_stoic_join:
            stmt = stmt.join(ReactionRecordORM.stoichiometries).options(
                contains_eager(ReactionRecordORM.stoichiometries)
            )

        stmt = stmt.where(*and_query)

        return self.root_socket.records.query_base(
            stmt=stmt,
            orm_type=SinglepointRecordORM,
            query_data=query_data,
            session=session,
        )

    def add_internal(
        self,
        stoichiometries: Sequence[Iterable[Tuple[float, int]]],  # coefficient, molecule_id
        qc_spec_id: int,
        tag: str,
        priority: PriorityEnum,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:

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
                        aggregate_order_by(
                            ReactionStoichiometryORM.molecule_id, ReactionStoichiometryORM.molecule_id.asc()
                        )
                    ).label("molecule_ids"),
                )
                .join(
                    ReactionStoichiometryORM,
                    ReactionStoichiometryORM.reaction_id == ReactionRecordORM.id,
                )
                .group_by(ReactionRecordORM.id)
                .cte()
            )

            for idx, rxn_mols in enumerate(stoichiometries):
                # sort molecule ids by increasing ids, and remove duplicates
                rxn_mol_ids = sorted(set(x[1] for x in rxn_mols))

                # does this exist?
                stmt = select(init_mol_cte.c.id)
                stmt = stmt.where(init_mol_cte.c.specification_id == qc_spec_id)
                stmt = stmt.where(init_mol_cte.c.molecule_ids == rxn_mol_ids)
                existing = session.execute(stmt).scalars().first()

                if not existing:
                    stoich_orm = [
                        ReactionStoichiometryORM(molecule_id=mid, coefficient=coeff) for coeff, mid in rxn_mols
                    ]

                    rxn_orm = ReactionRecordORM(
                        is_service=True,
                        specification_id=qc_spec_id,
                        stoichiometries=stoich_orm,
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
        qc_spec: ReactionQCSpecification,
        tag: str,
        priority: PriorityEnum,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new reaction calculations

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        If session is specified, changes are not committed to to the database, but the session is flushed.

        Parameters
        ----------
        stoichiometries
            Coefficient and molecules (objects or ids) to compute using the specification
        qc_spec
            Specification for the calculations
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input stoichiometries
        """

        with self.root_socket.optional_session(session, False) as session:

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

    def initialize_service(self, session: Session, service_orm: ServiceQueueORM) -> None:
        rxn_orm: ReactionRecordORM = service_orm.record

        output = "\n\nCreated reaction. Molecules:\n\n"
        table_rows = [
            (f"{m.coefficient:.8f}", m.molecule.identifiers["molecular_formula"], m.molecule_id)
            for m in rxn_orm.stoichiometries
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

        # Always update with the current provenance
        rxn_orm.compute_history[-1].provenance = {
            "creator": "qcfractal",
            "version": qcfractal_version,
            "routine": "qcfractal.services.reaction",
        }

        required_mols = [x.molecule_id for x in rxn_orm.stoichiometries]

        complete_tasks = service_orm.dependencies
        complete_mols = [x.record.molecule_id for x in complete_tasks]

        mols_to_compute = list(set(required_mols) - set(complete_mols))

        service_orm.dependencies = []

        if mols_to_compute:
            qc_spec_id = rxn_orm.specification_id
            meta, sp_ids = self.root_socket.records.singlepoint.add_internal(
                mols_to_compute, qc_spec_id, service_orm.tag, service_orm.priority, session=session
            )

            for mol_id, sp_id in zip(mols_to_compute, sp_ids):

                svc_dep = ServiceDependencyORM(record_id=sp_id, extras={})
                rxn_component = ReactionComponentORM(molecule_id=mol_id, singlepoint_id=sp_id)

                service_orm.dependencies.append(svc_dep)
                rxn_orm.components.append(rxn_component)

            output = "\nSubmitted singlepoint calculations:\n"
            output += tabulate.tabulate(zip(mols_to_compute, sp_ids), headers=["molecule id", "singlepoint id"])

        else:
            output = "\n\n" + "*" * 80 + "\n"
            output += "All reaction components are complete!\n\n"

            output += "Reaction results:\n"
            table = []
            total = 0.0

            coef_map = {x.molecule_id: x.coefficient for x in rxn_orm.stoichiometries}

            for component in rxn_orm.components:
                mol_form = component.molecule.identifiers["molecular_formula"]
                mol_id = component.molecule_id
                energy = component.energy
                coefficient = coef_map[mol_id]

                table_row = [mol_id, mol_form, component.singlepoint_id, component.energy, coefficient]
                table.append(table_row)

                total += coefficient * energy

            output += tabulate.tabulate(
                table, headers=["molecule id", "molecule", "singlepoint id", "energy (hartree)", "coefficient"]
            )
            output += "\n\n"
            output += f"Weighted total energy: {total:.16f} hartrees"

            rxn_orm.total_energy = total

        stdout_orm = rxn_orm.compute_history[-1].get_output(OutputTypeEnum.stdout)
        stdout_orm.append(output)

        return len(mols_to_compute) == 0
