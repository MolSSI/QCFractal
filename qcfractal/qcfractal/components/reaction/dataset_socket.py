from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, literal, insert

from qcfractal.components.dataset_socket import BaseDatasetSocket
from qcfractal.components.reaction.record_db_models import ReactionRecordORM
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.reaction import ReactionDatasetNewEntry, ReactionSpecification
from qcportal.record_models import PriorityEnum
from .dataset_db_models import (
    ReactionDatasetORM,
    ReactionDatasetSpecificationORM,
    ReactionDatasetEntryORM,
    ReactionDatasetStoichiometryORM,
    ReactionDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class ReactionDatasetSocket(BaseDatasetSocket):
    # Used by the base class
    dataset_orm = ReactionDatasetORM
    specification_orm = ReactionDatasetSpecificationORM
    entry_orm = ReactionDatasetEntryORM
    record_item_orm = ReactionDatasetRecordItemORM
    record_orm = ReactionRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: ReactionSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.reaction.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[ReactionDatasetNewEntry]):
        all_entries = []
        for entry in new_entries:
            # stoichiometries = list of tuples
            molecules = [x[1] if isinstance(x, tuple) else x.molecule for x in entry.stoichiometries]

            meta, mol_ids = self.root_socket.molecules.add_mixed(molecules, session=session)

            new_stoich_orm = []

            for coeff, mid in zip(entry.stoichiometries, mol_ids):
                new_stoich_orm.append(
                    ReactionDatasetStoichiometryORM(
                        coefficient=coeff[0] if isinstance(coeff, tuple) else coeff.coefficient,
                        molecule_id=mid,
                    )
                )

            new_ent = ReactionDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                stoichiometries=new_stoich_orm,
                additional_keywords=entry.additional_keywords,
                attributes=entry.attributes,
            )

            all_entries.append(new_ent)

        return all_entries

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[ReactionDatasetEntryORM],
        spec_orm: Iterable[ReactionDatasetSpecificationORM],
        existing_records: Iterable[Tuple[str, str]],
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
    ) -> InsertCountsMetadata:

        n_inserted = 0
        n_existing = 0

        # Weed out any with additional keywords
        special_entries = [x for x in entry_orm if x.additional_keywords]
        normal_entries = [x for x in entry_orm if not x.additional_keywords]

        # Normal entries - just let it rip
        for spec in spec_orm:
            new_normal_entries = [x for x in normal_entries if (x.name, spec.name) not in existing_records]
            stoichiometries = [[(x.coefficient, x.molecule_id) for x in y.stoichiometries] for y in new_normal_entries]

            meta, rxn_ids = self.root_socket.records.reaction.add_internal(
                stoichiometries=stoichiometries,
                rxn_spec_id=spec.specification_id,
                compute_tag=compute_tag,
                compute_priority=compute_priority,
                owner_user_id=owner_user_id,
                owner_group_id=owner_group_id,
                find_existing=find_existing,
                session=session,
            )

            for entry, oid in zip(new_normal_entries, rxn_ids):
                rec = ReactionDatasetRecordItemORM(
                    dataset_id=dataset_id, entry_name=entry.name, specification_name=spec.name, record_id=oid
                )
                session.add(rec)

            n_inserted += meta.n_inserted
            n_existing += meta.n_existing

        # Now the ones with additional keywords
        for spec in spec_orm:
            spec_obj = spec.specification.to_model(ReactionSpecification)
            spec_input_dict = spec_obj.dict()

            for entry in special_entries:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_spec = copy.deepcopy(spec_input_dict)
                new_spec["keywords"].update(entry.additional_keywords)

                stoichiometry = [(x.coefficient, x.molecule_id) for x in entry.stoichiometries]

                meta, rxn_ids = self.root_socket.records.reaction.add(
                    stoichiometries=[stoichiometry],
                    rxn_spec=ReactionSpecification(**new_spec),
                    compute_tag=compute_tag,
                    compute_priority=compute_priority,
                    owner_user=owner_user_id,
                    owner_group=owner_group_id,
                    find_existing=find_existing,
                    session=session,
                )

                if meta.success:
                    rec = ReactionDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=rxn_ids[0],
                    )
                    session.add(rec)

                n_inserted += meta.n_inserted
                n_existing += meta.n_existing

        return InsertCountsMetadata(n_inserted=n_inserted, n_existing=n_existing)

    def _copy_entries(
        self,
        session: Session,
        source_dataset_id: int,
        destination_dataset_id: int,
        entry_names: Optional[Iterable[str]] = None,
    ):

        select_stmt = select(
            literal(destination_dataset_id),
            self.entry_orm.name,
            self.entry_orm.comment,
            self.entry_orm.additional_keywords,
            self.entry_orm.attributes,
        )

        select_stmt = select_stmt.where(self.entry_orm.dataset_id == source_dataset_id)

        if entry_names is not None:
            select_stmt = select_stmt.where(self.entry_orm.name.in_(entry_names))

        stmt = insert(self.entry_orm)
        stmt = stmt.from_select(
            [
                self.entry_orm.dataset_id,
                self.entry_orm.name,
                self.entry_orm.comment,
                self.entry_orm.additional_keywords,
                self.entry_orm.attributes,
            ],
            select_stmt,
        )

        session.execute(stmt)

        # Now do the stoichiometries (stored in a separate table)
        select_stmt = select(
            literal(destination_dataset_id),
            ReactionDatasetStoichiometryORM.entry_name,
            ReactionDatasetStoichiometryORM.molecule_id,
            ReactionDatasetStoichiometryORM.coefficient,
        )

        select_stmt = select_stmt.where(ReactionDatasetStoichiometryORM.dataset_id == source_dataset_id)

        if entry_names is not None:
            select_stmt = select_stmt.where(ReactionDatasetStoichiometryORM.entry_name.in_(entry_names))

        stmt = insert(ReactionDatasetStoichiometryORM)
        stmt = stmt.from_select(
            [
                ReactionDatasetStoichiometryORM.dataset_id,
                ReactionDatasetStoichiometryORM.entry_name,
                ReactionDatasetStoichiometryORM.molecule_id,
                ReactionDatasetStoichiometryORM.coefficient,
            ],
            select_stmt,
        )

        session.execute(stmt)
