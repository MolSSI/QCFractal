from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, literal, insert

from qcfractal.components.dataset_socket import BaseDatasetSocket
from qcfractal.components.torsiondrive.record_db_models import TorsiondriveRecordORM
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.record_models import PriorityEnum
from qcportal.torsiondrive import TorsiondriveDatasetNewEntry, TorsiondriveSpecification
from .dataset_db_models import (
    TorsiondriveDatasetORM,
    TorsiondriveDatasetSpecificationORM,
    TorsiondriveDatasetEntryORM,
    TorsiondriveDatasetMoleculeORM,
    TorsiondriveDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class TorsiondriveDatasetSocket(BaseDatasetSocket):
    # Used by the base class
    dataset_orm = TorsiondriveDatasetORM
    specification_orm = TorsiondriveDatasetSpecificationORM
    entry_orm = TorsiondriveDatasetEntryORM
    record_item_orm = TorsiondriveDatasetRecordItemORM
    record_orm = TorsiondriveRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: TorsiondriveSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.torsiondrive.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[TorsiondriveDatasetNewEntry]):
        all_entries = []
        for entry in new_entries:
            meta, mol_ids = self.root_socket.molecules.add_mixed(entry.initial_molecules, session=session)

            new_ent_mols = [TorsiondriveDatasetMoleculeORM(molecule_id=mid) for mid in mol_ids]

            new_ent = TorsiondriveDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                additional_keywords=entry.additional_keywords,
                additional_optimization_keywords=entry.additional_optimization_keywords,
                attributes=entry.attributes,
                initial_molecules_assoc=new_ent_mols,
            )

            all_entries.append(new_ent)

        return all_entries

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[TorsiondriveDatasetEntryORM],
        spec_orm: Iterable[TorsiondriveDatasetSpecificationORM],
        existing_records: Iterable[Tuple[str, str]],
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
    ) -> InsertCountsMetadata:

        n_inserted = 0
        n_existing = 0

        for spec in spec_orm:
            td_spec_obj = spec.specification.to_model(TorsiondriveSpecification)
            td_spec_input_dict = td_spec_obj.dict()

            for entry in entry_orm:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_td_spec = copy.deepcopy(td_spec_input_dict)
                new_td_spec["keywords"].update(entry.additional_keywords)
                new_td_spec["optimization_specification"]["keywords"].update(entry.additional_optimization_keywords)

                td_spec = TorsiondriveSpecification(
                    optimization_specification=new_td_spec["optimization_specification"],
                    keywords=new_td_spec["keywords"],
                )

                meta, td_ids = self.root_socket.records.torsiondrive.add(
                    initial_molecules=[entry.initial_molecule_ids],
                    td_spec=td_spec,
                    as_service=True,
                    compute_tag=compute_tag,
                    compute_priority=compute_priority,
                    owner_user=owner_user_id,
                    owner_group=owner_group_id,
                    find_existing=find_existing,
                    session=session,
                )

                if meta.success:
                    rec = TorsiondriveDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=td_ids[0],
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
            self.entry_orm.additional_optimization_keywords,
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
                self.entry_orm.additional_optimization_keywords,
                self.entry_orm.attributes,
            ],
            select_stmt,
        )

        session.execute(stmt)

        # Now do the molecules (stored in a separate table)
        select_stmt = select(
            literal(destination_dataset_id),
            TorsiondriveDatasetMoleculeORM.entry_name,
            TorsiondriveDatasetMoleculeORM.molecule_id,
        )

        select_stmt = select_stmt.where(TorsiondriveDatasetMoleculeORM.dataset_id == source_dataset_id)

        if entry_names is not None:
            select_stmt = select_stmt.where(TorsiondriveDatasetMoleculeORM.entry_name.in_(entry_names))

        stmt = insert(TorsiondriveDatasetMoleculeORM)
        stmt = stmt.from_select(
            [
                TorsiondriveDatasetMoleculeORM.dataset_id,
                TorsiondriveDatasetMoleculeORM.entry_name,
                TorsiondriveDatasetMoleculeORM.molecule_id,
            ],
            select_stmt,
        )

        session.execute(stmt)
