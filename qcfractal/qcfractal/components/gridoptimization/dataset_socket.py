from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, literal, insert

from qcfractal.components.dataset_socket import BaseDatasetSocket
from qcfractal.components.gridoptimization.record_db_models import GridoptimizationRecordORM
from qcportal.gridoptimization import GridoptimizationDatasetNewEntry, GridoptimizationSpecification
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.record_models import PriorityEnum
from .dataset_db_models import (
    GridoptimizationDatasetORM,
    GridoptimizationDatasetSpecificationORM,
    GridoptimizationDatasetEntryORM,
    GridoptimizationDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class GridoptimizationDatasetSocket(BaseDatasetSocket):
    # Used by the base class
    dataset_orm = GridoptimizationDatasetORM
    specification_orm = GridoptimizationDatasetSpecificationORM
    entry_orm = GridoptimizationDatasetEntryORM
    record_item_orm = GridoptimizationDatasetRecordItemORM
    record_orm = GridoptimizationRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: GridoptimizationSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.gridoptimization.add_specification(specification, session=session)

    def _create_entries(
        self, session: Session, dataset_id: int, new_entries: Sequence[GridoptimizationDatasetNewEntry]
    ):
        all_entries = []
        for entry in new_entries:
            meta, mol_ids = self.root_socket.molecules.add_mixed([entry.initial_molecule], session=session)

            new_ent = GridoptimizationDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                additional_keywords=entry.additional_keywords,
                additional_optimization_keywords=entry.additional_optimization_keywords,
                attributes=entry.attributes,
                initial_molecule_id=mol_ids[0],
            )

            all_entries.append(new_ent)

        return all_entries

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[GridoptimizationDatasetEntryORM],
        spec_orm: Iterable[GridoptimizationDatasetSpecificationORM],
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
            goopt_spec_obj = spec.specification.to_model(GridoptimizationSpecification)
            goopt_spec_input_dict = goopt_spec_obj.dict()

            for entry in entry_orm:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_go_spec = copy.deepcopy(goopt_spec_input_dict)
                new_go_spec["keywords"].update(entry.additional_keywords)
                new_go_spec["optimization_specification"]["keywords"].update(entry.additional_optimization_keywords)

                go_spec = GridoptimizationSpecification(
                    optimization_specification=new_go_spec["optimization_specification"],
                    keywords=new_go_spec["keywords"],
                )

                meta, gridopt_ids = self.root_socket.records.gridoptimization.add(
                    initial_molecules=[entry.initial_molecule_id],
                    go_spec=go_spec,
                    compute_tag=compute_tag,
                    compute_priority=compute_priority,
                    owner_user=owner_user_id,
                    owner_group=owner_group_id,
                    find_existing=find_existing,
                    session=session,
                )

                if meta.success:
                    rec = GridoptimizationDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=gridopt_ids[0],
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
            self.entry_orm.initial_molecule_id,
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
                self.entry_orm.initial_molecule_id,
                self.entry_orm.additional_keywords,
                self.entry_orm.additional_optimization_keywords,
                self.entry_orm.attributes,
            ],
            select_stmt,
        )

        session.execute(stmt)
