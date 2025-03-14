from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, literal, insert

from qcfractal.components.dataset_socket import BaseDatasetSocket
from qcfractal.components.neb.record_db_models import NEBRecordORM
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.neb import NEBDatasetNewEntry, NEBSpecification
from qcportal.record_models import PriorityEnum
from .dataset_db_models import (
    NEBDatasetORM,
    NEBDatasetSpecificationORM,
    NEBDatasetEntryORM,
    NEBDatasetInitialMoleculeORM,
    NEBDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class NEBDatasetSocket(BaseDatasetSocket):
    # Used by the base class
    dataset_orm = NEBDatasetORM
    specification_orm = NEBDatasetSpecificationORM
    entry_orm = NEBDatasetEntryORM
    record_item_orm = NEBDatasetRecordItemORM
    record_orm = NEBRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: NEBSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.neb.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[NEBDatasetNewEntry]):
        all_entries = []
        for entry in new_entries:
            meta, mol_ids = self.root_socket.molecules.add_mixed(entry.initial_chain, session=session)

            new_ent_chain = [
                NEBDatasetInitialMoleculeORM(molecule_id=mid, position=pos) for pos, mid in enumerate(mol_ids)
            ]
            new_ent = NEBDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                additional_keywords=entry.additional_keywords,
                additional_singlepoint_keywords=entry.additional_singlepoint_keywords,
                attributes=entry.attributes,
                initial_chain_assoc=new_ent_chain,
            )
            all_entries.append(new_ent)

        return all_entries

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[NEBDatasetEntryORM],
        spec_orm: Iterable[NEBDatasetSpecificationORM],
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
            neb_spec_obj = spec.specification.to_model(NEBSpecification)
            neb_spec_input_dict = neb_spec_obj.dict()

            for entry in entry_orm:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_neb_spec = copy.deepcopy(neb_spec_input_dict)
                new_neb_spec["keywords"].update(entry.additional_keywords)
                new_neb_spec["singlepoint_specification"]["keywords"].update(entry.additional_singlepoint_keywords)
                initial_chain = [x.to_model(Molecule) for x in entry.initial_chain]
                meta, neb_ids = self.root_socket.records.neb.add(
                    initial_chains=[initial_chain],
                    neb_spec=NEBSpecification(**new_neb_spec),
                    compute_tag=compute_tag,
                    compute_priority=compute_priority,
                    owner_user=owner_user_id,
                    owner_group=owner_group_id,
                    find_existing=find_existing,
                    session=session,
                )

                if meta.success:
                    rec = NEBDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=neb_ids[0],
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
            self.entry_orm.additional_singlepoint_keywords,
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
                self.entry_orm.additional_singlepoint_keywords,
                self.entry_orm.attributes,
            ],
            select_stmt,
        )

        session.execute(stmt)

        # Now do the molecules (stored in a separate table)
        select_stmt = select(
            literal(destination_dataset_id),
            NEBDatasetInitialMoleculeORM.entry_name,
            NEBDatasetInitialMoleculeORM.molecule_id,
            NEBDatasetInitialMoleculeORM.position,
        )

        select_stmt = select_stmt.where(NEBDatasetInitialMoleculeORM.dataset_id == source_dataset_id)

        if entry_names is not None:
            select_stmt = select_stmt.where(NEBDatasetInitialMoleculeORM.entry_name.in_(entry_names))

        stmt = insert(NEBDatasetInitialMoleculeORM)
        stmt = stmt.from_select(
            [
                NEBDatasetInitialMoleculeORM.dataset_id,
                NEBDatasetInitialMoleculeORM.entry_name,
                NEBDatasetInitialMoleculeORM.molecule_id,
                NEBDatasetInitialMoleculeORM.position,
            ],
            select_stmt,
        )

        session.execute(stmt)
