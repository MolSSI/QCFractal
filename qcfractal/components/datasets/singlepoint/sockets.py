from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from qcfractal.components.datasets.sockets import BaseDatasetSocket
from qcportal.datasets.singlepoint import SinglepointDatasetNewEntry
from qcportal.exceptions import MissingDataError
from qcportal.records import PriorityEnum
from qcportal.records.singlepoint import QCSpecification, QCInputSpecification
from .db_models import (
    SinglepointDatasetORM,
    SinglepointDatasetSpecificationORM,
    SinglepointDatasetEntryORM,
    SinglepointDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.metadata_models import InsertMetadata
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class SinglepointDatasetSocket(BaseDatasetSocket):

    # Used by the base class
    dataset_orm = SinglepointDatasetORM
    specification_orm = SinglepointDatasetSpecificationORM
    entry_orm = SinglepointDatasetEntryORM
    record_item_orm = SinglepointDatasetRecordItemORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_records_select():
        stmt = select(
            SinglepointDatasetRecordItemORM.dataset_id.label("dataset_id"),
            SinglepointDatasetRecordItemORM.entry_name.label("entry_name"),
            SinglepointDatasetRecordItemORM.specification_name.label("specification_name"),
            SinglepointDatasetRecordItemORM.record_id.label("record_id"),
        )
        return [stmt]

    def _add_specification(
        self, session: Session, specification: QCInputSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.singlepoint.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[SinglepointDatasetNewEntry]):

        molecules = [x.molecule for x in new_entries]
        meta, mol_ids = self.root_socket.molecules.add_mixed(molecules)

        all_entries = []
        for entry, molecule_id in zip(new_entries, mol_ids):
            new_ent = SinglepointDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                molecule_id=molecule_id,
                additional_keywords=entry.additional_keywords,
                attributes=entry.attributes,
            )

            all_entries.append(new_ent)

        return all_entries

    def submit(
        self,
        dataset_id: int,
        entry_names: Optional[Iterable[str]],
        specification_names: Optional[Iterable[str]],
        tag: Optional[str],
        priority: Optional[PriorityEnum],
        *,
        session: Optional[Session] = None,
    ):

        with self.root_socket.optional_session(session) as session:
            tag, priority = self.get_tag_priority(dataset_id, tag, priority, session=session)

            # Get specification details
            stmt = select(SinglepointDatasetSpecificationORM)
            stmt = stmt.join(SinglepointDatasetSpecificationORM.specification)
            stmt = stmt.where(SinglepointDatasetSpecificationORM.dataset_id == dataset_id)

            if specification_names:
                stmt = stmt.where(SinglepointDatasetSpecificationORM.name.in_(specification_names))

            ds_specs = session.execute(stmt).scalars().all()

            if specification_names is not None:
                found_specs = {x.name for x in ds_specs}
                missing_specs = set(specification_names) - found_specs
                if missing_specs:
                    raise MissingDataError(f"Could not find all specifications. Missing: {missing_specs}")

            # Get entries
            stmt = select(SinglepointDatasetEntryORM)
            stmt = stmt.where(SinglepointDatasetEntryORM.dataset_id == dataset_id)

            if entry_names:
                stmt = stmt.where(SinglepointDatasetEntryORM.name.in_(entry_names))

            entries = session.execute(stmt).scalars().all()

            if entry_names is not None:
                found_entries = {x.name for x in entries}
                missing_entries = set(entry_names) - found_entries
                if missing_entries:
                    raise MissingDataError(f"Could not find all entries. Missing: {missing_specs}")

            # Weed out any with additional keywords
            special_entries = [x for x in entries if x.additional_keywords]
            normal_entries = [x for x in entries if not x.additional_keywords]

            # Normal entries - just let it rip
            molecule_ids = [x.molecule_id for x in normal_entries]
            for ds_spec in ds_specs:
                meta, sp_ids = self.root_socket.records.singlepoint.add_internal(
                    molecule_ids=molecule_ids,
                    qc_spec_id=ds_spec.specification_id,
                    tag=tag,
                    priority=priority,
                    session=session,
                )

                for idx, (entry, oid) in enumerate(zip(normal_entries, sp_ids)):
                    if idx in meta.inserted_idx:
                        rec = SinglepointDatasetRecordItemORM(
                            dataset_id=dataset_id, entry_name=entry.name, specification_name=ds_spec.name, record_id=oid
                        )
                        session.add(rec)

            # Now the ones with additional keywords
            for ds_spec in ds_specs:
                spec_obj = ds_spec.specification._to_model(QCSpecification)
                spec_input_dict = spec_obj.as_input().dict()

                for entry in special_entries:
                    new_spec = copy.deepcopy(spec_input_dict)
                    new_spec["keywords"].update(entry.additional_keywords)

                    meta, sp_ids = self.root_socket.records.singlepoint.add(
                        molecule=[entry.molecule_id],
                        qc_spec=QCInputSpecification(**new_spec),
                        tag=tag,
                        priority=priority,
                        session=session,
                    )

                    if meta.n_inserted == 1:
                        rec = SinglepointDatasetRecordItemORM(
                            dataset_id=dataset_id,
                            entry_name=entry.name,
                            specification_name=ds_spec.name,
                            record_id=sp_ids[0],
                        )
                        session.add(rec)
