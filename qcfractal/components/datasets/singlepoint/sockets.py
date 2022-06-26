from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from qcfractal.components.datasets.sockets import BaseDatasetSocket
from qcfractal.components.records.singlepoint.db_models import SinglepointRecordORM
from qcportal.datasets.singlepoint import SinglepointDatasetNewEntry
from qcportal.records import PriorityEnum
from qcportal.records.singlepoint import QCSpecification
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
    record_orm = SinglepointRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: QCSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.singlepoint.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[SinglepointDatasetNewEntry]):

        molecules = [x.molecule for x in new_entries]
        meta, mol_ids = self.root_socket.molecules.add_mixed(molecules, session=session)

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

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[SinglepointDatasetEntryORM],
        spec_orm: Iterable[SinglepointDatasetSpecificationORM],
        existing_records: Iterable[Tuple[str, str]],
        tag: Optional[str],
        priority: Optional[PriorityEnum],
    ):

        # Weed out any with additional keywords
        special_entries = [x for x in entry_orm if x.additional_keywords]
        normal_entries = [x for x in entry_orm if not x.additional_keywords]

        # Normal entries - just let it rip
        for spec in spec_orm:
            new_normal_entries = [x for x in normal_entries if (x.name, spec.name) not in existing_records]
            molecule_ids = [x.molecule_id for x in new_normal_entries]

            meta, sp_ids = self.root_socket.records.singlepoint.add_internal(
                molecule_ids=molecule_ids,
                qc_spec_id=spec.specification_id,
                tag=tag,
                priority=priority,
                session=session,
            )

            for entry, oid in zip(new_normal_entries, sp_ids):
                rec = SinglepointDatasetRecordItemORM(
                    dataset_id=dataset_id, entry_name=entry.name, specification_name=spec.name, record_id=oid
                )
                session.add(rec)

        # Now the ones with additional keywords
        for spec in spec_orm:
            spec_obj = spec.specification.to_model(QCSpecification)
            spec_input_dict = spec_obj.dict()

            for entry in special_entries:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_spec = copy.deepcopy(spec_input_dict)
                new_spec["keywords"].update(entry.additional_keywords)

                meta, sp_ids = self.root_socket.records.singlepoint.add(
                    molecules=[entry.molecule_id],
                    qc_spec=QCSpecification(**new_spec),
                    tag=tag,
                    priority=priority,
                    session=session,
                )

                if meta.n_inserted == 1:
                    rec = SinglepointDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=sp_ids[0],
                    )
                    session.add(rec)
