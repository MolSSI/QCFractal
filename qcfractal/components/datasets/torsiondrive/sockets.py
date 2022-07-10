from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from qcfractal.components.datasets.sockets import BaseDatasetSocket
from qcfractal.components.records.torsiondrive.db_models import TorsiondriveRecordORM
from qcportal.datasets.torsiondrive import TorsiondriveDatasetNewEntry
from qcportal.records import PriorityEnum
from qcportal.records.torsiondrive import TorsiondriveSpecification
from .db_models import (
    TorsiondriveDatasetORM,
    TorsiondriveDatasetSpecificationORM,
    TorsiondriveDatasetEntryORM,
    TorsiondriveDatasetMoleculeORM,
    TorsiondriveDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.metadata_models import InsertMetadata
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
        tag: str,
        priority: PriorityEnum,
    ):
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
                    tag=tag,
                    priority=priority,
                    session=session,
                )

                if meta.n_inserted == 1:
                    rec = TorsiondriveDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=td_ids[0],
                    )
                    session.add(rec)
