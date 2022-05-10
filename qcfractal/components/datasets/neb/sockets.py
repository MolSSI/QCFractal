from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from qcfractal.components.datasets.sockets import BaseDatasetSocket
from qcportal.datasets.neb import NEBDatasetNewEntry
from qcportal.records import PriorityEnum
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.neb import NEBSpecification
from .db_models import (
    NEBDatasetORM,
    NEBDatasetSpecificationORM,
    NEBDatasetEntryORM,
    NEBDatasetMoleculeORM,
    NEBDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.metadata_models import InsertMetadata
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class NEBDatasetSocket(BaseDatasetSocket):

    # Used by the base class
    dataset_orm = NEBDatasetORM
    specification_orm = NEBDatasetSpecificationORM
    entry_orm = NEBDatasetEntryORM
    record_item_orm = NEBDatasetRecordItemORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: QCSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.spimization.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[NEBDatasetNewEntry]):

        all_entries = []
        for entry in new_entries:
            meta, mol_ids = self.root_socket.molecules.add_mixed(entry.initial_molecules, session=session)

            new_ent_mols = [NEBDatasetMoleculeORM(molecule_id=mid) for mid in mol_ids]

            new_ent = NEBDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                neb_keywords=entry.neb_keywords.dict(),
                additional_keywords=entry.additional_keywords,
                attributes=entry.attributes,
                molecules=new_ent_mols,
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
        tag: str,
        priority: PriorityEnum,
    ):
        for spec in spec_orm:
            # The spec for a neb dataset is an spimization specification
            sp_spec_obj = spec.specification.to_model(QCSpecification)
            sp_spec_input_dict = sp_spec_obj.dict()

            for entry in entry_orm:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_sp_spec = copy.deepcopy(sp_spec_input_dict)
                new_sp_spec["keywords"].update(entry.additional_keywords)

                neb_spec = NEBSpecification(
                    qc_specification=new_sp_spec, keywords=entry.neb_keywords
                )

                meta, neb_ids = self.root_socket.records.neb.add(
                    initial_molecules=[entry.initial_molecule_ids],
                    neb_spec=neb_spec,
                    as_service=True,
                    tag=tag,
                    priority=priority,
                    session=session,
                )

                if meta.n_inserted == 1:
                    rec = NEBDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=neb_ids[0],
                    )
                    session.add(rec)
