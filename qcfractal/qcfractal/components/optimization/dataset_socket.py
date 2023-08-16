from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from qcfractal.components.dataset_socket import BaseDatasetSocket
from qcfractal.components.optimization.record_db_models import OptimizationRecordORM
from qcportal.optimization import OptimizationDatasetNewEntry, OptimizationSpecification
from qcportal.record_models import PriorityEnum
from .dataset_db_models import (
    OptimizationDatasetORM,
    OptimizationDatasetSpecificationORM,
    OptimizationDatasetEntryORM,
    OptimizationDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.metadata_models import InsertMetadata
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class OptimizationDatasetSocket(BaseDatasetSocket):

    # Used by the base class
    dataset_orm = OptimizationDatasetORM
    specification_orm = OptimizationDatasetSpecificationORM
    entry_orm = OptimizationDatasetEntryORM
    record_item_orm = OptimizationDatasetRecordItemORM
    record_orm = OptimizationRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: OptimizationSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.optimization.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[OptimizationDatasetNewEntry]):

        molecules = [x.initial_molecule for x in new_entries]
        meta, mol_ids = self.root_socket.molecules.add_mixed(molecules, session=session)

        all_entries = []
        for entry, molecule_id in zip(new_entries, mol_ids):
            new_ent = OptimizationDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                initial_molecule_id=molecule_id,
                additional_keywords=entry.additional_keywords,
                attributes=entry.attributes,
            )

            all_entries.append(new_ent)

        return all_entries

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[OptimizationDatasetEntryORM],
        spec_orm: Iterable[OptimizationDatasetSpecificationORM],
        existing_records: Iterable[Tuple[str, str]],
        tag: str,
        priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
    ):

        # Weed out any with additional keywords
        special_entries = [x for x in entry_orm if x.additional_keywords]
        normal_entries = [x for x in entry_orm if not x.additional_keywords]

        # Normal entries - just let it rip
        for spec in spec_orm:
            new_normal_entries = [x for x in normal_entries if (x.name, spec.name) not in existing_records]
            molecule_ids = [x.initial_molecule_id for x in new_normal_entries]

            meta, opt_ids = self.root_socket.records.optimization.add_internal(
                initial_molecule_ids=molecule_ids,
                opt_spec_id=spec.specification_id,
                tag=tag,
                priority=priority,
                owner_user_id=owner_user_id,
                owner_group_id=owner_group_id,
                find_existing=find_existing,
                session=session,
            )

            for entry, oid in zip(new_normal_entries, opt_ids):
                rec = OptimizationDatasetRecordItemORM(
                    dataset_id=dataset_id, entry_name=entry.name, specification_name=spec.name, record_id=oid
                )
                session.add(rec)

        # Now the ones with additional keywords
        for spec in spec_orm:
            spec_obj = spec.specification.to_model(OptimizationSpecification)
            spec_input_dict = spec_obj.dict()

            for entry in special_entries:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_spec = copy.deepcopy(spec_input_dict)
                new_spec["keywords"].update(entry.additional_keywords)

                meta, opt_ids = self.root_socket.records.optimization.add(
                    initial_molecules=[entry.initial_molecule_id],
                    opt_spec=OptimizationSpecification(**new_spec),
                    tag=tag,
                    priority=priority,
                    owner_user=owner_user_id,
                    owner_group=owner_group_id,
                    find_existing=find_existing,
                    session=session,
                )

                if meta.success:
                    rec = OptimizationDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=opt_ids[0],
                    )
                    session.add(rec)
