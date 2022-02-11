from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from qcfractal.components.datasets.sockets import BaseDatasetSocket
from qcportal.datasets.optimization import OptimizationDatasetNewEntry
from qcportal.exceptions import MissingDataError
from qcportal.records import PriorityEnum
from qcportal.records.optimization import OptimizationSpecification, OptimizationInputSpecification
from .db_models import (
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

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_records_select():
        stmt = select(
            OptimizationDatasetRecordItemORM.dataset_id.label("dataset_id"),
            OptimizationDatasetRecordItemORM.entry_name.label("entry_name"),
            OptimizationDatasetRecordItemORM.specification_name.label("specification_name"),
            OptimizationDatasetRecordItemORM.record_id.label("record_id"),
        )
        return [stmt]

    def _add_specification(
        self, session: Session, specification: OptimizationInputSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.optimization.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[OptimizationDatasetNewEntry]):

        molecules = [x.initial_molecule for x in new_entries]
        meta, mol_ids = self.root_socket.molecules.add_mixed(molecules)

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
            stmt = select(OptimizationDatasetSpecificationORM)
            stmt = stmt.join(OptimizationDatasetSpecificationORM.specification)
            stmt = stmt.where(OptimizationDatasetSpecificationORM.dataset_id == dataset_id)

            if specification_names:
                stmt = stmt.where(OptimizationDatasetSpecificationORM.name.in_(specification_names))

            ds_specs = session.execute(stmt).scalars().all()

            if specification_names is not None:
                found_specs = {x.name for x in ds_specs}
                missing_specs = set(specification_names) - found_specs
                if missing_specs:
                    raise MissingDataError(f"Could not find all specifications. Missing: {missing_specs}")

            # Get entries
            stmt = select(OptimizationDatasetEntryORM)
            stmt = stmt.where(OptimizationDatasetEntryORM.dataset_id == dataset_id)

            if entry_names:
                stmt = stmt.where(OptimizationDatasetEntryORM.name.in_(entry_names))

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
            molecule_ids = [x.initial_molecule_id for x in normal_entries]
            for ds_spec in ds_specs:
                meta, opt_ids = self.root_socket.records.optimization.add_internal(
                    initial_molecule_ids=molecule_ids,
                    opt_spec_id=ds_spec.specification_id,
                    tag=tag,
                    priority=priority,
                    session=session,
                )

                for idx, (entry, oid) in enumerate(zip(normal_entries, opt_ids)):
                    if idx in meta.inserted_idx:
                        rec = OptimizationDatasetRecordItemORM(
                            dataset_id=dataset_id, entry_name=entry.name, specification_name=ds_spec.name, record_id=oid
                        )
                        session.add(rec)

            # Now the ones with additional keywords
            for ds_spec in ds_specs:
                spec_obj = ds_spec.specification._to_model(OptimizationSpecification)
                spec_input_dict = spec_obj.as_input().dict()

                for entry in special_entries:
                    new_spec = copy.deepcopy(spec_input_dict)
                    new_spec["keywords"].update(entry.additional_keywords)

                    meta, opt_ids = self.root_socket.records.optimization.add(
                        initial_molecules=[entry.initial_molecule_id],
                        opt_spec=OptimizationInputSpecification(**new_spec),
                        tag=tag,
                        priority=priority,
                        session=session,
                    )

                    if meta.n_inserted == 1:
                        rec = OptimizationDatasetRecordItemORM(
                            dataset_id=dataset_id,
                            entry_name=entry.name,
                            specification_name=ds_spec.name,
                            record_id=opt_ids[0],
                        )
                        session.add(rec)
