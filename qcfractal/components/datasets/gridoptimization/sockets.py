from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from qcfractal.components.datasets.sockets import BaseDatasetSocket
from qcfractal.components.records.gridoptimization.db_models import GridoptimizationRecordORM
from qcportal.datasets.gridoptimization import GridoptimizationDatasetNewEntry
from qcportal.records import PriorityEnum
from qcportal.records.gridoptimization import GridoptimizationSpecification
from .db_models import (
    GridoptimizationDatasetORM,
    GridoptimizationDatasetSpecificationORM,
    GridoptimizationDatasetEntryORM,
    GridoptimizationDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.metadata_models import InsertMetadata
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
        tag: str,
        priority: PriorityEnum,
    ):
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
                    tag=tag,
                    priority=priority,
                    session=session,
                )

                if meta.n_inserted == 1:
                    rec = GridoptimizationDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=gridopt_ids[0],
                    )
                    session.add(rec)
