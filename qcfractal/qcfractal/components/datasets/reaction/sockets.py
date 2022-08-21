from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from qcfractal.components.datasets.sockets import BaseDatasetSocket
from qcfractal.components.records.reaction.db_models import ReactionRecordORM
from qcportal.datasets.reaction import ReactionDatasetNewEntry
from qcportal.records import PriorityEnum
from qcportal.records.reaction import ReactionSpecification
from .db_models import (
    ReactionDatasetORM,
    ReactionDatasetSpecificationORM,
    ReactionDatasetEntryORM,
    ReactionDatasetStoichiometryORM,
    ReactionDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.metadata_models import InsertMetadata
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Sequence, Iterable, Tuple


class ReactionDatasetSocket(BaseDatasetSocket):

    # Used by the base class
    dataset_orm = ReactionDatasetORM
    specification_orm = ReactionDatasetSpecificationORM
    entry_orm = ReactionDatasetEntryORM
    record_item_orm = ReactionDatasetRecordItemORM
    record_orm = ReactionRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseDatasetSocket.__init__(
            self,
            root_socket,
        )

        self._logger = logging.getLogger(__name__)

    def _add_specification(
        self, session: Session, specification: ReactionSpecification
    ) -> Tuple[InsertMetadata, Optional[int]]:
        return self.root_socket.records.reaction.add_specification(specification, session=session)

    def _create_entries(self, session: Session, dataset_id: int, new_entries: Sequence[ReactionDatasetNewEntry]):

        all_entries = []
        for entry in new_entries:
            # stoichiometries = list of tuples
            molecules = [x[1] for x in entry.stoichiometries]

            meta, mol_ids = self.root_socket.molecules.add_mixed(molecules, session=session)

            new_stoich_orm = []

            for coeff, mid in zip(entry.stoichiometries, mol_ids):
                new_stoich_orm.append(
                    ReactionDatasetStoichiometryORM(
                        coefficient=coeff[0],
                        molecule_id=mid,
                    )
                )

            new_ent = ReactionDatasetEntryORM(
                dataset_id=dataset_id,
                name=entry.name,
                comment=entry.comment,
                stoichiometries=new_stoich_orm,
                additional_keywords=entry.additional_keywords,
                attributes=entry.attributes,
            )

            all_entries.append(new_ent)

        return all_entries

    def _submit(
        self,
        session: Session,
        dataset_id: int,
        entry_orm: Iterable[ReactionDatasetEntryORM],
        spec_orm: Iterable[ReactionDatasetSpecificationORM],
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
            stoichiometries = [[(x.coefficient, x.molecule_id) for x in y.stoichiometries] for y in new_normal_entries]

            meta, rxn_ids = self.root_socket.records.reaction.add_internal(
                stoichiometries=stoichiometries,
                rxn_spec_id=spec.specification_id,
                tag=tag,
                priority=priority,
                session=session,
            )

            for (entry, oid) in zip(new_normal_entries, rxn_ids):
                rec = ReactionDatasetRecordItemORM(
                    dataset_id=dataset_id, entry_name=entry.name, specification_name=spec.name, record_id=oid
                )
                session.add(rec)

        # Now the ones with additional keywords
        for spec in spec_orm:

            spec_obj = spec.specification.to_model(ReactionSpecification)
            spec_input_dict = spec_obj.dict()

            for entry in special_entries:
                if (entry.name, spec.name) in existing_records:
                    continue

                new_spec = copy.deepcopy(spec_input_dict)
                new_spec["keywords"].update(entry.additional_keywords)
                print("NEW SPEC", new_spec)

                stoichiometry = [(x.coefficient, x.molecule_id) for x in entry.stoichiometries]

                meta, rxn_ids = self.root_socket.records.reaction.add(
                    stoichiometries=[stoichiometry],
                    rxn_spec=ReactionSpecification(**new_spec),
                    tag=tag,
                    priority=priority,
                    session=session,
                )

                if meta.n_inserted == 1:
                    rec = ReactionDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=rxn_ids[0],
                    )
                    session.add(rec)
