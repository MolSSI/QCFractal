from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, literal, text, insert

from qcfractal.components.dataset_socket import BaseDatasetSocket
from qcfractal.components.singlepoint.record_db_models import SinglepointRecordORM
from qcportal.exceptions import InvalidArgumentsError, MissingDataError
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint import SinglepointDatasetNewEntry, QCSpecification
from .dataset_db_models import (
    SinglepointDatasetORM,
    SinglepointDatasetSpecificationORM,
    SinglepointDatasetEntryORM,
    SinglepointDatasetRecordItemORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
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
                local_results=entry.local_results,
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
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user_id: Optional[int],
        owner_group_id: Optional[int],
        find_existing: bool,
    ) -> InsertCountsMetadata:

        n_inserted = 0
        n_existing = 0

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
                compute_tag=compute_tag,
                compute_priority=compute_priority,
                owner_user_id=owner_user_id,
                owner_group_id=owner_group_id,
                find_existing=find_existing,
                session=session,
            )

            for entry, oid in zip(new_normal_entries, sp_ids):
                rec = SinglepointDatasetRecordItemORM(
                    dataset_id=dataset_id, entry_name=entry.name, specification_name=spec.name, record_id=oid
                )
                session.add(rec)

            n_inserted += meta.n_inserted
            n_existing += meta.n_existing

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
                    compute_tag=compute_tag,
                    compute_priority=compute_priority,
                    owner_user=owner_user_id,
                    owner_group=owner_group_id,
                    find_existing=find_existing,
                    session=session,
                )

                if meta.success:
                    rec = SinglepointDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=sp_ids[0],
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
            self.entry_orm.molecule_id,
            self.entry_orm.additional_keywords,
            self.entry_orm.attributes,
            self.entry_orm.local_results,
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
                self.entry_orm.molecule_id,
                self.entry_orm.additional_keywords,
                self.entry_orm.attributes,
                self.entry_orm.local_results,
            ],
            select_stmt,
        )

        session.execute(stmt)

    def add_entries_from_ds(
        self,
        dataset_id: int,
        from_dataset_id: Optional[int],
        from_dataset_type: Optional[str],
        from_dataset_name: Optional[str],
        from_specification_name: Optional[str],
        *,
        session: Optional[Session] = None,
    ) -> InsertCountsMetadata:
        """
        Adds entries from another dataset into this Singlepoint dataset

        Either the from_dataset_id or both the from_dataset_type and from_dataset_name
        must be provided.

        If copying from an optimization dataset, then
        the from_specification_name must be provided.

        Parameters
        ----------
        dataset_id
            ID of the dataset to add entries to
        from_dataset_id
            ID of the dataset to add entries from
        from_dataset_type
            Type of the dataset to add entries from
        from_dataset_name
            Name of the dataset to add entries from
        from_specification_name
            Specification of the source dataset to use for molecules

        Returns
        -------
        :
            Metadata about the added entries
        """

        if dataset_id is None and (from_dataset_type is None or from_dataset_name is None):
            raise InvalidArgumentsError("dataset_id or both from_dataset_id and from_dataset_name must be provided")

        with self.root_socket.optional_session(session) as session:
            # Make sure dataset exists
            stmt = "SELECT (id) FROM base_dataset WHERE id = :dataset_id"
            r = session.execute(text(stmt), {"dataset_id": dataset_id}).scalar_one_or_none()
            if r is None:
                raise MissingDataError(f"Cannot find dataset with id={dataset_id}")

            # No matter what was passed in, we need the dataset type and id to copy from
            if from_dataset_id is not None:
                stmt = "SELECT (dataset_type) FROM base_dataset WHERE id = :from_dataset_id"
                ds_type = session.execute(text(stmt), {"from_dataset_id": from_dataset_id}).scalar_one_or_none()

                if ds_type is None:
                    raise MissingDataError(f"Cannot find dataset with id={from_dataset_id}")

                if from_dataset_type is not None and from_dataset_type.lower() != ds_type.lower():
                    raise InvalidArgumentsError(
                        f"Dataset id and type specified, but the type of the dataset id={from_dataset_id} is {ds_type}, not {from_dataset_type}"
                    )
                from_dataset_type = ds_type
            else:
                assert from_dataset_type is not None and from_dataset_name is not None
                stmt = "SELECT (id) FROM base_dataset WHERE dataset_type = :from_dataset_type and lname = :from_dataset_name"
                from_dataset_id = session.execute(
                    text(stmt), {"from_dataset_type": from_dataset_type, "from_dataset_name": from_dataset_name.lower()}
                ).scalar_one_or_none()

                if from_dataset_id is None:
                    raise MissingDataError(
                        f"Cannot find dataset with type={from_dataset_type} and name={from_dataset_name}"
                    )

            if from_dataset_type == "singlepoint":
                stmt = """
                    INSERT INTO singlepoint_dataset_entry (dataset_id, name, comment, molecule_id, attributes, additional_keywords)
                    SELECT :dataset_id, sde.name, sde.comment, sde.molecule_id, sde.attributes, sde.additional_keywords
                    FROM singlepoint_dataset_entry sde
                    WHERE sde.dataset_id = :from_dataset_id
                    ON CONFLICT (dataset_id, name) DO NOTHING
                    RETURNING 1
                """

                r = session.execute(
                    text(stmt),
                    {
                        "dataset_id": dataset_id,
                        "from_dataset_id": from_dataset_id,
                    },
                )

                meta = InsertCountsMetadata(n_inserted=r.rowcount, n_existing=0)

            elif from_dataset_type == "optimization":

                if from_specification_name is None:
                    raise InvalidArgumentsError(
                        "from_specification_name must be provided when adding entries from an optimization dataset"
                    )

                stmt = """
                    INSERT INTO singlepoint_dataset_entry (dataset_id, name, comment, molecule_id, attributes, additional_keywords)
                    SELECT :dataset_id, ode.name, ode.comment, opr.final_molecule_id, ode.attributes, '{}'::jsonb
                    FROM optimization_dataset_entry ode
                    INNER JOIN optimization_dataset_record odr ON ode.dataset_id = odr.dataset_id AND ode.name = odr.entry_name
                    INNER JOIN optimization_record opr ON odr.record_id = opr.id
                    INNER JOIN base_record br ON opr.id = br.id
                    WHERE ode.dataset_id = :optimization_dataset_id
                    AND odr.specification_name = :specification_name
                    AND br.status = 'complete'
                    AND opr.final_molecule_id IS NOT NULL
                    ON CONFLICT (dataset_id, name) DO NOTHING
                    RETURNING 1
                """

                r = session.execute(
                    text(stmt),
                    {
                        "dataset_id": dataset_id,
                        "optimization_dataset_id": from_dataset_id,
                        "specification_name": from_specification_name,
                    },
                )

                meta = InsertCountsMetadata(n_inserted=r.rowcount, n_existing=0)
            else:
                raise InvalidArgumentsError(
                    f"Unable to handle adding singlepoint dataset entries from dataset of type {from_dataset_type}"
                )

        return meta
