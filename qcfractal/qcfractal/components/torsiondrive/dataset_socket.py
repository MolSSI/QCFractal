from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select, literal, insert, text

from qcfractal.components.torsiondrive.record_db_models import TorsiondriveRecordORM
from qcportal.exceptions import InvalidArgumentsError, MissingDataError
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.record_models import PriorityEnum
from qcportal.torsiondrive import TorsiondriveDatasetNewEntry, TorsiondriveSpecification
from .dataset_db_models import (
    TorsiondriveDatasetORM,
    TorsiondriveDatasetSpecificationORM,
    TorsiondriveDatasetEntryORM,
    TorsiondriveDatasetMoleculeORM,
    TorsiondriveDatasetRecordItemORM,
)
from ..base_dataset_socket import BaseDatasetSocket

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
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
        compute_tag: str,
        compute_priority: PriorityEnum,
        creator_user_id: Optional[int],
        find_existing: bool,
    ) -> InsertCountsMetadata:

        n_inserted = 0
        n_existing = 0

        for spec in spec_orm:
            td_spec_obj = spec.specification.to_model(TorsiondriveSpecification)
            td_spec_input_dict = td_spec_obj.model_dump()

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
                    compute_tag=compute_tag,
                    compute_priority=compute_priority,
                    creator_user=creator_user_id,
                    find_existing=find_existing,
                    session=session,
                )

                if meta.success:
                    rec = TorsiondriveDatasetRecordItemORM(
                        dataset_id=dataset_id,
                        entry_name=entry.name,
                        specification_name=spec.name,
                        record_id=td_ids[0],
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
            self.entry_orm.additional_optimization_keywords,
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
                self.entry_orm.additional_optimization_keywords,
                self.entry_orm.attributes,
            ],
            select_stmt,
        )

        session.execute(stmt)

        # Now do the molecules (stored in a separate table)
        select_stmt = select(
            literal(destination_dataset_id),
            TorsiondriveDatasetMoleculeORM.entry_name,
            TorsiondriveDatasetMoleculeORM.molecule_id,
        )

        select_stmt = select_stmt.where(TorsiondriveDatasetMoleculeORM.dataset_id == source_dataset_id)

        if entry_names is not None:
            select_stmt = select_stmt.where(TorsiondriveDatasetMoleculeORM.entry_name.in_(entry_names))

        stmt = insert(TorsiondriveDatasetMoleculeORM)
        stmt = stmt.from_select(
            [
                TorsiondriveDatasetMoleculeORM.dataset_id,
                TorsiondriveDatasetMoleculeORM.entry_name,
                TorsiondriveDatasetMoleculeORM.molecule_id,
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
        Adds entries from another dataset into this torsiondrive dataset

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

        if from_dataset_id is None and (from_dataset_type is None or from_dataset_name is None):
            raise InvalidArgumentsError("from_dataset_id or both from_dataset_type and from_dataset_name must be provided")

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

            if from_dataset_type == "torsiondrive":
                stmt = text("""
                     WITH inserted_entries AS (
                         INSERT INTO torsiondrive_dataset_entry (
                                                                 dataset_id,
                                                                 name,
                                                                 comment,
                                                                 additional_keywords,
                                                                 additional_optimization_keywords,
                                                                 attributes
                             )
                             SELECT :dataset_id,
                                 name,
                                 comment,
                                 additional_keywords,
                                 additional_optimization_keywords,
                                 attributes
                             FROM torsiondrive_dataset_entry
                             WHERE dataset_id = :from_dataset_id
                             ON CONFLICT (dataset_id, name) DO NOTHING
                             RETURNING name
                     ),
                     inserted_molecules AS (
                             INSERT INTO torsiondrive_dataset_molecule (
                                                                        dataset_id,
                                                                        entry_name,
                                                                        molecule_id
                                 )
                                 SELECT
                                     :dataset_id,
                                     m.entry_name,
                                     m.molecule_id
                                 FROM torsiondrive_dataset_molecule m
                                          JOIN inserted_entries ie
                                               ON m.entry_name = ie.name
                                 WHERE m.dataset_id = :from_dataset_id
                                 ON CONFLICT DO NOTHING
                          )
                     SELECT count(*)
                     FROM inserted_entries
                """)

                n_inserted = session.execute(
                    stmt,
                    {
                        "dataset_id": dataset_id,
                        "from_dataset_id": from_dataset_id,
                    },
                ).scalar_one()

                meta = InsertCountsMetadata(
                    n_inserted=n_inserted,
                    n_existing=0,
                )


            elif from_dataset_type == "optimization":

                if from_specification_name is None:
                    raise InvalidArgumentsError(
                        "from_specification_name must be provided when adding entries from an optimization dataset"
                    )

                stmt = text("""
                    WITH source_rows AS (
                        SELECT
                            ode.name,
                            ode.comment,
                            ode.attributes,
                            opr.final_molecule_id
                        FROM optimization_dataset_entry ode
                                 INNER JOIN optimization_dataset_record odr
                                            ON ode.dataset_id = odr.dataset_id
                                                AND ode.name = odr.entry_name
                                 INNER JOIN optimization_record opr
                                            ON odr.record_id = opr.id
                                 INNER JOIN base_record br
                                            ON opr.id = br.id
                        WHERE ode.dataset_id = :optimization_dataset_id
                          AND odr.specification_name = :specification_name
                          AND br.status = 'complete'
                          AND opr.final_molecule_id IS NOT NULL
                    ),
                         inserted_entries AS (
                             INSERT INTO torsiondrive_dataset_entry (
                                                                     dataset_id,
                                                                     name,
                                                                     comment,
                                                                     additional_keywords,
                                                                     additional_optimization_keywords,
                                                                     attributes
                                 )
                                 SELECT
                                     :dataset_id,
                                     name,
                                     comment,
                                     '{}'::jsonb,
                                     '{}'::jsonb,
                                     attributes
                                 FROM source_rows
                                 ON CONFLICT (dataset_id, name) DO NOTHING
                                 RETURNING name
                         ),
                         inserted_molecules AS (
                             INSERT INTO torsiondrive_dataset_molecule (
                                                                        dataset_id,
                                                                        entry_name,
                                                                        molecule_id
                                 )
                                 SELECT
                                     :dataset_id,
                                     s.name,
                                     s.final_molecule_id
                                 FROM source_rows s
                                          INNER JOIN inserted_entries ie
                                                     ON s.name = ie.name
                                 ON CONFLICT DO NOTHING
                         )
                    SELECT count(*)
                    FROM inserted_entries;
                """)

                n_inserted = session.execute(
                    stmt,
                    {
                        "dataset_id": dataset_id,
                        "optimization_dataset_id": from_dataset_id,
                        "specification_name": from_specification_name,
                    },
                ).scalar_one()


                meta = InsertCountsMetadata(n_inserted=n_inserted, n_existing=0)
            else:
                raise InvalidArgumentsError(
                    f"Unable to handle adding torsiondrive dataset entries from dataset of type {from_dataset_type}"
                )

        return meta
