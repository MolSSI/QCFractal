"""migrate reaction dataset

Revision ID: 0b31ab4244de
Revises: 8a41b7dc30af
Create Date: 2022-03-15 12:51:52.995657

"""

import json
import os
import sys
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import table, column

from qcfractal import __version__ as qcfractal_version

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
from migration_helpers.v0_50_helpers import get_empty_keywords_id, add_qc_spec

# revision identifiers, used by Alembic.
revision = "0b31ab4244de"
down_revision = "8a41b7dc30af"
branch_labels = None
depends_on = None


def create_reaction_record(conn, ds_id, created_on, modified_on, spec_id, stoichiometries, records):
    # We do this allllll by hand. We don't want to depend on the real ORM code
    # First, add a base record
    r = conn.execute(
        sa.text(
            """INSERT INTO base_record (record_type, is_service, status, created_on, modified_on)
               VALUES ('reaction', true, :status, :created_on, :modified_on)
               RETURNING id"""
        ),
        parameters=dict(status="waiting", created_on=created_on, modified_on=modified_on),
    )

    reaction_id = r.scalar()

    # Add a helpful comment
    conn.execute(
        sa.text(
            """INSERT INTO record_comment (record_id, timestamp, comment)
               VALUES (:record_id, :timestamp, :comment)"""
        ),
        parameters=dict(
            record_id=reaction_id,
            timestamp=datetime.now(timezone.utc),
            comment=f"Reaction is imported/migrated from dataset {ds_id}",
        ),
    )

    # Insert the compute history (always, for a service)
    # Fake a provenance
    provenance = {
        "creator": "qcfractal",
        "version": qcfractal_version,
        "routine": "qcfractal.services.reaction",
    }
    conn.execute(
        sa.text(
            """INSERT INTO record_compute_history (record_id, status, modified_on, provenance)
               VALUES (:record_id, :status, :modified_on, :provenance) RETURNING id"""
        ),
        parameters=dict(
            record_id=reaction_id, status="waiting", modified_on=modified_on, provenance=json.dumps(provenance)
        ),
    )

    # Now add to the reaction record table
    conn.execute(
        sa.text("INSERT INTO reaction_record (id, specification_id) VALUES (:reaction_id, :spec_id)"),
        parameters=dict(reaction_id=reaction_id, spec_id=spec_id),
    )

    stoich_map = {int(mid): coef for mid, coef in stoichiometries}

    # Now any found component records
    for sp_id, mid, _, _, _ in records:
        conn.execute(
            sa.text(
                """INSERT INTO reaction_component (reaction_id, molecule_id, coefficient, singlepoint_id)
                   VALUES (:reaction_id, :molecule_id, :coef, :sp_id)"""
            ),
            parameters=dict(reaction_id=reaction_id, molecule_id=mid, coef=stoich_map[mid], sp_id=sp_id),
        )

    # Add the service
    sp_ids = [x[0] for x in records]

    # guess a tag
    r = conn.execute(
        sa.text("SELECT tag FROM task_queue WHERE record_id IN :record_id LIMIT 1"),
        parameters=dict(record_id=tuple(sp_ids)),
    )
    tag = r.scalar_one_or_none()
    if tag is None:
        tag = "_rxn_migrated"

    # Add to the service queue
    r = conn.execute(
        sa.text(
            """INSERT INTO service_queue (record_id, tag, priority, created_on)
           VALUES (:reaction_id, :tag, :priority, :created_on)
           RETURNING id
        """
        ),
        parameters=dict(reaction_id=reaction_id, created_on=created_on, tag=tag, priority=1),
    )

    return reaction_id


def create_manybody_record(conn, ds_id, created_on, modified_on, spec_id, mol_id, record):
    # We do this allllll by hand. We don't want to depend on the real ORM code
    # First, add a base record
    r = conn.execute(
        sa.text(
            """INSERT INTO base_record (record_type, is_service, status, created_on, modified_on)
               VALUES ('manybody', true, :status, :created_on, :modified_on)
               RETURNING id"""
        ),
        parameters=dict(status="waiting", created_on=created_on, modified_on=modified_on),
    )

    manybody_id = r.scalar()

    # Add a helpful comment
    conn.execute(
        sa.text(
            """INSERT INTO record_comment (record_id, timestamp, comment)
               VALUES (:record_id, :timestamp, :comment)"""
        ),
        parameters=dict(
            record_id=manybody_id,
            timestamp=datetime.now(timezone.utc),
            comment=f"Manybody record is imported/migrated from reaction dataset {ds_id}",
        ),
    )

    # Insert the compute history (always, for a service)
    # Fake a provenance
    provenance = {
        "creator": "qcfractal",
        "version": qcfractal_version,
        "routine": "qcfractal.services.manybody",
    }
    conn.execute(
        sa.text(
            """INSERT INTO record_compute_history (record_id, status, modified_on, provenance)
               VALUES (:record_id, :status, :modified_on, :provenance) RETURNING id"""
        ),
        parameters=dict(
            record_id=manybody_id, status="waiting", modified_on=modified_on, provenance=json.dumps(provenance)
        ),
    )

    # Now add to the manybody record table
    conn.execute(
        sa.text(
            "INSERT INTO manybody_record (id, initial_molecule_id, specification_id) VALUES (:manybody_id, :mol_id, :spec_id)"
        ),
        parameters=dict(manybody_id=manybody_id, mol_id=mol_id, spec_id=spec_id),
    )

    # guess a tag
    r = conn.execute(
        sa.text("SELECT tag FROM task_queue WHERE record_id = :record_id LIMIT 1"), parameters={"record_id": record[0]}
    )

    tag = r.scalar_one_or_none()
    if tag is None:
        tag = "_mb_migrated"

    # Add to the service queue
    r = conn.execute(
        sa.text(
            """INSERT INTO service_queue (record_id, tag, priority, created_on)
           VALUES (:manybody_id, :tag, :priority, :created_on)
           RETURNING id
        """
        ),
        parameters=dict(manybody_id=manybody_id, created_on=created_on, tag=tag, priority=1),
    )

    return manybody_id


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    #####################################
    # New specification table
    #####################################
    op.create_table(
        "reaction_dataset_specification",
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("specification_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["dataset_id"], ["reaction_dataset.id"], ondelete="cascade"),
        sa.ForeignKeyConstraint(
            ["specification_id"],
            ["reaction_specification.id"],
        ),
        sa.PrimaryKeyConstraint("dataset_id", "name"),
    )
    op.create_index(
        "ix_reaction_dataset_specification_dataset_id", "reaction_dataset_specification", ["dataset_id"], unique=False
    )
    op.create_index("ix_reaction_dataset_specification_name", "reaction_dataset_specification", ["name"], unique=False)
    op.create_index(
        "ix_reaction_dataset_specification_specification_id",
        "reaction_dataset_specification",
        ["specification_id"],
        unique=False,
    )

    #####################################
    # Modify existing entry table
    #####################################
    op.alter_column("reaction_dataset_entry", "reaction_dataset_id", new_column_name="dataset_id")
    op.add_column("reaction_dataset_entry", sa.Column("comment", sa.String(), nullable=True))
    op.add_column(
        "reaction_dataset_entry",
        sa.Column("additional_keywords", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.alter_column(
        "reaction_dataset_entry",
        "attributes",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
    )
    op.create_index("ix_reaction_dataset_entry_dataset_id", "reaction_dataset_entry", ["dataset_id"], unique=False)
    op.create_index("ix_reaction_dataset_entry_name", "reaction_dataset_entry", ["name"], unique=False)
    op.drop_constraint("reaction_dataset_entry_reaction_dataset_id_fkey", "reaction_dataset_entry", type_="foreignkey")
    op.create_foreign_key(
        None, "reaction_dataset_entry", "reaction_dataset", ["dataset_id"], ["id"], ondelete="cascade"
    )
    op.drop_constraint("reaction_dataset_entry_pkey", "reaction_dataset_entry", type_="primary")
    op.create_primary_key("reaction_dataset_entry_pkey", "reaction_dataset_entry", ["dataset_id", "name"])

    # Attributes should be not nullable
    # Also, merge extras with attributes
    op.execute(sa.text("UPDATE reaction_dataset_entry SET attributes='{}'::json WHERE attributes IS NULL"))
    op.execute(
        sa.text("UPDATE reaction_dataset_entry SET additional_keywords='{}'::json WHERE additional_keywords IS NULL")
    )
    op.execute(sa.text("UPDATE reaction_dataset_entry SET attributes = (extras::jsonb || attributes::jsonb)"))

    #####################################
    # New stoichiometry table
    #####################################
    op.create_table(
        "reaction_dataset_stoichiometry",
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("entry_name", sa.String(), nullable=False),
        sa.Column("molecule_id", sa.Integer(), nullable=False),
        sa.Column("coefficient", postgresql.DOUBLE_PRECISION(), nullable=False),
        sa.ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["reaction_dataset_entry.dataset_id", "reaction_dataset_entry.name"],
            onupdate="cascade",
            ondelete="cascade",
        ),
        sa.ForeignKeyConstraint(["dataset_id"], ["reaction_dataset.id"], ondelete="cascade"),
        sa.ForeignKeyConstraint(
            ["molecule_id"],
            ["molecule.id"],
        ),
        sa.PrimaryKeyConstraint("dataset_id", "entry_name", "molecule_id"),
    )
    op.create_index(
        "ix_reaction_dataset_stoichiometry_dataset_id", "reaction_dataset_stoichiometry", ["dataset_id"], unique=False
    )
    op.create_index(
        "ix_reaction_dataset_stoichiometry_entry_name", "reaction_dataset_stoichiometry", ["entry_name"], unique=False
    )
    op.create_index(
        "ix_reaction_dataset_stoichiometry_molecule_id", "reaction_dataset_stoichiometry", ["molecule_id"], unique=False
    )

    #####################################
    # New record (item) table
    #####################################
    op.create_table(
        "reaction_dataset_record",
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("entry_name", sa.String(), nullable=False),
        sa.Column("specification_name", sa.String(), nullable=False),
        sa.Column("record_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["dataset_id", "entry_name"],
            ["reaction_dataset_entry.dataset_id", "reaction_dataset_entry.name"],
            onupdate="cascade",
            ondelete="cascade",
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id", "specification_name"],
            ["reaction_dataset_specification.dataset_id", "reaction_dataset_specification.name"],
            onupdate="cascade",
            ondelete="cascade",
        ),
        sa.ForeignKeyConstraint(["dataset_id"], ["reaction_dataset.id"], ondelete="cascade"),
        sa.ForeignKeyConstraint(
            ["record_id"],
            ["reaction_record.id"],
        ),
        sa.PrimaryKeyConstraint("dataset_id", "entry_name", "specification_name"),
        sa.UniqueConstraint("dataset_id", "entry_name", "specification_name", name="ux_reaction_dataset_record_unique"),
    )
    op.create_index("ix_reaction_dataset_record_record_id", "reaction_dataset_record", ["record_id"], unique=False)

    ######################
    # Migrate data
    ######################
    conn = op.get_bind()
    empty_kw = get_empty_keywords_id(conn)

    op.execute(sa.text("UPDATE collection SET collection = 'reaction' where collection = 'reactiondataset'"))

    # rxn -> reaction
    # ie -> manybody
    op.execute(
        sa.text(
            """UPDATE collection
                   SET collection = 'reaction', collection_type = 'reaction'
                   FROM reaction_dataset
                   WHERE collection.collection = 'reaction'
                   AND reaction_dataset.id = collection.id
                   AND reaction_dataset.ds_type = 'rxn'"""
        )
    )

    op.execute(
        sa.text(
            """UPDATE collection
                   SET collection = 'manybody', collection_type = 'manybody'
                   FROM reaction_dataset
                   WHERE collection.collection = 'reaction'
                   AND reaction_dataset.id = collection.id
                   AND reaction_dataset.ds_type = 'ie'"""
        )
    )

    # Existing tables
    rxn_dataset_table = table(
        "reaction_dataset",
        column("id", sa.Integer),
        column("ds_type", sa.String),
        column("history", sa.JSON),
        column("history_keys", sa.JSON),
        column("alias_keywords", sa.JSON),
    )

    rxn_entry_table = table(
        "reaction_dataset_entry",
        column("name", sa.String),
        column("dataset_id", sa.Integer),
        column("attributes", sa.Integer),
        column("reaction_results", sa.JSON),
        column("stoichiometry", sa.JSON),
    )

    session = Session(conn)
    datasets = session.query(rxn_dataset_table).all()

    for ds in datasets:
        ds_type = ds.ds_type
        entries = session.query(rxn_entry_table).filter(rxn_entry_table.c.dataset_id == ds.id).all()

        # Test before ding migrations
        # Find all stoichiometries, and bail if there are more than 1 and they have different molecules
        for entry in entries:
            stoichs = list(entry.stoichiometry.items())
            if len(stoichs) == 0:
                continue

            if ds_type == "ie":
                # Only include stoichiometries if they don't end in a number...
                stoichs = [x for x in stoichs if x[0].rstrip("0123456789") == x[0]]

            if not all(x[1] == stoichs[0][1] for x in stoichs):
                raise RuntimeError("Multiple named stoichiometries with different molecules. Wasn't planning on this")

        # If an IE calculation, create an MB dataset
        if ds_type == "ie":
            # We have to add the manybody dataset
            conn.execute(sa.text("INSERT INTO manybody_dataset (id) VALUES (:ds_id)"), parameters=dict(ds_id=ds.id))

        # Loop over the history, forming specifications
        for idx, h in enumerate(ds.history):
            spec_name = f"spec_{idx}"
            spec = dict(zip(ds.history_keys, h))

            # keywords should be in alias_keywords, except for dftd3 directly run through the
            # composition planner......
            try:
                if spec["keywords"] is None:
                    kw = None
                else:
                    kw = ds.alias_keywords[spec["program"]][spec["keywords"]]
            except KeyError:
                if spec["program"] == "dftd3":
                    kw = empty_kw
                else:
                    raise RuntimeError(f"Missing entry from alias_keywords: {spec['program']}, {spec['keywords']}")

            if kw is None:
                kw = empty_kw
            if spec["basis"] is None:
                spec["basis"] = ""

            qc_spec_id = add_qc_spec(conn, spec["program"], spec["driver"], spec["method"], spec["basis"], kw, {})

            # For reactions, we modify in-place in the reaction dataset table
            if ds_type == "rxn":
                # Add the reaction spec
                conn.execute(
                    sa.text(
                        """INSERT INTO reaction_specification (program, singlepoint_specification_id, optimization_specification_id, keywords)
                       VALUES ('reaction', :spec_id, NULL, '{}'::jsonb)
                       ON CONFLICT DO NOTHING"""
                    ),
                    parameters=dict(spec_id=qc_spec_id),
                )

                rxn_spec_res = conn.execute(
                    sa.text(
                        """
                            SELECT id FROM reaction_specification
                            WHERE program = 'reaction'
                            AND singlepoint_specification_id = :spec_id
                            AND optimization_specification_id IS NULL
                            AND keywords = '{}'::jsonb"""
                    ),
                    parameters=dict(spec_id=qc_spec_id),
                )

                rxn_spec_id = rxn_spec_res.scalar()

                # Now add to reaction dataset specification
                conn.execute(
                    sa.text(
                        """INSERT INTO reaction_dataset_specification (dataset_id, name, specification_id)
                                VALUES (:ds_id, :spec_name, :spec_id)"""
                    ),
                    parameters=dict(ds_id=ds.id, spec_name=spec_name, spec_id=rxn_spec_id),
                )

                for entry in entries:
                    stoich_for_spec = entry.stoichiometry[spec["stoichiometry"]]

                    # molecule:coefficient pairs
                    stoich_mol_coeff = list(stoich_for_spec.items())

                    # Add these to the stoichiometry table
                    for mol_id, coeff in stoich_mol_coeff:
                        conn.execute(
                            sa.text(
                                """
                            INSERT INTO reaction_dataset_stoichiometry (dataset_id, entry_name, molecule_id, coefficient)
                            VALUES (:ds_id, :name, :mol_id, :coeff)
                            ON CONFLICT DO NOTHING
                        """
                            ),
                            parameters=dict(ds_id=ds.id, name=entry.name, mol_id=mol_id, coeff=coeff),
                        )

                    # This stoichiometry has molecule ids (as str) as keys
                    mol_ids = list(int(x) for x in stoich_for_spec.keys())

                    # Get info about all the singlepoint records making up this stoichiometry
                    r = conn.execute(
                        sa.text(
                            """SELECT sp.id,sp.molecule_id,br.status,br.created_on,br.modified_on
                                                FROM singlepoint_record sp
                                                INNER JOIN base_record br on sp.id = br.id
                                                WHERE sp.specification_id = :spec_id
                                                AND sp.molecule_id IN :mol_ids
                                                """
                        ),
                        parameters=dict(spec_id=qc_spec_id, mol_ids=tuple(mol_ids)),
                    )

                    records = r.all()
                    found_mol_id = [x[1] for x in records]
                    all_status = [x[2] for x in records]

                    # Were all molecules found?
                    all_found = set(found_mol_id) == set(mol_ids)

                    if len(records) > 0:
                        # The user must have tried to compute these?
                        created_on = min(x[3] for x in records)
                        modified_on = max(x[4] for x in records)
                        rxn_id = create_reaction_record(
                            conn, ds.id, created_on, modified_on, rxn_spec_id, stoich_mol_coeff, records
                        )

                        # Now add to dataset
                        conn.execute(
                            sa.text(
                                """
                            INSERT INTO reaction_dataset_record (dataset_id, entry_name, specification_name, record_id)
                            VALUES (:ds_id, :entry_name, :spec_name, :record_id)"""
                            ),
                            parameters=dict(ds_id=ds.id, entry_name=entry.name, spec_name=spec_name, record_id=rxn_id),
                        )

            else:  # Is interaction energy (ie)
                # What type of interaction energy is this?
                ie_type = spec["stoichiometry"]

                if ie_type == "cp":
                    mb_kw = {"bsse_correction": "cp"}
                else:
                    mb_kw = {"bsse_correction": "none"}

                mb_kw_str = json.dumps(mb_kw)

                # Add the manybody spec
                conn.execute(
                    sa.text(
                        """INSERT INTO manybody_specification (program, singlepoint_specification_id, keywords)
                       VALUES ('manybody', :spec_id, :mb_kw)
                       ON CONFLICT DO NOTHING"""
                    ),
                    parameters=dict(spec_id=qc_spec_id, mb_kw=mb_kw_str),
                )

                mb_spec_res = conn.execute(
                    sa.text(
                        """
                            SELECT id FROM manybody_specification
                            WHERE program = 'manybody'
                            AND singlepoint_specification_id = :spec_id
                            AND keywords = :mb_kw"""
                    ),
                    parameters=dict(spec_id=qc_spec_id, mb_kw=mb_kw_str),
                )

                mb_spec_id = mb_spec_res.scalar()

                # Now add to manybody dataset specification
                conn.execute(
                    sa.text(
                        """INSERT INTO manybody_dataset_specification (dataset_id, name, specification_id)
                                VALUES (:ds_id, :spec_name, :spec_id)"""
                    ),
                    parameters=dict(ds_id=ds.id, spec_name=spec_name, spec_id=mb_spec_id),
                )

                for entry in entries:
                    # we only want the main molecule ("stoichiometry" key that is without a number)
                    stoich_for_spec = entry.stoichiometry[spec["stoichiometry"]]

                    # Should only be one molecule
                    assert len(stoich_for_spec) == 1

                    mol_id = int(list(stoich_for_spec.keys())[0])

                    # Add the entry
                    conn.execute(
                        sa.text(
                            """
                        INSERT INTO manybody_dataset_entry (dataset_id, name, initial_molecule_id, attributes, additional_keywords)
                        VALUES (:ds_id, :name, :mol_id, :attrib, '{}'::jsonb)
                        ON CONFLICT DO NOTHING
                        """
                        ),
                        parameters=dict(
                            ds_id=ds.id, name=entry.name, mol_id=mol_id, attrib=json.dumps(entry.attributes)
                        ),
                    )

                    # See if this parent molecule has been computed
                    # (so we can get created_on, etc)
                    r = conn.execute(
                        sa.text(
                            """SELECT sp.id,sp.molecule_id,br.status,br.created_on,br.modified_on
                               FROM singlepoint_record sp
                               INNER JOIN base_record br on sp.id = br.id
                               WHERE sp.specification_id = :spec_id
                               AND sp.molecule_id = :mol_id
                               """
                        ),
                        parameters=dict(spec_id=qc_spec_id, mol_id=mol_id),
                    )

                    record = r.one_or_none()

                    # Just add an MBE service with this molecule, and let it
                    # find all the fragments

                    if record is not None:
                        # The user must have tried to compute these?
                        created_on = record[3]
                        modified_on = record[4]
                        mb_id = create_manybody_record(conn, ds.id, created_on, modified_on, mb_spec_id, mol_id, record)

                        # Now add to dataset
                        conn.execute(
                            sa.text(
                                """
                            INSERT INTO manybody_dataset_record (dataset_id, entry_name, specification_name, record_id)
                            VALUES (:ds_id, :entry_name, :spec_name, :record_id)"""
                            ),
                            parameters=dict(ds_id=ds.id, entry_name=entry.name, spec_name=spec_name, record_id=mb_id),
                        )

                # Remove from the reaction dataset table
                conn.execute(sa.text("DELETE FROM reaction_dataset WHERE id = :ds_id"), parameters=dict(ds_id=ds.id))

    #########################################
    # Modify existing reaction_dataset table
    #########################################

    # Make columns not nullable
    op.alter_column("reaction_dataset_entry", "attributes", nullable=False)

    # Drop unused columns
    op.drop_column("reaction_dataset_entry", "extras")
    op.drop_column("reaction_dataset_entry", "reaction_results")
    op.drop_column("reaction_dataset_entry", "stoichiometry")
    op.drop_column("reaction_dataset", "default_program")
    op.drop_column("reaction_dataset", "default_driver")
    op.drop_column("reaction_dataset", "alias_keywords")
    op.drop_column("reaction_dataset", "default_keywords")
    op.drop_column("reaction_dataset", "ds_type")
    op.drop_column("reaction_dataset", "history_keys")
    op.drop_column("reaction_dataset", "history")
    op.drop_column("reaction_dataset", "default_units")
    op.drop_column("reaction_dataset", "default_benchmark")

    # ### end Alembic commands ###


def downgrade():
    raise RuntimeError("Cannot downgrade")
