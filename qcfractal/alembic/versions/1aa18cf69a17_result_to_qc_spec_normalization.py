"""result to qc_spec normalization

Revision ID: 1aa18cf69a17
Revises: 3ae67d9668c0
Create Date: 2020-07-13 14:50:31.156948

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.expression import cast
from qcfractal.storage_sockets.models import QCSpecORM, KeywordsORM
from qcfractal.interface.models.records import DriverEnum


# revision identifiers, used by Alembic.
revision = "1aa18cf69a17"
down_revision = "3ae67d9668c0"
branch_labels = None
depends_on = None


def upgrade():

    op.add_column("result", sa.Column("qc_spec", sa.Integer))
    op.create_foreign_key(
        "result_qc_spec_fkey", "result", "qc_spec", ["qc_spec"], ["id"], ondelete="SET NULL",
    )
    # current state of the table in the database
    result_table = sa.Table(
        "result",
        sa.MetaData(),
        sa.Column("id", sa.Integer, sa.ForeignKey("base_result.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("program", sa.String(100), nullable=False),
        sa.Column("driver", sa.String(100), nullable=False),  # Old column.
        sa.Column("basis", sa.String(100)),
        sa.Column("method", sa.String(100), nullable=False),
        sa.Column("keywords", sa.Integer, sa.ForeignKey("keywords.id")),
        sa.Column("qc_spec", sa.Integer, sa.ForeignKey("qc_spec.id")),  # New column
    )
    bind = op.get_bind()
    session = Session(bind=bind)

    unique_specs = (
        session.query(
            result_table.c.program,
            result_table.c.driver,
            result_table.c.basis,
            result_table.c.method,
            result_table.c.keywords,
        )
        .distinct()
        .all()
    )
    # from sqlalchemy.dialects import postgresql
    # query = session.query(KeywordsORM.id).filter(KeywordsORM.values.cast(JSONB) == cast('{"args": "unknown"}',JSONB))
    # print (query.statement.compile(dialect=postgresql.dialect()))
    kw_dummy = (
        session.query(KeywordsORM.id).filter(KeywordsORM.values.cast(JSONB) == cast({"args": "unknown"}, JSONB)).first()
    )
    kw_dummy = kw_dummy[0]
    # iterate over all specs, either add them or if existing, retreive their ids
    for spec in unique_specs:
        if spec.keywords is None:
            keyword_id = None
        else:
            keyword_id = session.query(KeywordsORM.id).filter_by(id=spec.keywords).first()[0]
            if keyword_id is None:
                keyword_id = kw_dummy

        query = session.query(QCSpecORM.id).filter(
            sa.and_(
                QCSpecORM.program == spec.program,
                QCSpecORM.driver == spec.driver,
                QCSpecORM.method == spec.method,
                QCSpecORM.basis == spec.basis,
                QCSpecORM.keywords == keyword_id,
            )
        )
        found = query.first()
        # if found in the qc_spec table, no insertion only set
        if found is not None:
            update_cnt = (
                session.query(result_table)
                .filter(
                    result_table.c.method == spec.method,
                    result_table.c.basis == spec.basis,
                    result_table.c.driver == spec.driver,
                    result_table.c.program == spec.program,
                    result_table.c.keywords == keyword_id,
                )
                .update({result_table.c.qc_spec: found[0]}, synchronize_session=False)
            )
        # if not found, insert and use the id
        else:
            new_spec = QCSpecORM(
                program=spec.program, basis=spec.basis, method=spec.method, driver=spec.driver, keywords=keyword_id,
            )
            session.add(new_spec)
            # have to commit, so the id field would be updated.
            session.commit()

            update_cnt = (
                session.query(result_table)
                .filter(
                    result_table.c.basis == spec.basis,
                    result_table.c.method == spec.method,
                    result_table.c.driver == spec.driver,
                    result_table.c.program == spec.program,
                    result_table.c.keywords == keyword_id,
                )
                .update({result_table.c.qc_spec: new_spec.id}, synchronize_session=False)
            )

        # dropping the columns
    op.drop_column("result", "program")
    op.drop_column("result", "basis")
    op.drop_column("result", "keywords")
    op.drop_column("result", "driver")
    op.drop_column("result", "method")

    op.create_unique_constraint("uix_result_keys", "result", ["qc_spec", "molecule"])


def downgrade():

    op.add_column("result", sa.Column("program", sa.String(100), nullable=False, server_default="null"))
    op.add_column("result", sa.Column("basis", sa.String(100)))
    op.add_column("result", sa.Column("keywords", sa.Integer))
    op.add_column("result", sa.Column("driver", sa.String(100), nullable=False, server_default="energy"))
    op.add_column("result", sa.Column("method", sa.String(100), nullable=False, server_default="null"))

    bind = op.get_bind()
    session = Session(bind=bind)

    result_table = sa.Table(
        "result",
        sa.MetaData(),
        sa.Column("id", sa.Integer, sa.ForeignKey("base_result.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("program", sa.String(100), nullable=False),
        sa.Column("driver", sa.String(100), nullable=False),
        sa.Column("basis", sa.String(100)),
        sa.Column("method", sa.String(100), nullable=False),
        sa.Column("keywords", sa.Integer, sa.ForeignKey("keywords.id")),
        sa.Column("qc_spec", sa.Integer, sa.ForeignKey("qc_spec.id")),  # to be dropped column
    )

    unique_spec_ids = session.query(result_table.c.qc_spec).distinct().all()

    for spec_id in unique_spec_ids:
        spec = (
            session.query(QCSpecORM.program, QCSpecORM.driver, QCSpecORM.basis, QCSpecORM.method, QCSpecORM.keywords)
            .filter(QCSpecORM.id == spec_id)
            .first()
        )
        update_cnt = (
            session.query(result_table)
            .filter(result_table.c.qc_spec == spec_id)
            .update(
                {
                    result_table.c.program: spec.program,
                    result_table.c.driver: spec.driver,
                    result_table.c.basis: spec.basis,
                    result_table.c.method: spec.method,
                    result_table.c.keywords: spec.keywords,
                },
                synchronize_session=False,
            )
        )

    op.drop_column("result", "qc_spec")

    op.alter_column("result", "program", server_default=None)
    op.alter_column("result", "driver", server_default=None)
    op.alter_column("result", "method", server_default=None)
    # ### end Alembic commands ###
