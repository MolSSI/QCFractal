import logging
import sqlalchemy as sa
import tqdm
import numpy as np
from alembic import op
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.sql.expression import func

from qcelemental.util import msgpackext_dumps, msgpackext_loads
from qcelemental.testing import compare_recursive

logger = logging.getLogger('alembic')

old_type = sa.JSON
new_type = BYTEA


def _get_colnames(columns):
    pairs = {(x, x + "_") for x in columns}
    old = [x[0] for x in pairs]
    new = [x[1] for x in pairs]
    return (pairs, old, new)


def _intermediate_table(table_name, columns):

    column_pairs, old_names, new_names = _get_colnames(columns)
    table_data = [table_name, sa.MetaData(), sa.Column("id", sa.Integer, primary_key=True)]
    table_data.extend([sa.Column(x, old_type) for x in old_names])
    table_data.extend([sa.Column(x, new_type) for x in new_names])
    table = sa.Table(*table_data)
    return table


def json_to_msgpack_table(table_name, block_size, converters):

    update_columns = list(converters)

    logger.info(f"Converting {table_name} from JSON to msgpack.")
    logger.info(f"Columns: {update_columns}")
    column_pairs, old_names, new_names = _get_colnames(update_columns)

    # Schema migration: add all the new columns.
    for col_old, col_new in column_pairs:
        op.add_column(table_name, sa.Column(col_new, new_type, nullable=True))

    # Declare a view of the table
    table = _intermediate_table(table_name, update_columns)

    connection = op.get_bind()

    num_records = connection.execute(f"select count(*) from {table_name}").scalar()

    old_columns = [getattr(table.c, x) for x in old_names]

    logger.info("Converting data, this may take some time...")
    for block in tqdm.tqdm(range(0, num_records, block_size)):

        # Pull chunk to migrate
        data = connection.execute(sa.select([
            table.c.id,
            *old_columns,
        ], order_by=table.c.id.asc(), offset=block, limit=block_size)).fetchall()

        # Convert chunk to msgpack
        update_blobs = []
        for values in data:
            id_ = values[0]
            # print(id_)
            # First is id, then msgpack convert
            row = {}
            for k, v in zip(new_names, values[1:]):
                row[k] = msgpackext_dumps(converters[k[:-1]](v))

            connection.execute(table.update().where(table.c.id == id_).values(**row))


def json_to_msgpack_table_dropcols(table_name, block_size, update_columns):

    column_pairs, old_names, new_names = _get_colnames(update_columns)
    for col in new_names:
        op.drop_column(table_name, col)


def json_to_msgpack_table_altercolumns(table_name, update_columns, nullable_true=None):

    if nullable_true is None:
        nullable_true = set()

    connection = op.get_bind()
    table = _intermediate_table(table_name, update_columns)

    column_pairs, old_names, new_names = _get_colnames(update_columns)
    num_records = connection.execute(f"select count(*) from {table_name}").scalar()

    old_columns = [getattr(table.c, x) for x in old_names]
    new_columns = [getattr(table.c, x) for x in new_names]

    logger.info(f"Checking converted columns...")
    # Pull chunk to migrate
    data = connection.execute(sa.select([
        table.c.id,
        *old_columns,
        *new_columns,
    ], order_by=table.c.id.asc())).fetchall()
    # ], limit=100, order_by=func.random())).fetchall()

    col_names = ["id"] + old_names + new_names
    for values in data:
        row = {k: v for k, v in zip(col_names, values)}
        # print(row["id"])
        # print(row.keys())
        # for k, v in row.items():
        #     print(k, v)

        for name in old_names:
            comp_data = msgpackext_loads(row[name + "_"])
            assert compare_recursive(comp_data, row[name])

            # try:
            #     print(name, comp_data.dtype, comp_data)
            # except:
            #     print(name, comp_data[0].dtype, comp_data)
            #     pass
    # raise Exception()
    logger.info(f"Dropping old columns and renaming new.")
    # Drop old tables and swamp new ones in.
    for old_name, new_name in column_pairs:
        nullable = False
        if old_name in nullable_true:
            nullable = True

        op.drop_column(table_name, old_name)
        op.alter_column(table_name, new_name, new_column_name=old_name, nullable=nullable)
