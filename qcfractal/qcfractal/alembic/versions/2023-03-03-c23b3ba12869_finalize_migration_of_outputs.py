"""finalize migration of outputs

Revision ID: c23b3ba12869
Revises: 5a98e27e4c10
Create Date: 2023-03-03 14:56:36.329930

"""

import json
import lzma

import sqlalchemy as sa
import zstandard
from alembic import op
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import table, column

from qcportal.compression import compress, CompressionEnum

# revision identifiers, used by Alembic.
revision = "c23b3ba12869"
down_revision = "5a98e27e4c10"
branch_labels = None
depends_on = None


def decompress_old_string(compressed_data: bytes, compression_type: CompressionEnum) -> str:
    if compression_type == CompressionEnum.none:
        r = compressed_data
    elif compression_type == CompressionEnum.lzma:
        r = lzma.decompress(compressed_data)
    elif compression_type == CompressionEnum.zstd:
        r = zstandard.decompress(compressed_data)
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")
    return r.decode()


def upgrade():
    # Table with output data
    old_output_table = table(
        "output_store",
        column("id", sa.Integer),
        column("compression_type", sa.Enum(CompressionEnum)),
        column("compression_level", sa.Integer),
        column("data", sa.LargeBinary),
        column("value", sa.JSON),
        column("old_data", sa.LargeBinary),
    )

    conn = op.get_bind()
    session = Session(conn)

    old_outs = session.query(old_output_table).yield_per(10)

    for old_out in old_outs:
        if old_out.old_data is not None:
            old_data = decompress_old_string(old_out.old_data, old_out.compression_type)
            if old_data.startswith("{") and "error_type" in old_data:
                old_data = json.loads(old_data)
            new_data, ctype, clevel = compress(old_data, CompressionEnum.zstd)
        elif old_out.data is None:
            assert old_out.compression_type is None or old_out.compression_type == "none"
            new_data, ctype, clevel = compress(old_out.value, CompressionEnum.zstd)
        else:
            new_data = None

        if new_data is not None:
            conn.execute(
                sa.text(
                    """UPDATE output_store SET
                         value=NULL,
                         old_data=NULL,
                         compression_type=:ctype,
                         compression_level=:clevel,
                         data=:cdata
                       WHERE id = :id"""
                ),
                parameters={"id": old_out.id, "cdata": new_data, "ctype": ctype, "clevel": clevel},
            )

    op.drop_column("output_store", "old_data")
    op.drop_column("output_store", "value")
    op.alter_column("output_store", "data", nullable=False)
    op.alter_column("output_store", "compression_level", nullable=False)
    op.alter_column("output_store", "compression_type", nullable=False)


def downgrade():
    raise RuntimeError("Cannot downgrade")
    pass
