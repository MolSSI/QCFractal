from __future__ import annotations

from typing import Any

from qcfractal.components.record_db_models import OutputStoreORM
from qcportal.compression import compress, CompressionEnum
from qcportal.record_models import OutputTypeEnum


def create_output_orm(output_type: OutputTypeEnum, output: Any) -> OutputStoreORM:
    compressed_out, compression_type, compression_level = compress(output, CompressionEnum.zstd)
    out_orm = OutputStoreORM(
        output_type=output_type,
        compression_type=compression_type,
        compression_level=compression_level,
        data=compressed_out,
    )
    return out_orm
