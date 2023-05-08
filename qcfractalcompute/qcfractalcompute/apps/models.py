from __future__ import annotations

import pydantic

from qcportal.compression import decompress, CompressionEnum


class AppTaskResult(pydantic.BaseModel):
    """
    A result that is returned from a parsl future
    """

    success: bool
    walltime: float
    result_compressed: bytes

    @property
    def result(self):
        return decompress(self.result_compressed, CompressionEnum.zstd)
