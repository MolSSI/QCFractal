"""
Helpers for parsing or manipulating records
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from qcfractal.components.outputstore.sockets import OutputStoreSocket
from qcfractal.components.records.db_models import RecordComputeHistoryORM
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractal.components.wavefunctions.sockets import WavefunctionSocket
from qcfractal.interface.models import (
    AllResultTypes,
)
from qcfractal.portal.components.outputstore import OutputStore, OutputTypeEnum, CompressionEnum
from qcfractal.portal.components.wavefunctions import WavefunctionProperties

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)

_wfn_all_fields = set(WavefunctionProperties.__fields__.keys())


def create_compute_history_entry(
    result: AllResultTypes,
) -> RecordComputeHistoryORM:
    """
    Retrieves status and (possibly compressed) outputs from a result, and creates
    a record computation history entry
    """

    history_orm = RecordComputeHistoryORM()
    history_orm.status = "complete" if result.success else "error"
    history_orm.provenance = result.provenance.dict()

    # Get the compressed outputs if they exist
    compressed_output = result.extras.pop("_qcfractal_compressed_outputs", None)

    if compressed_output is not None:
        all_outputs = [OutputStore(**x) for x in compressed_output]

    else:
        all_outputs = []

        # This shouldn't happen, but if they aren't compressed, check for uncompressed
        if result.stdout is not None:
            logger.warning(f"Found uncompressed stdout for record id {result.id}")
            stdout = OutputStore.compress(
                OutputTypeEnum.stdout, result.stdout, compression_type=CompressionEnum.lzma, compression_level=1
            )
            all_outputs.append(stdout)
        if result.stderr is not None:
            logger.warning(f"Found uncompressed stderr for record id {result.id}")
            stderr = OutputStore.compress(
                OutputTypeEnum.stderr, result.stderr, compression_type=CompressionEnum.lzma, compression_level=1
            )
            all_outputs.append(stderr)
        if result.error is not None:
            logger.warning(f"Found uncompressed error for record id {result.id}")
            error = OutputStore.compress(
                OutputTypeEnum.error, result.error.dict(), compression_type=CompressionEnum.lzma, compression_level=1
            )
            all_outputs.append(error)

    history_orm.outputs = [OutputStoreSocket.output_to_orm(x) for x in all_outputs]

    return history_orm


def wavefunction_helper(wavefunction: Optional[WavefunctionProperties]) -> Optional[WavefunctionStoreORM]:

    if wavefunction is None:
        return None

    wfn_dict = wavefunction.dict()
    available_keys = set(wfn_dict.keys())

    # Extra fields are trimmed as we have a column *per* wavefunction structure.
    extra_fields = available_keys - _wfn_all_fields
    if extra_fields:
        logger.warning(f"Too much wavefunction data for result, removing extra data: {extra_fields}")
        available_keys &= _wfn_all_fields

    wavefunction_save = {k: wfn_dict[k] for k in available_keys}
    wfn_prop = WavefunctionProperties(**wavefunction_save)
    return WavefunctionSocket.wavefunction_to_orm(wfn_prop)
