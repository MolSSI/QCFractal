"""
Base class for computation procedures
"""

from __future__ import annotations

import logging
from qcfractal.interface.models import (
    KVStore,
    AllResultTypes,
    WavefunctionProperties,
)


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.components.records.db_models import BaseResultORM
    from typing import Optional, Dict, Tuple, Any


logger = logging.getLogger(__name__)

_wfn_return_names = set(WavefunctionProperties._return_results_names)
_wfn_all_fields = set(WavefunctionProperties.__fields__.keys())


def retrieve_outputs(
    storage_socket: SQLAlchemySocket, session: Session, result: AllResultTypes, base_result: BaseResultORM
):
    """
    Retrieves (possibly compressed) outputs from a result (AtomicResult, OptimizationResult)
    """

    # Get the compressed outputs if they exist
    stdout = result.extras.pop("_qcfractal_compressed_stdout", None)
    stderr = result.extras.pop("_qcfractal_compressed_stderr", None)
    error = result.extras.pop("_qcfractal_compressed_error", None)

    # Create KVStore objects from these
    if stdout is not None:
        stdout = KVStore(**stdout)
    if stderr is not None:
        stderr = KVStore(**stderr)
    if error is not None:
        error = KVStore(**error)

    # This shouldn't happen, but if they aren't compressed, check for uncompressed
    if stdout is None and result.stdout is not None:
        logger.warning(f"Found uncompressed stdout for result id {result.id}")
        stdout = KVStore(data=result.stdout)
    if stderr is None and result.stderr is not None:
        logger.warning(f"Found uncompressed stderr for result id {result.id}")
        stderr = KVStore(data=result.stderr)
    if error is None and result.error is not None:
        logger.warning(f"Found uncompressed error for result id {result.id}")
        error = KVStore(data=result.error)

    storage_socket.procedure.update_outputs(session, base_result, stdout=stdout, stderr=stderr, error=error)


def wavefunction_helper(
    storage_socket: SQLAlchemySocket, session: Session, wavefunction: Optional[WavefunctionProperties]
) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    if wavefunction is None:
        return None, None

    wfn_dict = wavefunction.dict()
    available = set(wfn_dict.keys()) - {"restricted", "basis"}
    return_map = {k: wfn_dict[k] for k in wfn_dict.keys() & _wfn_return_names}

    # Dictionary contains metadata about the wavefunction. It is stored with the result, not
    # in the wavefunction_store table
    info_dict = {
        "available": list(available),
        "restricted": wavefunction.restricted,
        "return_map": return_map,
    }

    # Extra fields are trimmed as we have a column *per* wavefunction structure.
    available_keys = wfn_dict.keys() - _wfn_return_names
    if available_keys > _wfn_all_fields:
        logger.warning(f"Too much wavefunction data for result, removing extra data.")
        available_keys &= _wfn_all_fields

    wavefunction_save = {k: wfn_dict[k] for k in available_keys}
    wfn_prop = WavefunctionProperties(**wavefunction_save)
    wfn_data_id = storage_socket.wavefunction.add([wfn_prop], session=session)[0]
    return wfn_data_id, info_dict
