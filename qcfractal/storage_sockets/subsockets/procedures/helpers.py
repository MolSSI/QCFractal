"""
Base class for computation procedures
"""

from __future__ import annotations

import logging
from ....interface.models import KVStore, CompressionEnum, AllResultTypes, WavefunctionProperties

logger = logging.getLogger(__name__)

_wfn_return_names = set(WavefunctionProperties._return_results_names)
_wfn_all_fields = set(WavefunctionProperties.__fields__.keys())

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...sqlalchemy_socket import SQLAlchemySocket
    from ...models import BaseResultORM, TaskQueueORM
    from typing import Optional, Union, Dict, Tuple, Any


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

    # delete existing
    to_delete = []
    if base_result.stdout is not None:
        to_delete.append(base_result.stdout)
    if base_result.stderr is not None:
        to_delete.append(base_result.stderr)
    if base_result.error is not None:
        to_delete.append(base_result.error)

    base_result.stdout = output_helper(storage_socket, session, stdout)
    base_result.stderr = output_helper(storage_socket, session, stderr)
    base_result.error = output_helper(storage_socket, session, error)

    # Can now safely delete the old ones now that they are not being referred to
    storage_socket.output_store.delete(to_delete, session=session)


def output_helper(
    storage_socket: SQLAlchemySocket, session, output: Optional[Union[Dict, str, KVStore]]
) -> Optional[int]:
    if output is None:
        return None

    if isinstance(output, KVStore):
        output_id = storage_socket.output_store.add([output], session=session)[0]
    else:
        compressed = KVStore.compress(output, CompressionEnum.lzma, 1)
        output_id = storage_socket.output_store.add([compressed], session=session)[0]
    return output_id


def wavefunction_helper(
    storage_socket, session, wavefunction: Optional[WavefunctionProperties]
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
