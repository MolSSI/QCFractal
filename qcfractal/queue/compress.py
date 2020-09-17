"""
Helpers for compressing data to send back to the server
"""

from typing import Union, Optional, Dict
from ..interface.models import KVStore, CompressionEnum
from qcelemental.models import AtomicResult, OptimizationResult


def _compress_common(
    result: Union[AtomicResult, OptimizationResult],
    compression: CompressionEnum = CompressionEnum.lzma,
    compression_level: int = None,
):
    """
    Compresses outputs of an AtomicResult or OptimizationResult, storing them in extras
    """

    stdout = result.stdout
    stderr = result.stderr
    error = result.error

    extras = result.extras
    update = {}
    if stdout is not None:
        extras["_qcfractal_compressed_stdout"] = KVStore.compress(stdout, compression, compression_level)
        update["stdout"] = None
    if stderr is not None:
        extras["_qcfractal_compressed_stderr"] = KVStore.compress(stderr, compression, compression_level)
        update["stderr"] = None
    if error is not None:
        extras["_qcfractal_compressed_error"] = KVStore.compress(error, compression, compression_level)
        update["error"] = None

    update["extras"] = extras
    return result.copy(update=update)


def _compress_optimizationresult(
    result: OptimizationResult,
    compression: CompressionEnum = CompressionEnum.lzma,
    compression_level: Optional[int] = None,
):
    """
    Compresses outputs inside an OptimizationResult, storing them in extras

    Outputs for the AtomicResults stored in the trajectory will be stored in the extras for that AtomicResult
    """

    # Handle the trajectory
    trajectory = [_compress_common(x, compression, compression_level) for x in result.trajectory]
    result = result.copy(update={"trajectory": trajectory})

    # Now handle the outputs of the optimization itself
    return _compress_common(result, compression, compression_level)


def compress_results(
    results: Dict[str, Union[AtomicResult, OptimizationResult]],
    compression: CompressionEnum = CompressionEnum.lzma,
    compression_level: int = None,
):
    """
    Compress outputs inside results, storing them in extras

    The compressed outputs are stored in extras. For OptimizationResult, the outputs for the optimization
    are stored in the extras field of the OptimizationResult, while the outputs for the trajectory
    are stored in the extras field for the AtomicResults within the trajectory
    """

    ret = {}
    for k, result in results.items():
        if isinstance(result, AtomicResult):
            ret[k] = _compress_common(result, compression, compression_level)
        elif isinstance(result, OptimizationResult):
            ret[k] = _compress_optimizationresult(result, compression, compression_level)
        else:
            ret[k] = result

    return ret
