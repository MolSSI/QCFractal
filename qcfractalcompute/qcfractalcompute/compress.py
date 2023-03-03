"""
Helpers for compressing data to send back to the server
"""

from typing import Union, Dict, Any

from qcelemental.models import AtomicResult, OptimizationResult

from qcportal.compression import CompressionEnum, compress
from qcportal.generic_result import GenericTaskResult


def _compress_common(
    result: Union[AtomicResult, OptimizationResult, GenericTaskResult],
):
    """
    Compresses outputs of an AtomicResult or OptimizationResult, storing them in extras
    """

    stdout = result.stdout
    stderr = result.stderr
    error = result.error

    compressed_outputs = {}
    update = {}

    if stdout is not None:
        new_stdout, ctype, clevel = compress(stdout, CompressionEnum.zstd)
        compressed_outputs["stdout"] = {"compression_type": ctype, "compression_level": clevel, "data": new_stdout}
        update["stderr"] = None

    if stderr is not None:
        new_stderr, ctype, clevel = compress(stderr, CompressionEnum.zstd)
        compressed_outputs["stderr"] = {"compression_type": ctype, "compression_level": clevel, "data": new_stderr}
        update["stderr"] = None

    if error is not None:
        new_error, ctype, clevel = compress(error.dict(), CompressionEnum.zstd)
        compressed_outputs["error"] = {"compression_type": ctype, "compression_level": clevel, "data": new_error}
        update["stderr"] = None

    update["extras"] = result.extras
    if compressed_outputs:
        update["extras"]["_qcfractal_compressed_outputs"] = compressed_outputs

    return result.copy(update=update)


def _compress_native_files(
    result: Union[AtomicResult, OptimizationResult],
):
    """
    Compresses outputs and native files, storing them in extras
    """

    if not result.native_files:
        return result

    compressed_nf = {}
    for name, data in result.native_files.items():
        nf, ctype, clevel = compress(data, CompressionEnum.zstd)

        compressed_nf[name] = {"compression_type": ctype, "compression_level": clevel, "data": nf}

    update = {"native_files": {}}

    update["extras"] = result.extras
    update["extras"]["_qcfractal_compressed_native_files"] = compressed_nf

    return result.copy(update=update)


def _compress_optimizationresult(
    result: OptimizationResult,
):
    """
    Compresses outputs inside an OptimizationResult, storing them in extras

    Outputs for the AtomicResults stored in the trajectory will be stored in the extras for that AtomicResult
    """

    # Handle the trajectory
    trajectory = [_compress_common(x) for x in result.trajectory]
    result = result.copy(update={"trajectory": trajectory})

    # Now handle the outputs of the optimization itself
    return _compress_common(result)


def compress_results(
    results: Dict[str, Any],
):
    """
    Compress outputs and native files inside results, storing them in extras

    The compressed outputs are stored in extras. For OptimizationResult, the outputs for the optimization
    are stored in the extras field of the OptimizationResult, while the outputs for the trajectory
    are stored in the extras field for the AtomicResults within the trajectory
    """

    ret = {}
    for k, result in results.items():
        if isinstance(result, AtomicResult):
            ret[k] = _compress_common(result)
            ret[k] = _compress_native_files(ret[k])
        elif isinstance(result, OptimizationResult):
            ret[k] = _compress_optimizationresult(result)
        elif isinstance(result, GenericTaskResult):
            ret[k] = _compress_common(result)
        else:
            ret[k] = result

    return ret
