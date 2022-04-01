"""
Helpers for compressing data to send back to the server
"""

from typing import Union, Dict, Any

from qcelemental.models import AtomicResult, OptimizationResult

from qcportal.compression import CompressionEnum
from qcportal.nativefiles import NativeFile
from qcportal.outputstore.models import OutputStore, OutputTypeEnum


def _compress_common(
    result: Union[AtomicResult, OptimizationResult],
):
    """
    Compresses outputs of an AtomicResult or OptimizationResult, storing them in extras
    """

    stdout = result.stdout
    stderr = result.stderr
    error = result.error

    compressed_outputs = []
    update = {}

    if stdout is not None:
        new_stdout = OutputStore.compress(OutputTypeEnum.stdout, stdout, CompressionEnum.lzma, 6)
        compressed_outputs.append(new_stdout)
        update["stdout"] = None

    if stderr is not None:
        new_stderr = OutputStore.compress(OutputTypeEnum.stderr, stderr, CompressionEnum.lzma, 6)
        compressed_outputs.append(new_stderr)
        update["stderr"] = None

    if error is not None:
        new_error = OutputStore.compress(OutputTypeEnum.error, error.dict(), CompressionEnum.lzma, 6)
        compressed_outputs.append(new_error)
        update["error"] = None

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
        return

    compressed_nf = {}
    for name, data in result.native_files.items():
        nf = NativeFile.compress(name, data, CompressionEnum.lzma, 6)

        compressed_nf[name] = nf

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
        else:
            ret[k] = result

    return ret
