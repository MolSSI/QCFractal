"""
Helpers for compressing data to send back to the server
"""

from typing import Dict, Any

import numpy

from qcportal.compression import CompressionEnum, compress


def _compress_common(result: Dict[str, Any]):
    """
    Compresses outputs of an AtomicResult or OptimizationResult, storing them in extras
    """

    stdout = result.get("stdout", None)
    stderr = result.get("stderr", None)
    error = result.get("error", None)

    compressed_outputs = {}

    if stdout is not None:
        result["extras"].setdefault("_qcfractal_compressed_outputs", {})
        new_stdout, ctype, clevel = compress(stdout, CompressionEnum.zstd)
        compressed_outputs["stdout"] = {"compression_type": ctype, "compression_level": clevel, "data": new_stdout}
        result["stdout"] = None

    if stderr is not None:
        result["extras"].setdefault("_qcfractal_compressed_outputs", {})
        new_stderr, ctype, clevel = compress(stderr, CompressionEnum.zstd)
        compressed_outputs["stderr"] = {"compression_type": ctype, "compression_level": clevel, "data": new_stderr}
        result["stderr"] = None

    if error is not None:
        result["extras"].setdefault("_qcfractal_compressed_outputs", {})
        new_error, ctype, clevel = compress(error.dict(), CompressionEnum.zstd)
        compressed_outputs["error"] = {"compression_type": ctype, "compression_level": clevel, "data": new_error}
        result["error"] = None

    if compressed_outputs:
        result["extras"]["_qcfractal_compressed_outputs"] = compressed_outputs


def _compress_native_files(result: Dict[str, Any]):
    """
    Compresses outputs and native files, storing them in extras
    """

    native_files = result.get("native_files", None)
    if not native_files:
        return result

    compressed_nf = {}
    for name, data in native_files.items():
        nf, ctype, clevel = compress(data, CompressionEnum.zstd)
        compressed_nf[name] = {"compression_type": ctype, "compression_level": clevel, "data": nf}

    result["native_files"] = {}
    result["extras"]["_qcfractal_compressed_native_files"] = compressed_nf


def _compress_optimizationresult(result: Dict[str, Any]):
    """
    Compresses outputs inside an OptimizationResult, storing them in extras

    Outputs for the AtomicResults stored in the trajectory will be stored in the extras for that AtomicResult
    """

    # Handle the trajectory
    if result.get("trajectory", None):
        for x in result["trajectory"]:
            _compress_common(x)

    # Now handle the outputs of the optimization itself
    _compress_common(result)


def _convert_numpy(obj):
    if isinstance(obj, dict):
        return {k: _convert_numpy(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [_convert_numpy(v) for v in obj]
    elif isinstance(obj, numpy.ndarray):
        if obj.shape:
            return obj.ravel().tolist()
        else:
            return obj.tolist()
    else:
        return obj


def compress_result(result: Dict[str, Any]) -> bytes:
    """
    Compress outputs and native files inside results, storing them in extras. Then compress the whole result

    Outputs and native files are put into the database compressed, so no decompression is done
    until someone requests them (and then decompression happens on the client)

    The compressed outputs are stored in extras. For OptimizationResult, the outputs for the optimization
    are stored in the extras field of the OptimizationResult, while the outputs for the trajectory
    are stored in the extras field for the AtomicResults within the trajectory
    """

    result = _convert_numpy(result)
    schema_type = result.get("schema_name", None)

    if schema_type == "qcschema_output":
        _compress_common(result)
        _compress_native_files(result)
    elif schema_type == "qcschema_optimization_output":
        _compress_optimizationresult(result)
    elif schema_type == "qca_generic_task_result":
        _compress_common(result)
    else:
        pass

    # Compress the whole thing
    r, _, _ = compress(result, CompressionEnum.zstd)
    return r
