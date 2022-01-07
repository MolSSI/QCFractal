"""
Helpers for compressing data to send back to the server
"""

import json
import lzma
from typing import Union, Dict, Any

from qcelemental.models import AtomicResult, OptimizationResult


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
        compressed_outputs.append(
            dict(
                output_type="stdout",
                compression="lzma",
                compression_level=6,
                data=lzma.compress(stdout.encode("utf-8"), preset=6),
            )
        )

        update["stdout"] = None

    if stderr is not None:
        compressed_outputs.append(
            dict(
                output_type="stderr",
                compression="lzma",
                compression_level=6,
                data=lzma.compress(stderr.encode("utf-8"), preset=6),
            )
        )

        compressed_outputs["stderr"] = lzma.compress(stderr.encode("utf-8"), preset=6)
        update["stderr"] = None

    if error is not None:
        compressed_outputs.append(
            dict(
                output_type="error",
                compression="lzma",
                compression_level=6,
                data=lzma.compress(json.dumps(error).encode("utf-8"), preset=6),
            )
        )

        update["error"] = None

    extras = result.extras
    update["extras"] = extras
    if compressed_outputs:
        update["extras"]["_qcfractal_compressed_outputs"] = compressed_outputs

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
    Compress outputs inside results, storing them in extras

    The compressed outputs are stored in extras. For OptimizationResult, the outputs for the optimization
    are stored in the extras field of the OptimizationResult, while the outputs for the trajectory
    are stored in the extras field for the AtomicResults within the trajectory
    """

    ret = {}
    for k, result in results.items():
        if isinstance(result, AtomicResult):
            ret[k] = _compress_common(result)
        elif isinstance(result, OptimizationResult):
            ret[k] = _compress_optimizationresult(result)
        else:
            ret[k] = result

    return ret
