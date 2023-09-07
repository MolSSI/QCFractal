from __future__ import annotations

import os
import tempfile
from typing import Optional

from qcfractalcompute.config import ExecutorConfig
from .models import AppTaskResult


def qcengine_conda_app(
    record_id: int,
    function_kwargs_compressed: bytes,
    executor_config: ExecutorConfig,
    conda_env_name: Optional[str],
) -> AppTaskResult:
    import json
    from qcportal.compression import decompress, CompressionEnum
    from qcfractalcompute.apps.helpers import run_conda_subprocess
    from qcfractalcompute.run_scripts import get_script_path

    script_path = get_script_path("qcengine_compute.py")

    # This function handles both compute and compute_procedure
    # Record id can be ignored, but is here for consistency with other apps

    if executor_config.scratch_directory:
        scratch_directory = os.path.expandvars(executor_config.scratch_directory)
        scratch_directory = os.path.expanduser(scratch_directory)
    else:
        scratch_directory = None

    qcengine_options = {}
    qcengine_options["memory"] = executor_config.memory_per_worker
    qcengine_options["ncores"] = executor_config.cores_per_worker
    qcengine_options["scratch_directory"] = scratch_directory

    function_kwargs = decompress(function_kwargs_compressed, CompressionEnum.zstd)
    function_kwargs = {**function_kwargs, "task_config": qcengine_options}

    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(function_kwargs, f)
        f.flush()

        cmd = ["python3", script_path, f.name]
        return run_conda_subprocess(conda_env_name, cmd, executor_config.scratch_directory, {})


def qcengine_apptainer_app(
    record_id: int,
    function_kwargs_compressed: bytes,
    executor_config: ExecutorConfig,
    sif_path: str,
) -> AppTaskResult:
    import json
    from qcportal.compression import decompress, CompressionEnum
    from qcfractalcompute.apps.helpers import run_apptainer
    from qcfractalcompute.run_scripts import get_script_path

    script_path = get_script_path("qcengine_compute.py")

    # This function handles both compute and compute_procedure
    # Record id can be ignored, but is here for consistency with other apps

    if executor_config.scratch_directory:
        scratch_directory = os.path.expandvars(executor_config.scratch_directory)
        scratch_directory = os.path.expanduser(scratch_directory)
    else:
        scratch_directory = None

    qcengine_options = {}
    qcengine_options["memory"] = executor_config.memory_per_worker
    qcengine_options["ncores"] = executor_config.cores_per_worker
    qcengine_options["scratch_directory"] = scratch_directory

    function_kwargs = decompress(function_kwargs_compressed, CompressionEnum.zstd)
    function_kwargs = {**function_kwargs, "task_config": qcengine_options}

    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(function_kwargs, f)
        f.flush()

        volumes = [(script_path, "/qcengine_compute.py"), (f.name, "/input.json")]
        cmd = ["python3", "/qcengine_compute.py", "/input.json"]

        return run_apptainer(sif_path, command=cmd, volumes=volumes)
