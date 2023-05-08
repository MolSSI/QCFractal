from __future__ import annotations

import os
import tempfile
from typing import Optional

from qcfractalcompute.config import ExecutorConfig
from .models import AppTaskResult


def geometric_nextchain_conda_app(
    record_id: int,
    function_kwargs_compressed: bytes,
    executor_config: ExecutorConfig,
    conda_env_name: Optional[str],
) -> AppTaskResult:
    import json
    from qcportal.compression import decompress, CompressionEnum
    from qcfractalcompute.apps.helpers import run_conda_subprocess
    from qcfractalcompute.run_scripts import get_script_path

    script_path = get_script_path("geometric_nextchain.py")

    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = str(executor_config.cores_per_worker)
    env["MKL_NUM_THREADS"] = str(executor_config.cores_per_worker)

    function_kwargs = decompress(function_kwargs_compressed, CompressionEnum.zstd)

    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(function_kwargs, f)
        f.flush()

        cmd = ["python3", script_path, str(record_id), f.name]
        return run_conda_subprocess(conda_env_name, cmd, executor_config.scratch_directory, env)


def geometric_nextchain_apptainer_app(
    record_id: int,
    function_kwargs_compressed: bytes,
    executor_config: ExecutorConfig,
    sif_path: str,
) -> AppTaskResult:
    import json
    from qcportal.compression import decompress, CompressionEnum
    from qcfractalcompute.apps.helpers import run_apptainer
    from qcfractalcompute.run_scripts import get_script_path

    script_path = get_script_path("geometric_nextchain.py")

    function_kwargs = decompress(function_kwargs_compressed, CompressionEnum.zstd)

    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(function_kwargs, f)
        f.flush()

        volumes = [(script_path, "/geometric_nextchain.py"), (f.name, "/input.json")]
        cmd = ["python3", "/geometric_nextchain.py", str(record_id), "/input.json"]

        return run_apptainer(sif_path, command=cmd, volumes=volumes)
