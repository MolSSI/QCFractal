from __future__ import annotations

import os
import tempfile

from qcfractalcompute.config import ExecutorConfig


def geometric_nextchain_conda_app(
    record_id: int, function_kwargs_compressed: bytes, executor_config: ExecutorConfig, conda_env_name: str
):
    import json
    import subprocess
    from qcportal.compression import decompress, CompressionEnum
    from qcfractalcompute.apps.helpers import get_conda_env_conda
    from qcfractalcompute.run_scripts import get_script_path

    script_path = get_script_path("geometric_nextchain.py")

    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = str(executor_config.cores_per_worker)
    env["MKL_NUM_THREADS"] = str(executor_config.cores_per_worker)

    function_kwargs = decompress(function_kwargs_compressed, CompressionEnum.zstd)

    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(function_kwargs, f)
        f.flush()

        if conda_env_name:
            cmd = ["conda", "run", "-n", conda_env_name, "python3", script_path, str(record_id), f.name]
        else:
            cmd = ["python3", script_path, str(record_id), f.name]

        proc_result = subprocess.run(cmd, capture_output=True, text=True, cwd=executor_config.scratch_directory)

        if proc_result.returncode == 0:
            ret = json.loads(proc_result.stdout)
        else:
            raise RuntimeError(
                f"QCEngine failed with error code {proc_result.returncode}\n"
                f"stdout: {proc_result.stdout}\n"
                f"stderr: {proc_result.stderr}"
            )

        # Add conda environment to the provenance
        if "provenance" in ret:
            ret["provenance"]["conda_environment"] = get_conda_env_conda(conda_env_name)

        return ret


def geometric_nextchain_apptainer_app(
    record_id: int,
    function_kwargs_compressed: bytes,
    executor_config: ExecutorConfig,
    sif_path: str,
):
    import json
    from qcportal.compression import decompress, CompressionEnum
    from qcfractalcompute.apps.helpers import run_apptainer, get_conda_env_apptainer
    from qcfractalcompute.run_scripts import get_script_path

    script_path = get_script_path("geometric_nextchain.py")

    function_kwargs = decompress(function_kwargs_compressed, CompressionEnum.zstd)

    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(function_kwargs, f)
        f.flush()

        volumes = [(script_path, "/geometric_nextchain.py"), (f.name, "/input.json")]
        cmd = ["python3", "/geometric_nextchain.py", str(record_id), "/input.json"]

        ret = run_apptainer(sif_path, command=cmd, volumes=volumes)

        # Add conda environment to the provenance
        if "provenance" in ret:
            ret["provenance"]["conda_environment"] = get_conda_env_apptainer(sif_path)

        return ret
