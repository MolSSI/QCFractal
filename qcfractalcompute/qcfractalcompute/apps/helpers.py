from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from typing import Optional, Dict, Tuple, List

from qcfractalcompute.compress import compress_result
from .models import AppTaskResult

_apptainer_cmd = None


def get_apptainer_cmd() -> str:
    global _apptainer_cmd

    if _apptainer_cmd is not None:
        return _apptainer_cmd

    _apptainer_cmd = shutil.which("apptainer")
    if _apptainer_cmd is None:
        _apptainer_cmd = shutil.which("singularity")
    if _apptainer_cmd is None:
        raise RuntimeError("apptainer or singularity not found in PATH")

    return _apptainer_cmd


def run_apptainer(sif_path: str, command: List[str], volumes: List[Tuple[str, str]]) -> AppTaskResult:
    cmd = [get_apptainer_cmd()]

    volumes_tmp = [f"{v[0]}:{v[1]}" for v in volumes]
    cmd.extend(["run", "--bind", ",".join(volumes_tmp), sif_path])
    cmd.extend(command)

    time_0 = time.time()
    proc_result = subprocess.run(cmd, capture_output=True, text=True)
    time_1 = time.time()

    if proc_result.returncode == 0:
        ret = json.loads(proc_result.stdout)
    else:
        msg = (
            f"Running in apptainer failed with error code {proc_result.returncode}\n"
            f"stdout: {proc_result.stdout}\n"
            f"stderr: {proc_result.stderr}"
        )

        ret = {"success": False, "error": {"error_type": "RuntimeError", "error_message": msg}}

    return AppTaskResult(
        success=ret["success"],
        walltime=time_1 - time_0,
        result_compressed=compress_result(ret),
    )


def run_conda_subprocess(
    conda_env_name: Optional[str], cmd: List[str], cwd: Optional[str], env: Dict[str, str]
) -> AppTaskResult:
    if cwd:
        cwd = os.path.expandvars(cwd)

    sub_env = os.environ.copy()
    sub_env.update(env)

    if conda_env_name:
        cmd = ["conda", "run", "-n", conda_env_name] + cmd

    time_0 = time.time()
    proc_result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd, env=sub_env)
    time_1 = time.time()

    if proc_result.returncode == 0:
        try:
            if proc_result.stdout == "":
                ret = {}
            else:
                ret = json.loads(proc_result.stdout)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse JSON from subprocess stdout: {proc_result.stdout}")
    else:
        msg = (
            f"Subprocess failed with error code {proc_result.returncode}\n"
            f"stdout: {proc_result.stdout}\n"
            f"stderr: {proc_result.stderr}"
        )

        ret = {"success": False, "error": {"error_type": "RuntimeError", "error_message": msg}}

    return AppTaskResult(
        success=ret["success"],
        walltime=time_1 - time_0,
        result_compressed=compress_result(ret),
    )
