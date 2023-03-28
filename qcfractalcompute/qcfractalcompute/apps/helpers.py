from __future__ import annotations

import json
import shutil
import subprocess
from functools import lru_cache
from typing import Optional, Dict, Tuple, List, Any

from qcfractalcompute.run_scripts import get_script_path

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


def run_apptainer(sif_path: str, command: List[str], volumes: List[Tuple[str, str]]):

    cmd = [get_apptainer_cmd()]

    volumes_tmp = [f"{v[0]}:{v[1]}" for v in volumes]
    cmd.extend(["run", "--bind", ",".join(volumes_tmp), sif_path])
    cmd.extend(command)

    proc_result = subprocess.run(cmd, capture_output=True, text=True)

    if proc_result.returncode == 0:
        return json.loads(proc_result.stdout)
    else:
        raise RuntimeError(
            f"QCEngine failed with error code {proc_result.returncode}\n"
            f"stdout: {proc_result.stdout}\n"
            f"stderr: {proc_result.stderr}"
        )


@lru_cache()
def get_conda_env_conda(
    conda_env_name: Optional[str],
) -> Dict[str, Any]:

    env_script_path = get_script_path("conda_list_env.sh")
    if conda_env_name:
        cmd = ["conda", "run", "-n", conda_env_name, "/bin/bash", env_script_path]
    else:
        cmd = ["/bin/bash", env_script_path]

    conda_env = subprocess.check_output(cmd, universal_newlines=True)
    return json.loads(conda_env)


@lru_cache()
def get_conda_env_apptainer(sif_path: str) -> Dict[str, Any]:

    # Add conda environment to the provenance
    env_script_path = get_script_path("conda_list_env.sh")
    volumes = [(env_script_path, "/conda_list_env.sh")]

    cmd = ["/bin/bash", "/conda_list_env.sh"]
    return run_apptainer(sif_path, cmd, volumes)
