from __future__ import annotations

import json
import subprocess
import tempfile
from functools import lru_cache, partial
from typing import TYPE_CHECKING, Dict, List, Set, Any, Optional

import parsl

from qcfractalcompute.apps.geometric import geometric_nextchain_conda_app, geometric_nextchain_apptainer_app
from qcfractalcompute.apps.helpers import run_apptainer
from qcfractalcompute.apps.qcengine import qcengine_conda_app, qcengine_apptainer_app
from qcfractalcompute.run_scripts import get_script_path
from qcportal.record_models import RecordTask

if TYPE_CHECKING:
    from parsl.dataflow.dflow import DataFlowKernel
    from qcfractalcompute.config import FractalComputeConfig


@lru_cache()
def discover_programs_conda(conda_env_name: Optional[str]) -> Dict[str, Dict[str, Any]]:
    qcengine_list_path = get_script_path("qcengine_list.py")

    if conda_env_name:
        cmd = ["conda", "run", "-n", conda_env_name, "python3", qcengine_list_path]
    else:
        cmd = ["python3", qcengine_list_path]

    # Use a temporary dir. QCEngine will sometimes write files there (like timer.dat)
    with tempfile.TemporaryDirectory() as tmpdir:
        result = subprocess.check_output(cmd, universal_newlines=True, cwd=tmpdir)

    # QCEngine differentiates between programs and procedures, but we don't
    program_info = json.loads(result)

    # functions are always the same
    functions = {
        "qcengine.compute": {
            "programs": program_info,
            "app_function": partial(qcengine_conda_app, conda_env_name=conda_env_name),
        },
        "qcengine.compute_procedure": {
            "programs": program_info,
            "app_function": partial(qcengine_conda_app, conda_env_name=conda_env_name),
        },
    }

    if "geometric" in program_info:
        functions["geometric.qcf_neb.nextchain"] = {
            "programs": {"geometric": program_info["geometric"]},
            "app_function": partial(geometric_nextchain_conda_app, conda_env_name=conda_env_name),
        }

    return functions


@lru_cache()
def discover_programs_apptainer(sif_path: str) -> Dict[str, Dict[str, Any]]:
    qcengine_list_path = get_script_path("qcengine_list.py")

    program_info = run_apptainer(
        sif_path, command=["python3", "/qcengine_list.py"], volumes=[(qcengine_list_path, "/qcengine_list.py")]
    )

    # functions are always the same
    functions = {
        "qcengine.compute": {
            "programs": program_info,
            "app_function": partial(qcengine_apptainer_app, sif_path=sif_path),
        },
        "qcengine.compute_procedure": {
            "programs": program_info,
            "app_function": partial(qcengine_apptainer_app, sif_path=sif_path),
        },
    }

    if "geometric" in program_info:
        functions["geometric.qcf_neb.nextchain"] = {
            "programs": {"geometric": program_info["geometric"]},
            "app_function": partial(geometric_nextchain_apptainer_app, sif_path=sif_path),
        }

    return functions


class AppManager:
    def __init__(self, manager_config: FractalComputeConfig):
        # key is executor label
        self._parsl_apps = {}

        for executor_label, executor_config in manager_config.executors.items():
            self._parsl_apps[executor_label] = []

            # Merge in the global config into the executor-specific config
            use_current_env = executor_config.environments.use_manager_environment
            conda_envs = set(manager_config.environments.conda) | set(executor_config.environments.conda)

            # Same for apptainers
            apptainers = set(manager_config.environments.apptainer) | set(executor_config.environments.apptainer)

            # Check the current environment
            if use_current_env:
                qcengine_functions = discover_programs_conda(None)
                for qcengine_function_name, func_info in qcengine_functions.items():
                    self._parsl_apps[executor_label].append(
                        (qcengine_function_name, func_info["programs"], func_info["app_function"])
                    )

            # Check the conda environments in the config
            for conda_env in conda_envs:
                qcengine_functions = discover_programs_conda(conda_env)
                for qcengine_function_name, func_info in qcengine_functions.items():
                    self._parsl_apps[executor_label].append(
                        (qcengine_function_name, func_info["programs"], func_info["app_function"])
                    )

            for apptainer in apptainers:
                qcengine_functions = discover_programs_apptainer(apptainer)
                for qcengine_function_name, func_info in qcengine_functions.items():
                    self._parsl_apps[executor_label].append(
                        (qcengine_function_name, func_info["programs"], func_info["app_function"])
                    )

    def get_app(self, dflow_kernel: DataFlowKernel, executor_label: str, task: RecordTask) -> Any:
        task_programs = set(task.required_programs)

        if executor_label not in self._parsl_apps:
            raise KeyError(f"No executor available with label {executor_label}")

        for name, programs, func in self._parsl_apps[executor_label]:
            if task.function == name and task_programs.issubset(programs.keys()):
                return parsl.python_app(func, data_flow_kernel=dflow_kernel, executors=[executor_label])

        raise KeyError(
            f"No app available for executor {executor_label}, function {task.function}, programs {task_programs}"
        )

    def all_program_info(self, executor_label: Optional[str] = None) -> Dict[str, List[str]]:
        """Returns a dictionary of all program information.

        The dictionary is has keys of program name, and values of a list of available versions.
        """

        ret: Dict[str, Set[str]] = {}
        if executor_label is not None:
            for _, program_info, _ in self._parsl_apps[executor_label]:
                for prog, ver in program_info.items():
                    ret.setdefault(prog, set())
                    if ver is None:
                        ver = "unknown"
                    ret[prog].add(ver)
        else:
            for executor_label, executor_info in self._parsl_apps.items():
                for _, program_info, _ in executor_info:
                    for prog, ver in program_info.items():
                        ret.setdefault(prog, set())
                        if ver is None:
                            ver = "unknown"
                        ret[prog].add(ver)

        # Convert versions to a list
        return {k: list(v) for k, v in ret.items()}
