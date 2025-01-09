from __future__ import annotations

import copy

import pytest
import yaml

from qcfractalcompute.config import SlurmExecutorConfig, FractalComputeConfig

_base_config = {
    "cluster": "testcluster",
    "server": {
        "fractal_uri": "http://localhost:7777/",
    },
    "executors": {},
}


@pytest.mark.parametrize("time_str", ["02:01:59", "72:00:00", "10:00:00"])
def test_manager_config_walltime(time_str):
    # Walltimes in a yaml can be parsed as seconds. Test we convert that correctly

    # Walltime as a string
    config_yaml = f"""
        queue_tags:
          - '*'
        cores_per_worker: 1
        memory_per_worker: 1.0
        max_nodes: 1
        workers_per_node: 1
        walltime: "{time_str}"
    """

    config = yaml.load(config_yaml, yaml.SafeLoader)
    executor_config = SlurmExecutorConfig(**config)
    assert executor_config.walltime == time_str

    # Walltime without quotes (gets converted by yaml to int)
    config_yaml = f"""
        queue_tags:
          - '*'
        cores_per_worker: 1
        memory_per_worker: 1.0
        max_nodes: 1
        workers_per_node: 1
        walltime: {time_str}
    """

    config = yaml.load(config_yaml, yaml.SafeLoader)
    executor_config = SlurmExecutorConfig(**config)
    assert executor_config.walltime == time_str


def test_manager_config_durations(tmp_path):
    base_folder = str(tmp_path)
    base_config = copy.deepcopy(_base_config)

    base_config["update_frequency"] = "900"
    base_config["max_idle_time"] = "3600"
    manager_config = FractalComputeConfig(base_folder=base_folder, **base_config)
    assert manager_config.update_frequency == 900
    assert manager_config.max_idle_time == 3600

    base_config["update_frequency"] = 900
    base_config["max_idle_time"] = 3600
    manager_config = FractalComputeConfig(base_folder=base_folder, **base_config)
    assert manager_config.update_frequency == 900
    assert manager_config.max_idle_time == 3600

    base_config["update_frequency"] = "3d4h80m09s"
    base_config["max_idle_time"] = "1d8h99m77s"
    manager_config = FractalComputeConfig(base_folder=base_folder, **base_config)
    assert manager_config.update_frequency == 278409
    assert manager_config.max_idle_time == 121217

    base_config["update_frequency"] = "3:04:80:9"
    base_config["max_idle_time"] = "1:8:99:77"
    manager_config = FractalComputeConfig(base_folder=base_folder, **base_config)
    assert manager_config.update_frequency == 278409
    assert manager_config.max_idle_time == 121217
