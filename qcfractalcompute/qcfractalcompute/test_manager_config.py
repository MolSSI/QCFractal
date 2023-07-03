from __future__ import annotations

import yaml

import pytest
from qcfractalcompute.config import SlurmExecutorConfig


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
