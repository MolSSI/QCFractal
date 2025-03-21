from __future__ import annotations

import math
import sys
from typing import TYPE_CHECKING

from parsl.executors import ThreadPoolExecutor, HighThroughputExecutor
from parsl.providers import SlurmProvider, TorqueProvider, LSFProvider

from qcfractalcompute.config import (
    ExecutorConfig,
    LocalExecutorConfig,
    SlurmExecutorConfig,
    TorqueExecutorConfig,
    LSFExecutorConfig,
    CustomExecutorConfig,
)

if TYPE_CHECKING:
    from parsl.executors.base import ParslExecutor


def build_executor(executor_label: str, executor_config: ExecutorConfig) -> ParslExecutor:
    """
    Build a Parsl executor from a given ExecutorConfig
    """

    ##############################################################################
    # General notes
    # -------------
    # Overall, what we call "task" is what Parsl calls a "worker". In Parsl,
    # a worker is responsible for running a single task at a time. So we tend
    # to think of stuff in terms of concurrent tasks rather than workers.
    ##############################################################################

    if executor_config.type == "local":
        assert isinstance(executor_config, LocalExecutorConfig)

        # Use a thread pool with a local provider
        # Always use just a single block in the provider
        # Pretty straightforward mapping from config to parsl config
        return ThreadPoolExecutor(
            label=executor_label,
            max_threads=executor_config.max_workers,
            **executor_config.extra_executor_options,
        )

        ####################################################################################
        # The code below may make sense at some point, but closing a HighThroughputExecutor
        # doesn't seem to work properly (it leaves threads/processes running).
        ####################################################################################
        ## Use a HighThroughputExecutor with a local provider
        ## Always use just a single block in the provider
        ## Pretty straightforward mapping from config to parsl config
        # return HighThroughputExecutor(
        #    label=executor_label,
        #    cores_per_worker=executor_config.cores_per_worker,
        #    mem_per_worker=executor_config.memory_per_worker,
        #    max_workers=executor_config.max_workers,
        #    address="127.0.0.1",
        #    provider=LocalProvider(
        #        init_blocks=1, max_blocks=1, worker_init=";".join(executor_config.worker_init)
        #    ),
        # )

    elif executor_config.type == "slurm":
        assert isinstance(executor_config, SlurmExecutorConfig)
        # Use a HighThroughputExecutor with a Slurm provider
        # Use blocks of size 1, so number of nodes = number of blocks
        # Pretty straightforward mapping from config to parsl config
        # We let the high-throughput executor handle max_workers (which is the
        # max *per node*, not overall)

        # User specifies resources per worker, so convert to resources per node
        mem_per_node = executor_config.memory_per_worker * executor_config.workers_per_node
        cores_per_node = executor_config.cores_per_worker * executor_config.workers_per_node

        return HighThroughputExecutor(
            label=executor_label,
            cores_per_worker=executor_config.cores_per_worker,
            mem_per_worker=executor_config.memory_per_worker,
            address=executor_config.bind_address,
            provider=SlurmProvider(
                init_blocks=0,
                min_blocks=0,
                max_blocks=executor_config.max_nodes,
                nodes_per_block=1,
                mem_per_node=math.ceil(mem_per_node),
                cores_per_node=cores_per_node,
                exclusive=executor_config.exclusive,
                walltime=executor_config.walltime,
                account=executor_config.account,
                partition=executor_config.partition,
                worker_init=";".join(executor_config.worker_init),
                scheduler_options="\n".join(executor_config.scheduler_options),
            ),
        )

    elif executor_config.type == "torque":
        assert isinstance(executor_config, TorqueExecutorConfig)
        # Use a HighThroughputExecutor with a Torque provider
        # Use blocks of size 1, so number of nodes = number of blocks
        # Pretty straightforward mapping from config to parsl config
        # We let the high-throughput executor handle max_workers (which is the
        # max *per node*, not overall)

        # User specifies resources per worker, so convert to resources per node
        cores_per_node = executor_config.cores_per_worker * executor_config.workers_per_node

        return HighThroughputExecutor(
            label=executor_label,
            cores_per_worker=executor_config.cores_per_worker,
            mem_per_worker=executor_config.memory_per_worker,
            address=executor_config.bind_address,
            provider=TorqueProvider(
                init_blocks=0,
                min_blocks=0,
                max_blocks=executor_config.max_nodes,
                nodes_per_block=1,
                walltime=executor_config.walltime,
                account=executor_config.account,
                queue=executor_config.queue,
                worker_init=";".join(executor_config.worker_init),
                scheduler_options="\n".join(executor_config.scheduler_options),
            ),
        )

    elif executor_config.type == "lsf":
        assert isinstance(executor_config, LSFExecutorConfig)
        # Use a HighThroughputExecutor with a LSF provider
        # Use blocks of size 1, so number of nodes = number of blocks
        # Pretty straightforward mapping from config to parsl config
        # We let the high-throughput executor handle max_workers (which is the
        # max *per node*, not overall)

        # User specifies resources per worker, so convert to resources per node
        cores_per_node = executor_config.cores_per_worker * executor_config.workers_per_node

        cores_per_block = None

        if executor_config.request_by_nodes is False:
            # For us, 1 block = 1 node
            cores_per_block = cores_per_node

        return HighThroughputExecutor(
            label=executor_label,
            cores_per_worker=executor_config.cores_per_worker,
            mem_per_worker=executor_config.memory_per_worker,
            address=executor_config.bind_address,
            provider=LSFProvider(
                init_blocks=0,
                min_blocks=0,
                max_blocks=executor_config.max_nodes,
                cores_per_block=cores_per_block,
                nodes_per_block=1,
                cores_per_node=cores_per_node,
                walltime=executor_config.walltime,
                project=executor_config.project,
                queue=executor_config.queue,
                request_by_nodes=executor_config.request_by_nodes,
                bsub_redirection=executor_config.bsub_redirection,
                worker_init=";".join(executor_config.worker_init),
                scheduler_options="\n".join(executor_config.scheduler_options),
            ),
        )

    elif executor_config.type == "custom":
        assert isinstance(executor_config, CustomExecutorConfig)
        raise RuntimeError("TODO")
        sys.path.append("/home/ben/work/qcarchive/servers/next/new_worker")

        m = importlib.import_module(executor_config.config_file)
        ex = m.get_executor()

        for tag in executor_config.compute_tags:
            self.tag_executor_map[tag] = ex.label

        self.executor_config_map[ex.label] = executor_config
    else:
        raise ValueError("Unknown executor type: {}".format(executor_config.type))
