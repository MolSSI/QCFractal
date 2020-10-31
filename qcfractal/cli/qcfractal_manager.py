"""
A command line interface to the qcfractal server.
"""

import argparse
import json
import logging
import os
import signal
from enum import Enum
from math import ceil
from typing import List, Optional, Union

import tornado.log
import yaml
from pydantic import Field, validator

import qcengine as qcng
import qcfractal

from ..interface.models import AutodocBaseSettings, ProtoModel
from . import cli_utils

__all__ = ["main"]

QCA_RESOURCE_STRING = "--resources process=1"

logger = logging.getLogger("qcfractal.cli")


class SettingsCommonConfig:
    env_prefix = "QCA_"
    case_insensitive = True
    extra = "forbid"


class AdapterEnum(str, Enum):
    dask = "dask"
    pool = "pool"
    parsl = "parsl"


class CommonManagerSettings(AutodocBaseSettings):
    """
    The Common settings are the settings most users will need to adjust regularly to control the nature of
    task execution and the hardware under which tasks are executed on. This block is often unique to each deployment,
    user, and manager and will be the most commonly updated options, even as config files are copied and reused, and
    even on the same platform/cluster.

    """

    adapter: AdapterEnum = Field(
        AdapterEnum.pool, description="Which type of Distributed adapter to run tasks through."
    )
    tasks_per_worker: int = Field(
        1,
        description="Number of concurrent tasks to run *per Worker* which is executed. Total number of concurrent "
        "tasks is this value times max_workers, assuming the hardware is available. With the "
        "pool adapter, and/or if max_workers=1, tasks_per_worker *is* the number of concurrent tasks.",
    )
    cores_per_worker: int = Field(
        qcng.config.get_global("ncores"),
        description="""
        Number of cores to be consumed by the Worker and distributed over the tasks_per_worker. These 
        cores are divided evenly, so it is recommended that quotient of cores_per_worker/tasks_per_worker 
        be a whole number else the core distribution is left up to the logic of the adapter. The default
        value is read from the number of detected cores on the system you are executing on.
        
        In the case of node-parallel tasks, this number means the number of cores per node.
        """,
        gt=0,
    )
    memory_per_worker: float = Field(
        qcng.config.get_global("memory"),
        description="Amount of memory (in GB) to be consumed and distributed over the tasks_per_worker. This memory is "
        "divided evenly, but is ultimately at the control of the adapter. Engine will only allow each of "
        "its calls to consume memory_per_worker/tasks_per_worker of memory. Total memory consumed by this "
        "manager at any one time is this value times max_workers. The default value is read "
        "from the amount of memory detected on the system you are executing on.",
        gt=0,
    )
    max_workers: int = Field(
        1,
        description="The maximum number of Workers which are allowed to be run at the same time. The total number of "
        "concurrent tasks will maximize at this quantity times tasks_per_worker."
        "The total number "
        "of Jobs on a cluster which will be started is equal to this parameter in most cases, and should "
        "be assumed 1 Worker per Job. Any exceptions to this will be documented. "
        "In node exclusive mode this is equivalent to the maximum number of nodes which you will consume. "
        "This must be a positive, non zero integer.",
        gt=0,
    )
    retries: int = Field(
        2,
        description="Number of retries that QCEngine will attempt for RandomErrors detected when running "
        "its computations. After this many attempts (or on any other type of error), the "
        "error will be raised.",
        ge=0,
    )
    scratch_directory: Optional[str] = Field(None, description="Scratch directory for Engine execution jobs.")
    verbose: bool = Field(
        False,
        description="Turn on verbose mode or not. In verbose mode, all messages from DEBUG level and up are shown, "
        "otherwise, defaults are all used for any logger.",
    )
    nodes_per_job: int = Field(
        1, description="The number of nodes to request per job. Only used by the Parsl adapter at present", gt=0
    )
    nodes_per_task: int = Field(
        1, description="The number of nodes to use for each tasks. Only relevant for node-parallel executables.", gt=0
    )
    cores_per_rank: int = Field(
        1,
        description="The number of cores per MPI rank for MPI-parallel applications. Only relevant for node-parallel"
        " codes and the most relevant to codes that with hybrid MPI+OpenMP parallelism (e.g., NWChem).",
    )

    class Config(SettingsCommonConfig):
        pass


class FractalServerSettings(AutodocBaseSettings):
    """
    Settings pertaining to the Fractal Server you wish to pull tasks from and push completed tasks to. Each manager
    supports exactly 1 Fractal Server to be in communication with, and exactly 1 user on that Fractal Server. These
    can be changed, but only once the Manager is shutdown and the settings changed. Multiple Managers however can be
    started in parallel with each other, but must be done as separate calls to the CLI.

    Caution: The password here is written in plain text, so it is up to the owner/writer of the configuration file
    to ensure its security.
    """

    fractal_uri: str = Field("localhost:7777", description="Full URI to the Fractal Server you want to connect to")
    username: Optional[str] = Field(
        None,
        description="Username to connect to the Fractal Server with. When not provided, a connection is attempted "
        "as a guest user, which in most default Servers will be unable to return results.",
    )
    password: Optional[str] = Field(
        None, description="Password to authenticate to the Fractal Server with (alongside the `username`)"
    )
    verify: Optional[bool] = Field(None, description="Use Server-side generated SSL certification or not.")

    class Config(SettingsCommonConfig):
        pass


class QueueManagerSettings(AutodocBaseSettings):
    """
    Fractal Queue Manger settings. These are options which control the setup and execution of the Fractal Manager
    itself.
    """

    manager_name: str = Field(
        "unlabeled",
        description="Name of this scheduler to present to the Fractal Server. Descriptive names help the server "
        "identify the manager resource and assists with debugging.",
    )
    queue_tag: Optional[Union[str, List[str]]] = Field(
        None,
        description="Only pull tasks from the Fractal Server with this tag. If not set (None/null), then pull untagged "
        "tasks, which should be the majority of tasks. This option should only be used when you want to "
        "pull very specific tasks which you know have been tagged as such on the server. If the server has "
        "no tasks with this tag, no tasks will be pulled (and no error is raised because this is intended "
        "behavior). If multiple tags are provided, tasks will be pulled (but not necessarily executed) in order of the "
        "tags.",
    )
    log_file_prefix: Optional[str] = Field(
        None,
        description="Full path to save a log file to, including the filename. If not provided, information will still "
        "be reported to terminal, but not saved. When set, logger information is sent both to this file "
        "and the terminal.",
    )
    update_frequency: float = Field(
        30,
        description="Time between heartbeats/update checks between this Manager and the Fractal Server. The lower this "
        "value, the shorter the intervals. If you have an unreliable network connection, consider "
        "increasing this time as repeated, consecutive network failures will cause the Manager to shut "
        "itself down to maintain integrity between it and the Fractal Server. Units of seconds",
        gt=0,
    )
    test: bool = Field(
        False,
        description="Turn on testing mode for this Manager. The Manager will not connect to any Fractal Server, and "
        "instead submits netsts worth trial tasks per quantum chemistry program it finds. These tasks are "
        "generated locally and do not need a running Fractal Server to work. Helpful for ensuring the "
        "Manager is configured correctly and the quantum chemistry codes are operating as expected.",
    )
    ntests: int = Field(
        5,
        description="Number of tests to run if the `test` flag is set to True. Total number of tests will be this "
        "number times the number of found quantum chemistry programs. Does nothing if `test` is False."
        "If set to 0, then this submits no tests, but it will run through the setup and client "
        "initialization.",
        gt=-1,
    )
    max_queued_tasks: Optional[int] = Field(
        None,
        description="Generally should not be set. Number of tasks to pull from the Fractal Server to keep locally at "
        "all times. If `None`, this is automatically computed as "
        "`ceil(common.tasks_per_worker*common.max_workers*2.0) + 1`. As tasks are completed, the "
        "local pool is filled back up to this value. These tasks will all attempt to be run concurrently, "
        "but concurrent tasks are limited by number of cluster jobs and tasks per job. Pulling too many of "
        "these can result in under-utilized managers from other sites and result in less FIFO returns. As "
        "such it is recommended not to touch this setting in general as you will be given enough tasks to "
        "fill your maximum throughput with a buffer (assuming the queue has them).",
        gt=0,
    )


class SchedulerEnum(str, Enum):
    slurm = "slurm"
    pbs = "pbs"
    sge = "sge"
    moab = "moab"
    lsf = "lsf"
    cobalt = "cobalt"


class AdaptiveCluster(str, Enum):
    static = "static"
    adaptive = "adaptive"


class ClusterSettings(AutodocBaseSettings):
    """
    Settings tied to the cluster you are running on. These settings are mostly tied to the nature of the cluster
    jobs you are submitting, separate from the nature of the compute tasks you will be running within them. As such,
    the options here are things like wall time (per job), which Scheduler your cluster has (like PBS or SLURM),
    etc. No additional options are allowed here.
    """

    node_exclusivity: bool = Field(
        False,
        description="Run your cluster jobs in node-exclusivity mode. This option may not be available to all scheduler "
        "types and thus may not do anything. Related to this, the flags we have found for this option "
        "may not be correct for your scheduler and thus might throw an error. You can always add the "
        "correct flag/parameters to the `scheduler_options` parameter and leave this as False if you "
        "find it gives you problems.",
    )
    scheduler: SchedulerEnum = Field(
        None,
        description="Option of which Scheduler/Queuing system your cluster uses. Note: not all scheduler options are "
        "available with every adapter.",
    )
    scheduler_options: List[str] = Field(
        [],
        description="Additional options which are fed into the header files for your submitted jobs to your cluster's "
        "Scheduler/Queuing system. The directives are automatically filled in, so if you want to set "
        "something like '#PBS -n something', you would instead just do '-n something'. Each directive "
        "should be a separate string entry in the list. No validation is done on this with respect to "
        "valid directives so it is on the user to know what they need to set.",
    )
    task_startup_commands: List[str] = Field(
        [],
        description="Additional commands to be run before starting the Workers and the task distribution. This can "
        "include commands needed to start things like conda environments or setting environment variables "
        "before executing the Workers. These commands are executed first before any of the distributed "
        "commands run and are added to the batch scripts as individual commands per entry, verbatim.",
    )
    walltime: str = Field(
        "06:00:00",
        description="Wall clock time of each cluster job started. Presented as a string in HH:MM:SS form, but your "
        "cluster may have a different structural syntax. This number should be set high as there should "
        "be a number of Fractal tasks which are run for each submitted cluster job. Ideally, the job "
        "will start, the Worker will land, and the Worker will crunch through as many tasks as it can; "
        "meaning the job which has a Worker in it must continue existing to minimize time spend "
        "redeploying Workers.",
    )
    adaptive: AdaptiveCluster = Field(
        AdaptiveCluster.adaptive,
        description="Whether or not to use adaptive scaling of Workers or not. If set to 'static', a fixed number of "
        "Workers will be started (and likely *NOT* restarted when the wall clock is reached). When set to "
        "'adaptive' (the default), the distributed engine will try to adaptively scale the number of "
        "Workers based on tasks in the queue. This is str instead of bool type variable in case more "
        "complex adaptivity options are added in the future.",
    )

    class Config(SettingsCommonConfig):
        pass

    @validator("scheduler", "adaptive", pre=True)
    def _lcase(cls, v):
        if v:
            v = v.lower()
        return v


class SettingsBlocker(AutodocBaseSettings):
    """Helper class to auto block certain entries, overwrite hidden methods to access"""

    _forbidden_set = set()
    _forbidden_name = "SettingsBlocker"

    def __init__(self, **kwargs):
        """
        Enforce that the keys we are going to set remain untouched. Blocks certain keywords for the classes
        they will be fed into, not whatever Fractal is using as keywords.
        """
        bad_set = set(kwargs.keys()) & self._forbidden_set
        if bad_set:
            raise KeyError(
                "The following items were set as part of {}, however, "
                "there are other config items which control these in more generic "
                "settings locations: {}".format(self._forbidden_name, bad_set)
            )
        super().__init__(**kwargs)

    class Config(SettingsCommonConfig):
        # This overwrites the base config to allow other keywords to be fed in
        extra = "allow"


class DaskQueueSettings(SettingsBlocker):
    """
    Settings for the Dask Cluster class. Values set here are passed directly into the Cluster objects based on the
    `cluster.scheduler` settings. Although many values are set automatically from other settings, there are
    some additional values such as `interface` and `extra` which are passed through to the constructor.

    Valid values for this field are functions of your cluster.scheduler and no linting is done ahead of trying to pass
    these to Dask.

    NOTE: The parameters listed here are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered. If you try to set a value
    which is derived from other options in the YAML file, an error is raised and you are told exactly which one is
    forbidden.

    Please see the docs for the provider for more information.
    """

    interface: Optional[str] = Field(
        None,
        description="Name of the network adapter to use as communication between the head node and the compute node."
        "There are oddities of this when the head node and compute node use different ethernet adapter "
        "names and we have not figured out exactly which combination is needed between this and the "
        "poorly documented `ip` keyword which appears to be for Workers, but not the Client.",
    )
    extra: Optional[List[str]] = Field(
        None,
        description="Additional flags which are fed into the Dask Worker CLI startup, can be used to overwrite "
        "pre-configured options. Do not use unless you know exactly which flags to use.",
    )
    lsf_units: Optional[str] = Field(
        None,
        description="Unit system for an LSF cluster limits (e.g. MB, GB, TB). If not set, the units are "
        "are attempted to be set from the `lsf.conf` file in the default locations. This does nothing "
        "if the cluster is not LSF",
    )
    _forbidden_set = {"name", "cores", "memory", "processes", "walltime", "env_extra", "qca_resource_string"}
    _forbidden_name = "dask_jobqueue"


class ParslExecutorSettings(SettingsBlocker):
    """
    Settings for the Parsl Executor class. This serves as the primary mechanism for distributing Workers to jobs.
    In most cases, you will not need to set any of these options, as several options are automatically inferred
    from other settings. Any option set here is passed through to the HighThroughputExecutor class of Parsl.

    https://parsl.readthedocs.io/en/latest/stubs/parsl.executors.HighThroughputExecutor.html

    NOTE: The parameters listed here are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered. If you try to set a value
    which is derived from other options in the YAML file, an error is raised and you are told exactly which one is
    forbidden.

    """

    address: Optional[str] = Field(
        None,
        description="This only needs to be set in conditional cases when the head node and compute nodes use a "
        "differently named ethernet adapter.\n\n"
        "An address to connect to the main Parsl process which is reachable from the network in which "
        "Workers will be running. This can be either a hostname as returned by hostname or an IP address. "
        "Most login nodes on clusters have several network interfaces available, only some of which can be "
        "reached from the compute nodes. Some trial and error might be necessary to identify what "
        "addresses are reachable from compute nodes.",
    )
    _forbidden_set = {"label", "provider", "cores_per_worker", "max_workers"}
    _forbidden_name = "the parsl executor"


class ParslLauncherSettings(AutodocBaseSettings):
    """
    Set the Launcher in a Parsl Provider, and its options, if not set, the defaults are used.

    This is a rare use case where the ``launcher`` key of the Provider is needed to be set. Since it must be a class
    first, you will need to specify the ``launcher_type`` options which is interpreted as the Class Name of the
    Launcher to load and pass the rest of the options set here into it. Any unset key will just be left as defaults.
    It is up to the user to consult the Parsl Docs for their desired Launcher's options and what they do.

    The known launchers below are case-insensitive,
    but if new launchers come out (or you are using a custom/developmental build of Parsl), then you can pass your
    own Launcher in verbatim, with case sensitivity, and the Queue Manager will try to load it.

    Known Launchers:
        - ``SimpleLauncher``: https://parsl.readthedocs.io/en/latest/stubs/parsl.launchers.SimpleLauncher.html
        - ``SingleNodeLauncher``: https://parsl.readthedocs.io/en/latest/stubs/parsl.launchers.SingleNodeLauncher.html
        - ``SrunLauncher``: https://parsl.readthedocs.io/en/latest/stubs/parsl.launchers.SrunLauncher.html
        - ``AprunLauncher``: https://parsl.readthedocs.io/en/latest/stubs/parsl.launchers.AprunLauncher.html
        - ``SrunMPILauncher``: https://parsl.readthedocs.io/en/latest/stubs/parsl.launchers.SrunMPILauncher.html
        - ``GnuParallelLauncher``: https://parsl.readthedocs.io/en/latest/stubs/parsl.launchers.GnuParallelLauncher.html
        - ``MpiExecLauncher`` : https://parsl.readthedocs.io/en/latest/stubs/parsl.launchers.MpiExecLauncher.html
    """

    launcher_class: str = Field(
        ...,
        description="Class of Launcher to use. This is a setting unique to QCArchive which is then used to pass onto "
        "the Provider's ``launcher`` setting and the remaining keys are passed to that Launcher's options.",
    )

    def _get_launcher(self, launcher_base: str) -> "Launcher":
        launcher_lower = launcher_base.lower()
        launcher_map = {
            "simplelauncher": "SimpleLauncher",
            "singlenodelauncher": "SingleNodeLauncher",
            "srunlauncher": "SrunLauncher",
            "aprunlauncher": "AprunLauncher",
            "srunmpiLauncher": "SrunMPILauncher",
            "gnuparallellauncher": "GnuParallelLauncher",
            "mpiexeclauncher": "MpiExecLauncher",
        }
        launcher_string = launcher_map[launcher_lower] if launcher_lower in launcher_map else launcher_base

        try:
            launcher_load = cli_utils.import_module("parsl.launchers", package=launcher_string)
            launcher = getattr(launcher_load, launcher_string)
        except ImportError:
            raise ImportError(
                f"Could not import Parsl Launcher: {launcher_base}. Please make sure you have Parsl "
                f"installed and are requesting one of the launchers within the package."
            )
        return launcher

    def build_launcher(self):
        """Import and load the desired launcher"""
        launcher = self._get_launcher(self.launcher_class)
        return launcher(**self.dict(exclude={"launcher_class"}))

    class Config(SettingsCommonConfig):
        extra = "allow"


class ParslProviderSettings(SettingsBlocker):
    """
    Settings for the Parsl Provider class. Valid values for this field depend on your choice of  cluster.scheduler and
    are defined in `the Parsl docs for the providers
    <https://parsl.readthedocs.io/en/stable/userguide/execution.html#execution-providers>`_ with some minor exceptions.
    The initializer function for the Parsl settings will indicate which

    NOTE: The parameters listed here are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered. If you try to set a value
    which is derived from other options in the YAML file, an error is raised and you are told exactly which one is
    forbidden.

    SLURM: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.SlurmProvider.html
    PBS/Torque/Moab: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.TorqueProvider.html
    SGE (Sun GridEngine): https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.GridEngineProvider.html

    """

    def __init__(self, **kwargs):
        if "max_blocks" in kwargs:
            raise ValueError("``max_blocks`` is set based on ``common.max_workers`` " "and ``common.nodes_per_job``")
        super().__init__(**kwargs)

    partition: str = Field(
        None,
        description="The name of the cluster.scheduler partition being submitted to. Behavior, valid values, and even"
        "its validity as a set variable are a function of what type of queue scheduler your specific "
        "cluster has (e.g. this variable should NOT be present for PBS clusters). "
        "Check with your Sys. Admins and/or your cluster documentation.",
    )
    launcher: ParslLauncherSettings = Field(
        None,
        description="The Parsl Launcher to use with your Provider. If left to ``None``, defaults are assumed (check "
        "the Provider's defaults), otherwise this should be a dictionary requiring the option "
        "``launcher_class`` as a str to specify which Launcher class to load, and the remaining settings "
        "will be passed on to the Launcher's constructor.",
    )
    _forbidden_set = {"worker_init", "scheduler_options", "wall_time", "nodes_per_block"}
    _forbidden_name = "parsl's provider"


class ParslQueueSettings(AutodocBaseSettings):
    """
    The Parsl-specific configurations used with the `common.adapter = parsl` setting. The parsl config is broken up into
    a top level `Config` class, an `Executor` sub-class, and a `Provider` sub-class of the `Executor`.
    Config -> Executor -> Provider. Each of these have their own options, and extra values fed into the
    ParslQueueSettings are fed to the `Config` level.

    It requires both `executor` and `provider` settings, but will default fill them in and often does not need
    any further configuration which is handled by other settings in the config file.
    """

    executor: ParslExecutorSettings = ParslExecutorSettings()
    provider: ParslProviderSettings = ParslProviderSettings()

    class Config(SettingsCommonConfig):
        extra = "allow"


class ManagerSettings(ProtoModel):
    """
    The config file for setting up a QCFractal Manager, all sub fields of this model are at equal top-level of the
    YAML file. No additional top-level fields are permitted, but sub-fields may have their own additions.

    Not all fields are required and many will depend on the cluster you are running, and the adapter you choose
    to run on.
    """

    common: CommonManagerSettings = CommonManagerSettings()
    server: FractalServerSettings = FractalServerSettings()
    manager: QueueManagerSettings = QueueManagerSettings()
    cluster: Optional[ClusterSettings] = ClusterSettings()
    dask: Optional[DaskQueueSettings] = DaskQueueSettings()
    parsl: Optional[ParslQueueSettings] = ParslQueueSettings()

    class Config(ProtoModel.Config):
        extra = "forbid"


def parse_args():
    parser = argparse.ArgumentParser(
        description="A CLI for a QCFractal QueueManager with a ProcessPoolExecutor, Dask, or Parsl backend. "
        "The Dask and Parsl backends *requires* a config file due to the complexity of its setup. If a config "
        "file is specified, the remaining options serve as CLI overwrites of the config."
    )
    parser.add_argument("--version", action="version", version=f"{qcfractal.__version__}")

    parser.add_argument("--config-file", type=str, default=None)

    # Common settings
    common = parser.add_argument_group("Common Adapter Settings")
    common.add_argument(
        "--adapter", type=str, help="The backend adapter to use, currently only {'dask', 'parsl', 'pool'} are valid."
    )
    common.add_argument(
        "--tasks-per-worker",
        type=int,
        help="The number of simultaneous tasks for the executor to run, resources will be divided evenly.",
    )
    common.add_argument("--cores-per-worker", type=int, help="The number of process for each executor's Workers")
    common.add_argument("--memory-per-worker", type=int, help="The total amount of memory on the system in GB")
    common.add_argument("--scratch-directory", type=str, help="Scratch directory location")
    common.add_argument("--retries", type=int, help="Number of RandomError retries per task before failing the task")
    common.add_argument("-v", "--verbose", action="store_true", help="Increase verbosity of the logger.")

    # FractalClient options
    server = parser.add_argument_group("FractalServer connection settings")
    server.add_argument("--fractal-uri", type=str, help="FractalServer location to pull from")
    server.add_argument("-u", "--username", type=str, help="FractalServer username")
    server.add_argument("-p", "--password", type=str, help="FractalServer password")
    server.add_argument(
        "--verify",
        type=str,
        help="Do verify the SSL certificate, leave off (unset) for servers with custom SSL certificates.",
    )

    # QueueManager options
    manager = parser.add_argument_group("QueueManager settings")
    manager.add_argument("--manager-name", type=str, help="The name of the manager to start")
    manager.add_argument("--queue-tag", type=str, help="The queue tag to pull from")
    manager.add_argument("--log-file-prefix", type=str, help="The path prefix of the logfile to write to.")
    manager.add_argument("--update-frequency", type=int, help="The frequency in seconds to check for complete tasks.")
    manager.add_argument(
        "--max-queued-tasks",
        type=int,
        help="Maximum number of tasks to hold at any given time. " "Generally should not be set.",
    )

    # Additional args
    optional = parser.add_argument_group("Optional Settings")
    optional.add_argument("--test", action="store_true", help="Boot and run a short test suite to validate setup")
    optional.add_argument(
        "--ntests", type=int, help="How many tests per found program to run, does nothing without --test set"
    )
    optional.add_argument(
        "--schema",
        action="store_true",
        help="Display the current Schema (Pydantic) for the YAML "
        "config file and exit. This will always show the "
        "most up-to-date schema. It will be presented in a "
        "JSON-like format.",
    )
    optional.add_argument(
        "--skeleton",
        "--skel",
        type=str,
        const="manager_config.yaml",
        default=None,
        action="store",
        nargs="?",
        help="Create a skeleton/example YAML config file at the specified path. This does not start "
        "the manager and instead creates a skeleton based on all the options specified.",
    )

    # Move into nested namespace
    args = vars(parser.parse_args())

    def _build_subset(args, keys):
        ret = {}
        for k in keys:
            v = args[k]

            if v is None:
                continue

            ret[k] = v
        return ret

    # Stupid we cannot inspect groups
    data = {
        "common": _build_subset(
            args,
            {
                "adapter",
                "tasks_per_worker",
                "cores_per_worker",
                "memory_per_worker",
                "scratch_directory",
                "retries",
                "verbose",
            },
        ),
        "server": _build_subset(args, {"fractal_uri", "password", "username", "verify"}),
        "manager": _build_subset(
            args,
            {"max_queued_tasks", "manager_name", "queue_tag", "log_file_prefix", "update_frequency", "test", "ntests"},
        ),
        # This set is for this script only, items here should not be passed to the ManagerSettings nor any other
        # classes
        "debug": _build_subset(args, {"schema", "skeleton"}),
    }  # yapf: disable

    if args["config_file"] is not None:
        config_data = cli_utils.read_config_file(args["config_file"])
        for name, subparser in [("common", common), ("server", server), ("manager", manager)]:
            if name not in config_data:
                continue

            data[name] = cli_utils.argparse_config_merge(subparser, data[name], config_data[name], check=False)

        for name in ["cluster", "dask", "parsl"]:
            if name in config_data:
                data[name] = config_data[name]
                if data[name] is None:
                    # Handle edge case where None provided here is explicitly treated as
                    # "do not parse" by Pydantic (intended behavior) instead of the default empty dict
                    # being used instead. This only happens when a user sets in the YAML file
                    # the top level header and nothing below it.
                    data[name] = {}

    return data


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()
    exit_callbacks = []

    try:
        if args["debug"]["schema"]:
            print(ManagerSettings.schema_json(indent=2))
            return  # We're done, exit normally
    except KeyError:
        pass  # Don't worry if schema isn't in the list
    finally:
        debug_args = args.pop("debug", {})  # Ensure the debug key is not present

    # Construct object
    settings = ManagerSettings(**args)

    # Handle Skeleton Generation
    if debug_args.get("skeleton", None):

        class IndentListDumper(yaml.Dumper):
            """
            Internal yaml Dumper to make lists indent in the output YAML

            Buried inside this since its only used in "skeleton," once, and then exits. Does not need to be imported
            anywhere else or accessed somehow

            Based on response:
            https://stackoverflow.com/questions/25108581/python-yaml-dump-bad-indentation/39681672#39681672
            """

            def increase_indent(self, flow=False, indentless=False):
                return super(IndentListDumper, self).increase_indent(flow, False)

        skel_path = os.path.expanduser(debug_args["skeleton"])
        with open(skel_path, "w") as skel:
            # cast to
            data = yaml.dump(json.loads(settings.json()), Dumper=IndentListDumper, default_flow_style=False)
            skel.write(data)
            print(
                f"Skeleton Queue Manager YAML file written to {skel_path}\n"
                f"Run: `qcfractal-manager --config-file={skel_path}` to start a manager with this configuration."
            )
            return

    logger_map = {AdapterEnum.pool: "", AdapterEnum.dask: "dask_jobqueue.core", AdapterEnum.parsl: "parsl"}
    if settings.common.verbose:
        adapter_logger = logging.getLogger(logger_map[settings.common.adapter])
        adapter_logger.setLevel("DEBUG")
        logger.setLevel("DEBUG")

    if settings.manager.log_file_prefix is not None:
        tornado.options.options["log_file_prefix"] = settings.manager.log_file_prefix
        # Clones the log to the output
        tornado.options.options["log_to_stderr"] = True
    tornado.log.enable_pretty_logging()

    if settings.manager.test:
        # Test this manager, no client needed
        client = None
    else:
        # Connect to a specified fractal server
        client = qcfractal.interface.FractalClient(
            address=settings.server.fractal_uri, **settings.server.dict(skip_defaults=True, exclude={"fractal_uri"})
        )

    # Figure out per-task data
    node_parallel_tasks = settings.common.nodes_per_task > 1  # Whether tasks are node-parallel
    if node_parallel_tasks:
        supported_adapters = ["parsl"]
        if settings.common.adapter not in supported_adapters:
            raise ValueError("Node-parallel jobs are only supported with {} adapters".format(supported_adapters))
        # Node-parallel tasks use all cores on a worker
        cores_per_task = settings.common.cores_per_worker
        memory_per_task = settings.common.memory_per_worker
        if settings.common.tasks_per_worker > 1:
            raise ValueError(">1 task per node and >1 node per tasks are mutually-exclusive")
    else:
        cores_per_task = settings.common.cores_per_worker // settings.common.tasks_per_worker
        memory_per_task = settings.common.memory_per_worker / settings.common.tasks_per_worker
    if cores_per_task < 1:
        raise ValueError("Cores per task must be larger than one!")

    if settings.common.adapter == "pool":
        from concurrent.futures import ProcessPoolExecutor

        # TODO: Replace with passing via mp_context to ProcessPoolExecutor
        # when python 3.6 is dead and buried
        from multiprocessing import set_start_method

        set_start_method("spawn")

        # Error if the number of nodes per jobs is more than 1
        if settings.common.nodes_per_job > 1:
            raise ValueError("Pool adapters only run on a single local node")
        queue_client = ProcessPoolExecutor(max_workers=settings.common.tasks_per_worker)

    elif settings.common.adapter == "dask":

        dask_settings = settings.dask.dict(skip_defaults=True)
        # Checks
        if "extra" not in dask_settings:
            dask_settings["extra"] = []
        if QCA_RESOURCE_STRING not in dask_settings["extra"]:
            dask_settings["extra"].append(QCA_RESOURCE_STRING)
        # Scheduler opts
        scheduler_opts = settings.cluster.scheduler_options.copy()

        # Error if the number of nodes per jobs is more than 1
        if settings.common.nodes_per_job > 1:
            raise NotImplementedError("Support for >1 node per job is not yet supported by QCFractal + Dask")
            # TODO (wardlt): Implement multinode jobs in Dask

        _cluster_loaders = {
            "slurm": "SLURMCluster",
            "pbs": "PBSCluster",
            "moab": "MoabCluster",
            "sge": "SGECluster",
            "lsf": "LSFCluster",
        }
        dask_exclusivity_map = {
            "slurm": "--exclusive",
            "pbs": "-n",
            "moab": "-n",  # Less sure about this one
            "sge": "-l exclusive=true",
            "lsf": "-x",
        }
        if settings.cluster.node_exclusivity and dask_exclusivity_map[settings.cluster.scheduler] not in scheduler_opts:
            scheduler_opts.append(dask_exclusivity_map[settings.cluster.scheduler])

        # Create one construct to quickly merge dicts with a final check
        dask_construct = {
            "name": "QCFractal_Dask_Compute_Executor",
            "cores": settings.common.cores_per_worker,
            "memory": str(settings.common.memory_per_worker) + "GB",
            "processes": settings.common.tasks_per_worker,  # Number of workers to generate == tasks in this construct
            "walltime": settings.cluster.walltime,
            "job_extra": scheduler_opts,
            "env_extra": settings.cluster.task_startup_commands,
            **dask_settings,
        }

        try:
            # Import the dask things we need
            import dask_jobqueue
            from dask.distributed import Client

            cluster_module = cli_utils.import_module(
                "dask_jobqueue", package=_cluster_loaders[settings.cluster.scheduler]
            )
            cluster_class = getattr(cluster_module, _cluster_loaders[settings.cluster.scheduler])
            if dask_jobqueue.__version__ < "0.5.0":
                raise ImportError
        except ImportError:
            raise ImportError("You need`dask-jobqueue >= 0.5.0` to use the `dask` adapter")

        cluster = cluster_class(**dask_construct)

        # Setup up adaption
        # Workers are distributed down to the cores through the sub-divided processes
        # Optimization may be needed
        workers = settings.common.tasks_per_worker * settings.common.max_workers
        if settings.cluster.adaptive == AdaptiveCluster.adaptive:
            cluster.adapt(minimum=0, maximum=workers, interval="10s")
        else:
            cluster.scale(workers)

        queue_client = Client(cluster)

    elif settings.common.adapter == "parsl":

        scheduler_opts = settings.cluster.scheduler_options

        if not settings.cluster.node_exclusivity:
            raise ValueError(
                "For now, QCFractal can only be run with Parsl in node exclusivity. This will be relaxed "
                "in a future release of Parsl and QCFractal"
            )

        # Import helpers
        _provider_loaders = {
            "slurm": "SlurmProvider",
            "pbs": "TorqueProvider",
            "moab": "TorqueProvider",
            "sge": "GridEngineProvider",
            "cobalt": "CobaltProvider",
            "lsf": None,
        }

        if _provider_loaders[settings.cluster.scheduler] is None:
            raise ValueError(f"Parsl does not know how to handle cluster of type {settings.cluster.scheduler}.")

        # Headers
        _provider_headers = {
            "slurm": "#SBATCH",
            "pbs": "#PBS",
            "moab": "#PBS",
            "sge": "#$$",
            "lsf": None,
            "cobalt": "#COBALT",
        }

        # Import the parsl things we need
        try:
            import parsl
            from parsl.config import Config
            from parsl.executors import HighThroughputExecutor
            from parsl.addresses import address_by_hostname

            provider_module = cli_utils.import_module(
                "parsl.providers", package=_provider_loaders[settings.cluster.scheduler]
            )
            provider_class = getattr(provider_module, _provider_loaders[settings.cluster.scheduler])
            provider_header = _provider_headers[settings.cluster.scheduler]
            if parsl.__version__ < "0.9.0":
                raise ImportError
        except ImportError:
            raise ImportError("You need `parsl >=0.9.0` to use the `parsl` adapter")

        if _provider_loaders[settings.cluster.scheduler] == "moab":
            logger.warning(
                "Parsl uses its TorqueProvider for Moab clusters due to the scheduler similarities. "
                "However, if you find a bug with it, please report to the Parsl and QCFractal developers so "
                "it can be fixed on each respective end."
            )

        # Setup the providers

        # Determine the maximum number of blocks
        # TODO (wardlt): Math assumes that user does not set aside a compute node for the adapter
        max_nodes = settings.common.max_workers * settings.common.nodes_per_task
        if settings.common.nodes_per_job > max_nodes:
            raise ValueError("Number of nodes per job is more than the maximum number of nodes used by manager")
        if max_nodes % settings.common.nodes_per_job != 0:
            raise ValueError(
                "Maximum number of nodes (maximum number of workers times nodes per task) "
                "needs to be a multiple of the number of nodes per job"
            )
        if settings.common.nodes_per_job % settings.common.nodes_per_task != 0:
            raise ValueError("Number of nodes per job needs to be a multiple of the number of nodes per task")
        max_blocks = max_nodes // settings.common.nodes_per_job

        # Create one construct to quickly merge dicts with a final check
        common_parsl_provider_construct = {
            "init_blocks": 0,  # Update this at a later time of Parsl
            "max_blocks": max_blocks,
            "walltime": settings.cluster.walltime,
            "scheduler_options": f"{provider_header} " + f"\n{provider_header} ".join(scheduler_opts) + "\n",
            "nodes_per_block": settings.common.nodes_per_job,
            "worker_init": "\n".join(settings.cluster.task_startup_commands),
            **settings.parsl.provider.dict(skip_defaults=True, exclude={"partition", "launcher"}),
        }
        if settings.cluster.scheduler.lower() == "slurm" and "cores_per_node" not in common_parsl_provider_construct:
            common_parsl_provider_construct["cores_per_node"] = settings.common.cores_per_worker
        # TODO: uncomment after Parsl#1416 is resolved
        # if settings.cluster.scheduler.lower() == "slurm" and "mem_per_node" not in common_parsl_provider_construct:
        #    common_parsl_provider_construct["mem_per_node"] = settings.common.memory_per_worker

        if settings.parsl.provider.launcher:
            common_parsl_provider_construct["launcher"] = settings.parsl.provider.launcher.build_launcher()
        if settings.cluster.scheduler == "slurm":
            # The Parsl SLURM constructor has a strange set of arguments
            provider = provider_class(
                settings.parsl.provider.partition,
                exclusive=settings.cluster.node_exclusivity,
                **common_parsl_provider_construct,
            )
        else:
            provider = provider_class(**common_parsl_provider_construct)

        # The executor for Parsl is different for node parallel tasks and shared-memory tasks
        if node_parallel_tasks:
            # Tasks are launched from a single worker on the login node
            # TODO (wardlt): Remove assumption that there is only one Parsl worker running all tasks
            tasks_per_job = settings.common.nodes_per_job // settings.common.nodes_per_task
            logger.info(f"Preparing a HTEx to use node-parallel tasks with {tasks_per_job} workers")
            parsl_executor_construct = {
                "label": "QCFractal_Parsl_{}_Executor".format(settings.cluster.scheduler.title()),
                # Parsl will create one worker process per MPI task. Normally, Parsl prevents having
                #  more processes than cores. However, as each worker will spend most of its time
                #  waiting for the MPI task to complete, we can safely oversubscribe (e.g., more worker
                #  processes than cores), which requires setting "cores_per_worker" to <1
                "cores_per_worker": 1e-6,
                "max_workers": tasks_per_job,
                "provider": provider,
                "address": address_by_hostname(),
                **settings.parsl.executor.dict(skip_defaults=True),
            }
        else:

            parsl_executor_construct = {
                "label": "QCFractal_Parsl_{}_Executor".format(settings.cluster.scheduler.title()),
                "cores_per_worker": cores_per_task,
                "max_workers": settings.common.tasks_per_worker,
                "provider": provider,
                "address": address_by_hostname(),
                **settings.parsl.executor.dict(skip_defaults=True),
            }

        queue_client = Config(
            retries=settings.common.retries, executors=[HighThroughputExecutor(**parsl_executor_construct)]
        )

    else:
        raise KeyError(
            "Unknown adapter type '{}', available options: {}.\n"
            "This code should also be unreachable with pydantic Validation, so if "
            "you see this message, please report it to the QCFractal GitHub".format(
                settings.common.adapter, [getattr(AdapterEnum, v).value for v in AdapterEnum]
            )
        )

    # Build out the manager itself
    # Compute max tasks
    max_concurrent_tasks = settings.common.tasks_per_worker * settings.common.max_workers
    if settings.manager.max_queued_tasks is None:
        # Tasks * jobs * buffer + 1
        max_queued_tasks = ceil(max_concurrent_tasks * 2.00) + 1
    else:
        max_queued_tasks = settings.manager.max_queued_tasks

    # The queue manager is configured differently for node-parallel and single-node tasks
    manager = qcfractal.queue.QueueManager(
        client,
        queue_client,
        max_tasks=max_queued_tasks,
        queue_tag=settings.manager.queue_tag,
        manager_name=settings.manager.manager_name,
        update_frequency=settings.manager.update_frequency,
        cores_per_task=cores_per_task,
        memory_per_task=memory_per_task,
        nodes_per_task=settings.common.nodes_per_task,
        scratch_directory=settings.common.scratch_directory,
        retries=settings.common.retries,
        verbose=settings.common.verbose,
        cores_per_rank=settings.common.cores_per_rank,
        configuration=settings,
    )

    # Set stats correctly since we buffer the max tasks a bit
    manager.statistics.max_concurrent_tasks = max_concurrent_tasks

    # Add exit callbacks
    for cb in exit_callbacks:
        manager.add_exit_callback(cb[0], *cb[1], **cb[2])

    # Either startup the manager or run until complete
    if settings.manager.test:
        success = manager.test(settings.manager.ntests)
        if success is False:
            raise ValueError("Testing was not successful, failing.")
    else:

        for signame in {"SIGHUP", "SIGINT", "SIGTERM"}:

            def stop(*args, **kwargs):
                manager.stop(signame)
                raise KeyboardInterrupt()

            signal.signal(getattr(signal, signame), stop)

        # Blocks until signal
        try:
            manager.start()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
