"""
A command line interface to the qcfractal server.
"""

import argparse
import inspect
import signal
import logging
import os
from enum import Enum
from functools import partial
from math import ceil
from typing import List, Optional

import tornado.log

import qcengine as qcng
import qcfractal
from pydantic import BaseModel, BaseSettings, validator, Schema

from . import cli_utils

__all__ = ["main"]

QCA_RESOURCE_STRING = '--resources process=1'

logger = logging.getLogger("qcfractal.cli")


class SettingsCommonConfig:
    env_prefix = "QCA_"
    case_insensitive = True
    extra = "forbid"


class AdapterEnum(str, Enum):
    dask = "dask"
    pool = "pool"
    parsl = "parsl"


class CommonManagerSettings(BaseSettings):
    """
    The Common settings are the most settings most users will need to adjust regularly and control the nature of
    task execution and the hardware under which tasks are executed on. This block is often unique to each deployment,
    user, and manager and will be the most commonly updated options, even as config files are copied and reused, and
    even on the same platform/cluster.

    Parameters
    ----------
    adapter : str, Default: "pool"
        Which type of Distrusted adapter to run tasks through. Current options: "dask", "pool", and "parsl"
    ntasks : int, Default: 1
        Number of concurrent tasks to run *per cluster job* which is executed. Total number of concurrent
        tasks is this value times cluster.max_cluster_jobs, assuming the hardware is available. With the
        pool adapter, and/or if cluster.max_cluster_jobs is 1, this is the number of concurrent jobs.
    ncores : int, Default: Current processors
        Number of cores to be consumed and distributed over the ntasks. These tasks are divided evenly,
        so it is recommended that ncores/ntasks be a whole number else the core distribution is left
        up to the logic of the adapter.
    memory : float, Default: Current Memory
        Amount of memory (in GB) to be consumed and distributed over the ntasks. This memory is divided
        evenly, but is ultimately at the control of the adapter. Engine will only allow each of its
        calls to consume memory/ntasks of memory. Total memory consumed by this manager at any one
        time is this value times cluster.max_cluster_jobs.
    scratch_directory : str, Optional
        Scratch directory for Engine execution jobs.
    verbose : bool, Default: False
        Turn on verbose mode or not. In verbose mode, all messages from DEBUG level and up are shown,
        otherwise, defaults are all used for any logger.
    """
    adapter: AdapterEnum = Schema(
        AdapterEnum.pool,
        description="Which type of Distrusted adapter to run tasks through."
    )
    ntasks: int = Schema(
        1,
        description="Number of concurrent tasks to run *per cluster job* which is executed. Total number of concurrent "
                    "tasks is this value times cluster.max_cluster_jobs, assuming the hardware is available. With the "
                    "pool adapter, and/or if cluster.max_cluster_jobs is 1, this is the number of concurrent jobs."
    )
    ncores: int = Schema(
        qcng.config.get_global("ncores"),
        description="Number of cores to be consumed and distributed over the ntasks. These tasks are divided evenly, "
                    "so it is recommended that ncores/ntasks be a whole number else the core distribution is left "
                    "up to the logic of the adapter."
    )
    memory: float = Schema(
        qcng.config.get_global("memory"),
        description="Amount of memory (in GB) to be consumed and distributed over the ntasks. This memory is divided "
                    "evenly, but is ultimately at the control of the adapter. Engine will only allow each of its "
                    "calls to consume memory/ntasks of memory. Total memory consumed by this manager at any one "
                    "time is this value times cluster.max_cluster_jobs.",
        gt=0
    )
    scratch_directory: Optional[str] = Schema(
        None,
        description="Scratch directory for Engine execution jobs."
    )
    verbose: bool = Schema(
        False,
        description="Turn on verbose mode or not. In verbose mode, all messages from DEBUG level and up are shown, "
                    "otherwise, defaults are all used for any logger."
    )

    class Config(SettingsCommonConfig):
        pass


class FractalServerSettings(BaseSettings):
    """
    Settings pertaining to the Fractal Server you wish to pull tasks from and push completed tasks to. Each manager
    supports exactly 1 Fractal Server to be in communication with, and exactly 1 user on that Fractal Server. These
    can be changed, but only once the Manager is shutdown and the settings changed.

    Caution: The password here is written in plain text, so it is up to the owner/writer of the configuration file
    to ensure its security.

    Parameters
    ----------
    fractal_uri : str, Default: localhost:7777
        Full URI to the Fractal Server you want to connect to
    username : str, Optional
        Username to connect to the Fractal Server with. When not provided, a connection is attempted
        as a guest user, which in most default Servers will be unable to return results.
    password : str, Optional
        Password to authenticate to the Fractal Server with (alongside the `username`)
    verify : bool, Optional
        Use Server-side generated SSL certification or not.
    """
    fractal_uri: str = Schema(
        "localhost:7777",
        description="Full URI to the Fractal Server you want to connect to"
    )
    username: Optional[str] = Schema(
        None,
        description="Username to connect to the Fractal Server with. When not provided, a connection is attempted "
                    "as a guest user, which in most default Servers will be unable to return results."
    )
    password: Optional[str] = Schema(
        None,
        description="Password to authenticate to the Fractal Server with (alongside the `username`)"
    )
    verify: Optional[bool] = Schema(
        None,
        description="Use Server-side generated SSL certification or not."
    )

    class Config(SettingsCommonConfig):
        pass


class QueueManagerSettings(BaseSettings):
    """
    Fractal Queue Manger settings. These are options which control the setup and execution of the Fractal Manager
    itself.

    Parameters
    ----------
    max_tasks : int, Default: 50
        Number of tasks to pull from the Fractal Server to keep locally at all times.
        As tasks are completed, the local pool is filled back up to this value. These tasks will all
        attempt to be run at once, but parallel tasks are limited bu number of cluster jobs and ntasks.
    manager_name : str, Default: "unlabeled"
        Name of this scheduler to present to the Fractal Server. Descriptive names help the server
        identify you and debugging purposes.
    queue_tag : str, Optional
        Only pull jobs from the Fractal Server with this tag. If not set (None/null), then pull untagged
        jobs, which should be the majority of jobs. This option should only be used when you want to
        pull very specific jobs which you know have been tagged as such on the server.
    log_file_prefix : str, Optional
        Full path to save a log file to, including the filename. If not provided, information will still
        be reported to terminal, but not saved.
    update_frequency : float, Default: 30
        Time between heartbeats/update checks between this Manager and the Fractal Server. The lower this
        value, the shorter the intervals. If you have an unreliable network connection, consider
        increasing this time as repeated, consecutive network failures will cause the Manager to shut
        itself down to maintain integrity between it and the Fractal Server. Units of seconds
    test : bool, Default: False
        Turn on testing mode for this Manager. The Manager will not connect to any Fractal Server, and
        instead submit netsts worth trial jobs per quantum chemistry program it finds. These jobs are
        generated locally and do not need a running Fractal server to work. Helpful for ensuring the
        Manager is configured correctly and the quantum chemistry codes are operating as expected.
    ntests : int, Default: 5
        Number of tests to run if the `test` flag is set to True. Total number of tests will be this
        number times the number of found quantum chemistry programs.

    """
    max_tasks: int = Schema(
        50,
        description="Number of tasks to pull from the Fractal Server to keep locally at all times. "
                    "As tasks are completed, the local pool is filled back up to this value. These tasks will all "
                    "attempt to be run at once, but parallel tasks are limited bu number of cluster jobs and ntasks.",
        gt=0
    )
    manager_name: str = Schema(
        "unlabeled",
        description="Name of this scheduler to present to the Fractal Server. Descriptive names help the server "
                    "identify you and debugging purposes."
    )
    queue_tag: Optional[str] = Schema(
        None,
        description="Only pull jobs from the Fractal Server with this tag. If not set (None/null), then pull untagged "
                    "jobs, which should be the majority of jobs. This option should only be used when you want to "
                    "pull very specific jobs which you know have been tagged as such on the server."
    )
    log_file_prefix: Optional[str] = Schema(
        None,
        description="Full path to save a log file to, including the filename. If not provided, information will still "
                    "be reported to terminal, but not saved."
    )
    update_frequency: float = Schema(
        30,
        description="Time between heartbeats/update checks between this Manager and the Fractal Server. The lower this "
                    "value, the shorter the intervals. If you have an unreliable network connection, consider "
                    "increasing this time as repeated, consecutive network failures will cause the Manager to shut "
                    "itself down to maintain integrity between it and the Fractal Server. Units of seconds",
        gt=0
    )
    test: bool = Schema(
        False,
        description="Turn on testing mode for this Manager. The Manager will not connect to any Fractal Server, and "
                    "instead submit netsts worth trial jobs per quantum chemistry program it finds. These jobs are "
                    "generated locally and do not need a running Fractal server to work. Helpful for ensuring the "
                    "Manager is configured correctly and the quantum chemistry codes are operating as expected."
    )
    ntests: int = Schema(
        5,
        description="Number of tests to run if the `test` flag is set to True. Total number of tests will be this "
                    "number times the number of found quantum chemistry programs.",
        gt=0
    )


class SchedulerEnum(str, Enum):
    slurm = "slurm"
    pbs = "pbs"
    sge = "sge"
    moab = "moab"
    lsf = "lsf"


class AdaptiveCluster(str, Enum):
    static = "static"
    adaptive = "adaptive"


class ClusterSettings(BaseSettings):
    """
    Settings tied to the cluster you are running on. These settings are mostly tied to the nature of the cluster
    jobs you are submitting, separate from the nature of the compute jobs you will be running within them. As such,
    the options here are things like wall time (per job), which Scheduler your cluster has (like PBS or SLURM),
    etc. No additional options are allowed here.

    Parameters
    ----------
    max_cluster_jobs : int, Default: 1
        The maximum number of cluster jobs which are allowed to be run at the same time. The total number
        of workers which can be started (and thus simultaneous Fractal tasks which can be run) is equal
        to this parameter times common.ntasks.
        In exclusive mode this is equivalent to the maximum number of nodes which you will consume.
        This must be a positive, non zero integer.
    node_exclusivity : bool, Defualt: False
        Run your cluster jobs in node-exclusivity mode. This option may not be available to all scheduler types and thus
        may not do anything.
    scheduler : str, Optional
        Option of which Scheduler/Queuing system your cluster uses. Valid options are: slurm, pbs, sge, moab, and lsf.
        Note: not all scheduler options are available with every adapter.
    scheduler_options : list, Default: []
        Additional options which are fed into the header files for your submitted jobs to your cluster's
        Scheduler/Queuing system. The directives are automatically filled in, so if you want to set
        something like '#PBS -n something', you would instead just do '-n something'. Each directive
        should be a separate string entry in the list. No validation is done on this w.r.t. valid
        directives so it is on the user to know what they need to set.
    task_startup_commands : list, Default: []
        Additional commands to be run before starting the workers and the task distribution. This can
        include commands needed to start things like conda environments or setting environment variables
        before executing the workers. These commands are executed first before any of the distributed
        commands run and are added to the batch scripts as individual commands per entry, verbatim.
    walltime : str, Default: "06:00:00"
        Wall clock time of each cluster job started. Presented as a string in HH:MM:SS form, but your
        cluster may have a different structural syntax. This number should be set high as there should
        be a number of Fractal jobs which are run for each submitted cluster job.
    adaptive : str, Default: "adaptive"
        Whether or not to use adaptive scaling of workers or not. If set to 'static', a fixed number of
        workers will be started (and likely *NOT* restarted when the wall clock is reached). When set to
        'adaptive' (the default), the distributed engine will try to adaptively scale the number of
        workers based on jobs in the queue. This is str instead of bool type variable in case more
        complex adaptivity options are added in the future. Valid options: "adaptive" and "static"
    """
    max_cluster_jobs: int = Schema(
        1,
        description="The maximum number of cluster jobs which are allowed to be run at the same time. The total number "
                    "of workers which can be started (and thus simultaneous Fractal tasks which can be run) is equal "
                    "to this parameter times common.ntasks."
                    "In exclusive mode this is equivalent to the maximum number of nodes which you will consume. "
                    "This must be a positive, non zero integer.",
        gt=0
    )
    node_exclusivity: bool = Schema(
        False,
        description="Run your cluster jobs in node-exclusivity mode. This option may not be available to all scheduler "
                    "types and thus may not do anything."
    )
    scheduler: SchedulerEnum = Schema(
        None,
        description="Option of which Scheduler/Queuing system your cluster uses. Note: not all scheduler options are "
                    "available with every adapter."
    )
    scheduler_options: List[str] = Schema(
        [],
        description="Additional options which are fed into the header files for your submitted jobs to your cluster's "
                    "Scheduler/Queuing system. The directives are automatically filled in, so if you want to set "
                    "something like '#PBS -n something', you would instead just do '-n something'. Each directive "
                    "should be a separate string entry in the list. No validation is done on this w.r.t. valid "
                    "directives so it is on the user to know what they need to set."
    )
    task_startup_commands: List[str] = Schema(
        [],
        description="Additional commands to be run before starting the workers and the task distribution. This can "
                    "include commands needed to start things like conda environments or setting environment variables "
                    "before executing the workers. These commands are executed first before any of the distributed "
                    "commands run and are added to the batch scripts as individual commands per entry, verbatim."
    )
    walltime: str = Schema(
        "06:00:00",
        description="Wall clock time of each cluster job started. Presented as a string in HH:MM:SS form, but your "
                    "cluster may have a different structural syntax. This number should be set high as there should "
                    "be a number of Fractal jobs which are run for each submitted cluster job."
    )
    adaptive: AdaptiveCluster = Schema(
        AdaptiveCluster.adaptive,
        description="Whether or not to use adaptive scaling of workers or not. If set to 'static', a fixed number of "
                    "workers will be started (and likely *NOT* restarted when the wall clock is reached). When set to "
                    "'adaptive' (the default), the distributed engine will try to adaptively scale the number of "
                    "workers based on jobs in the queue. This is str instead of bool type variable in case more "
                    "complex adaptivity options are added in the future."
    )

    class Config(SettingsCommonConfig):
        pass

    @validator('scheduler', 'adaptive', pre=True)
    def things_to_lcase(cls, v):
        return v.lower()


class SettingsBlocker(BaseSettings):
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
            raise KeyError("The following items were set as part of {}, however, "
                           "there are other config items which control these in more generic "
                           "settings locations: {}".format(self._forbidden_name, bad_set))
        super().__init__(**kwargs)

    class Config(SettingsCommonConfig):
        # This overwrites the base config to allow other keywords to be fed in
        extra = "allow"


class DaskQueueSettings(SettingsBlocker):
    """
    Settings for the Dask Cluster class. Values set here are passed directly into the Cluster objects based on the
    `cluster.scheduler` settings. Although many values are set automatically from other settings, there are
    some additional values such as `interface` and `extra` which are passed through to the constructor.

    Valid values for this field are function of your cluster.scheduler and no linting is done ahead of trying to pass
    these to Dask.

    NOTE: The parameters listed here have are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered

    Please see the docs for the provider for more information.

    Parameters
    ----------
    interface : str, Optional
        Name of the network adapter to use as communication between the head node and the compute node.
        There are oddities of this when the head node and compute node use different ethernet adapter
        names and we have not figured out exactly which combination is needed between this and the
        poorly documented `ip` keyword which appears to be for workers, but not the Client.
    extra : list[str], Optional
        Additional flags which are fed into the Dask Worker CLI startup, can be used to overwrite
        pre-configured options. Do not use unless you know exactly which flags to use.
    lsf_units : str, Optional
        Unit system for an LSF cluster limits (e.g. MB, GB, TB). If not set, the units are
        are attempted to be set from the `lsf.conf` file in the default locations. This does nothing
        if the cluster is not LSF.

    """
    interface: Optional[str] = Schema(
        None,
        description="Name of the network adapter to use as communication between the head node and the compute node."
                    "There are oddities of this when the head node and compute node use different ethernet adapter "
                    "names and we have not figured out exactly which combination is needed between this and the "
                    "poorly documented `ip` keyword which appears to be for workers, but not the Client."
    )
    extra: Optional[List[str]] = Schema(
        None,
        description="Additional flags which are fed into the Dask Worker CLI startup, can be used to overwrite "
                    "pre-configured options. Do not use unless you know exactly which flags to use."
    )
    lsf_units: Optional[str] = Schema(
        None,
        description="Unit system for an LSF cluster limits (e.g. MB, GB, TB). If not set, the units are "
                    "are attempted to be set from the `lsf.conf` file in the default locations. This does nothing "
                    "if the cluster is not LSF"
    )
    _forbidden_set = {"name", "cores", "memory", "processes", "walltime", "env_extra", "qca_resource_string"}
    _forbidden_name = "dask_jobqueue"


class ParslExecutorSettings(SettingsBlocker):
    """
    Settings for the Parsl Executor class. This serves as the primary mechanism for distributing jobs.
    In most cases, you will not need to set any of these options, as several options are automatically inferred
    from other settings. Any option set here is passed through to the HighThroughputExecutor class of Parsl.

    https://parsl.readthedocs.io/en/latest/stubs/parsl.executors.HighThroughputExecutor.html

    NOTE: The parameters listed here have are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered

    Parameters
    ----------
    address : str, Optional
        This only needs to be set in conditional cases when the head node and compute nodes use a
        differently named ethernet adapter.

        An address to connect to the main Parsl process which is reachable from the network in which
        workers will be running. This can be either a hostname as returned by hostname or an IP address.
        Most login nodes on clusters have several network interfaces available, only some of which can be
        reached from the compute nodes. Some trial and error might be necessary to identify what
        addresses are reachable from compute nodes.
    """
    address: Optional[str] = Schema(
        None,
        description="This only needs to be set in conditional cases when the head node and compute nodes use a "
                    "differently named ethernet adapter.\n"
                    "An address to connect to the main Parsl process which is reachable from the network in which "
                    "workers will be running. This can be either a hostname as returned by hostname or an IP address. "
                    "Most login nodes on clusters have several network interfaces available, only some of which can be "
                    "reached from the compute nodes. Some trial and error might be necessary to identify what "
                    "addresses are reachable from compute nodes."
    )
    _forbidden_set = {"label", "provider", "cores_per_worker", "max_workers"}
    _forbidden_name = "the parsl executor"


class ParslProviderSettings(SettingsBlocker):
    """
    Settings for the Parsl Provider class. Valid values for this field are function of your cluster.scheduler and no
    linting is done ahead of trying to pass these to Parsl.
    Please see the docs for the provider information

    NOTE: The parameters listed here have are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered

    SLURM: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.SlurmProvider.html
    PBS/Torque/Moba: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.TorqueProvider.html
    SGE (Sun GridEngine): https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.GridEngineProvider.html

    Parameters
    ----------
    partition : str, Optional
        The name of the cluster.scheduler partition being submitted to. Behavior, valid values, and even
        its validity as a variable are a function of what type of queue scheduler your specific cluster
        has (e.g. this variable should NOT be present for PBS clusters).
        Check with your Sys. Admins and/or your cluster documentation.
    """
    partition: str = Schema(
        None,
        description="The name of the cluster.scheduler partition being submitted to. Behavior, valid values, and even"
                    "its validity as a variable are a function of what type of queue scheduler your specific cluster "
                    "has (e.g. this variable should NOT be present for PBS clusters). "
                    "Check with your Sys. Admins and/or your cluster documentation."
    )
    _forbidden_set = {"nodes_per_block", "max_blocks", "worker_init", "scheduler_options", "wall_time"}
    _forbidden_name = "parsl's provider"


class ParslQueueSettings(BaseSettings):
    """
    The Parsl-specific configurations used with the `common.adapter = parsl` setting. The parsl config is broken up into
    a top level `Config` class, an `Executor` sub-class, and a `Provider` sub-class of the `Executor`.
    Config -> Executor -> Provider. Each of these have their own options, and extra values fed into the
    ParslQueueSettings are fed to the `Config` level.

    It requires both `executor` and `provider` settings, but will default fill them in and often does not need
    any further configuration which is handled by other settings in the config file.

    Parameters
    ----------
    executor : :class:`ParslExecutorSettings`
    provider : :class:`ParslProviderSettings`
    """
    executor: ParslExecutorSettings = ParslExecutorSettings()
    provider: ParslProviderSettings = ParslProviderSettings()

    class Config(SettingsCommonConfig):
        pass


class ManagerSettings(BaseModel):
    """
    The config file for setting up a QCFractal Manager, all sub fields of this model are at equal top-level of the
    YAML file. No additional top-level fields are permitted, but sub-fields may have their own additions.

    Not all fields are required and many will depend on the cluster you are running, and the adapter you choose
    to run on

    Parameters
    ----------
    common : :class:`CommonManagerSettings`
    server : :class:`FractalServerSettings`
    manager : :class:`QueueManagerSettings`
    cluster : :class:`ClusterSettings`, Optional
    dask : :class:`DaskQueueSettings`, Optional
    parsl : :class:`ParslQueueSettings`, Optional
    """
    common: CommonManagerSettings = CommonManagerSettings()
    server: FractalServerSettings = FractalServerSettings()
    manager: QueueManagerSettings = QueueManagerSettings()
    cluster: Optional[ClusterSettings] = None
    dask: Optional[DaskQueueSettings] = None
    parsl: Optional[ParslQueueSettings] = None

    class Config:
        extra = "forbid"


def parse_args():
    parser = argparse.ArgumentParser(
        description='A CLI for a QCFractal QueueManager with a ProcessPoolExecutor, Dask, or Parsl backend. '
        'The Dask and Parsl backends *requires* a config file due to the complexity of its setup. If a config '
        'file is specified, the remaining options serve as CLI overwrites of the config.')

    parser.add_argument("--config-file", type=str, default=None)

    # Common settings
    common = parser.add_argument_group('Common Adapter Settings')
    common.add_argument(
        "--adapter", type=str, help="The backend adapter to use, currently only {'dask', 'parsl', 'pool'} are valid.")
    common.add_argument(
        "--ntasks",
        type=int,
        help="The number of simultaneous tasks for the executor to run, resources will be divided evenly.")
    common.add_argument("--ncores", type=int, help="The number of process for the executor")
    common.add_argument("--memory", type=int, help="The total amount of memory on the system in GB")
    common.add_argument("--scratch-directory", type=str, help="Scratch directory location")
    common.add_argument("-v", "--verbose", action="store_true", help="Increase verbosity of the logger.")

    # FractalClient options
    server = parser.add_argument_group('FractalServer connection settings')
    server.add_argument("--fractal-uri", type=str, help="FractalServer location to pull from")
    server.add_argument("-u", "--username", type=str, help="FractalServer username")
    server.add_argument("-p", "--password", type=str, help="FractalServer password")
    server.add_argument(
        "--verify",
        type=str,
        help="Do verify the SSL certificate, turn off for servers with custom SSL certificiates.")

    # QueueManager options
    manager = parser.add_argument_group("QueueManager settings")
    manager.add_argument("--max-tasks", type=int, help="Maximum number of tasks to hold at any given time.")
    manager.add_argument("--manager-name", type=str, help="The name of the manager to start")
    manager.add_argument("--queue-tag", type=str, help="The queue tag to pull from")
    manager.add_argument("--log-file-prefix", type=str, help="The path prefix of the logfile to write to.")
    manager.add_argument("--update-frequency", type=int, help="The frequency in seconds to check for complete tasks.")

    # Additional args
    optional = parser.add_argument_group('Optional Settings')
    optional.add_argument("--test", action="store_true", help="Boot and run a short test suite to validate setup")
    optional.add_argument(
        "--ntests", type=int, help="How many tests per found program to run, does nothing without --test set")

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
        "common": _build_subset(args, {"adapter", "ntasks", "ncores", "memory", "scratch_directory", "verbose"}),
        "server": _build_subset(args, {"fractal_uri", "password", "username", "verify"}),
        "manager": _build_subset(args, {"max_tasks", "manager_name", "queue_tag", "log_file_prefix", "update_frequency",
                                        "test", "ntests"}),
    } # yapf: disable

    if args["config_file"] is not None:
        config_data = cli_utils.read_config_file(args["config_file"])
        for name, subparser in [("common", common), ("server", server), ("manager", manager)]:
            if name not in config_data:
                continue

            data[name] = cli_utils.argparse_config_merge(subparser, data[name], config_data[name], check=False)

        for name in ["cluster", "dask", "parsl"]:
            if name in config_data:
                data[name] = config_data[name]

    return data


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()
    exit_callbacks = []

    # Construct object
    settings = ManagerSettings(**args)

    logger_map = {AdapterEnum.pool: "",
                  AdapterEnum.dask: "dask_jobqueue.core",
                  AdapterEnum.parsl: "parsl"}
    if settings.common.verbose:
        adapter_logger = logging.getLogger(logger_map[settings.common.adapter])
        adapter_logger.setLevel("DEBUG")

    if settings.manager.log_file_prefix is not None:
        tornado.options.options['log_file_prefix'] = settings.manager.log_file_prefix
        # Clones the log to the output
        tornado.options.options['log_to_stderr'] = True
    tornado.log.enable_pretty_logging()

    if settings.manager.test:
        # Test this manager, no client needed
        client = None
    else:
        # Connect to a specified fractal server
        client = qcfractal.interface.FractalClient(
            address=settings.server.fractal_uri, **settings.server.dict(skip_defaults=True, exclude={"fractal_uri"}))

    # Figure out per-task data
    cores_per_task = settings.common.ncores // settings.common.ntasks
    memory_per_task = settings.common.memory / settings.common.ntasks
    if cores_per_task < 1:
        raise ValueError("Cores per task must be larger than one!")

    if settings.common.adapter == "pool":
        from concurrent.futures import ProcessPoolExecutor

        queue_client = ProcessPoolExecutor(max_workers=settings.common.ntasks)

    elif settings.common.adapter == "dask":

        dask_settings = settings.dask.dict(skip_defaults=True)
        # Checks
        if "extra" not in dask_settings:
            dask_settings["extra"] = []
        if QCA_RESOURCE_STRING not in dask_settings["extra"]:
            dask_settings["extra"].append(QCA_RESOURCE_STRING)
        # Scheduler opts
        scheduler_opts = settings.cluster.scheduler_options.copy()
        if settings.cluster.node_exclusivity and "--exclusive" not in scheduler_opts:
            scheduler_opts.append("--exclusive")

        _cluster_loaders = {"slurm": "SLURMCluster", "pbs": "PBSCluster", "moab": "MoabCluster", "sge": "SGECluster",
                            "lsf": "LSFCluster"}

        # Create one construct to quickly merge dicts with a final check
        dask_construct = {
            "name": "QCFractal_Dask_Compute_Executor",
            "cores": settings.common.ncores,
            "memory": str(settings.common.memory) + "GB",
            "processes": settings.common.ntasks, # Number of workers to generate == tasks
            "walltime": settings.cluster.walltime,
            "job_extra": scheduler_opts,
            "env_extra": settings.cluster.task_startup_commands,
            **dask_settings}

        # Import the dask things we need
        from dask.distributed import Client
        cluster_module = cli_utils.import_module("dask_jobqueue", package=_cluster_loaders[settings.cluster.scheduler])
        cluster_class = getattr(cluster_module, _cluster_loaders[settings.cluster.scheduler])

        from dask_jobqueue import SGECluster

        class SGEClusterWithJobQueue(SGECluster):
            """Helper class until Dask Jobqueue fixes #256"""
            def __init__(self, job_extra=None, **kwargs):
                super().__init__(**kwargs)
                if job_extra is not None:
                    more_header = ["#$ %s" % arg for arg in job_extra]
                    self.job_header += "\n" + "\n".join(more_header)

        from dask_jobqueue import LSFCluster
        from dask_jobqueue import lsf

        def lsf_format_bytes_ceil_with_unit(n: int, unit: str = "mb") -> str:
            """
            Special function we will use to monkey-patch as a partial into the dask_jobqueue.lsf file if need be
            Because the function exists as part of the lsf.py and not a staticmethod of the LSFCluster class, we have
            to do it this way.
            """
            unit = unit.lower()
            converter = {
                "b": 0,
                "kb": 1,
                "mb": 2,
                "gb": 3,
                "tb": 4,
                "pb": 5,
                "eb": 6
            }
            return "%d" % ceil(n / (1000 ** converter[unit]))

        # Temporary fix until Dask Jobqueue fixes #256
        if cluster_class is SGECluster and 'job_extra' not in inspect.getfullargspec(SGECluster.__init__).args:
            # Should the SGECluster ever get fixed, this if statement should automatically ensure we stop
            # using the custom class
            cluster_class = SGEClusterWithJobQueue
        # Temporary fix until unit system is checked in the LSFCluster of dask
        elif cluster_class is LSFCluster and 'lsf_units' not in inspect.getfullargspec(SGECluster.__init__).args:
            # We have to do some serious monkey patching here
            # Try to infer the unit system
            if settings.dask.lsf_units is not None:
                logger.debug(f"Setting the unit system for LSF to {settings.dask.lsf_units} based on Manager config")
                unit = settings.dask.lsf_units
            else:
                # Not manually set, search for automatically, Using docs from LSF 9.1.3 for search/defaults
                unit = "kb"  # Default fallback unit
                try:
                    # Start looking for the LSF conf file
                    conf_dir = "/etc"  # Fall back directory
                    # Search the two environment variables the docs say it could be at (likely a typo in docs)
                    for conf_env in ["$LSF_ENVDIR", "$LSF_CONFDIR"]:
                        conf_search = os.environ.get(conf_env, None)
                        if conf_search is not None:
                            conf_dir = conf_search
                            break
                    conf_path = os.path.join(conf_dir, 'lsf.conf')
                    conf_file = open(conf_path, 'r').readlines()
                    # Reverse order search (in case defined twice)
                    for line in conf_file[::-1]:
                        # Look for very specific line
                        line = line.strip()
                        if not line.strip().startswith("LSF_UNIT_FOR_LIMITS"):
                            continue
                        # Found the line, infer the unit
                        unit = line.split("=")[1].lower()
                        break
                    logger.debug(f"Setting units to {unit} from the LSF config file at {conf_path}")
                # Trap the lsf.conf does not exist, and the conf file not setup right (i.e. "$VAR=xxx^" regex-form)
                except (FileNotFoundError, IndexError):
                    # No conf file found, assume defaults
                    logger.warning("Could not find lsf.conf file and LSF_UNIT_FOR_LIMITS variable within ")
            settings.dask.pop('lsf_units')  # Remove for integrity
            lsf_format_bytes_ceil = partial(lsf_format_bytes_ceil_with_unit, unit=unit)
            # Finally, monkey patch unit calculation routine with the partial function at fixed units
            lsf.lsf_format_bytes_ceil = lsf_format_bytes_ceil

        cluster = cluster_class(**dask_construct)

        # Setup up adaption
        # Workers are distributed down to the cores through the sub-divided processes
        # Optimization may be needed
        workers = settings.common.ntasks * settings.cluster.max_cluster_jobs
        if settings.cluster.adaptive == AdaptiveCluster.adaptive:
            cluster.adapt(minimum=0, maximum=workers, interval="10s")
        else:
            cluster.scale(workers)

        queue_client = Client(cluster)

        # Make sure tempdir gets assigned correctly

        # Dragonstooth has the low priority queue

    elif settings.common.adapter == "parsl":

        scheduler_opts = settings.cluster.scheduler_options

        if not settings.cluster.node_exclusivity:
            raise ValueError("For now, QCFractal can only be run with Parsl in node exclusivity. This will be relaxed "
                             "in a future release of Parsl and QCFractal")

        # Import helpers
        _provider_loaders = {"slurm": "SlurmProvider",
                             "pbs": "TorqueProvider",
                             "moab": "TorqueProvider",
                             "sge": "GridEngineProvider",
                             "lsf": None}

        if _provider_loaders[settings.cluster.scheduler] is None:
            raise ValueError(f"Parsl does not know how to handle cluster of type {settings.cluster.scheduler}.")

        # Headers
        _provider_headers = {"slurm": "#SBATCH",
                             "pbs": "#PBS",
                             "moab": "#PBS",
                             "sge": "#$$",
                             "lsf": None
                             }

        # Import the parsl things we need
        from parsl.config import Config
        from parsl.executors import HighThroughputExecutor
        from parsl.addresses import address_by_hostname
        provider_module = cli_utils.import_module("parsl.providers",
                                                  package=_provider_loaders[settings.cluster.scheduler])
        provider_class = getattr(provider_module, _provider_loaders[settings.cluster.scheduler])
        provider_header = _provider_headers[settings.cluster.scheduler]

        if _provider_loaders[settings.cluster.scheduler] == "moab":
            logger.warning("Parsl uses its TorqueProvider for Moab clusters due to the scheduler similarities. "
                           "However, if you find a bug with it, please report to the Parsl and QCFractal developers so "
                           "it can be fixed on each respective end.")

        # Setup the providers

        # Create one construct to quickly merge dicts with a final check
        common_parsl_provider_construct = {
            "init_blocks": 0,  # Update this at a later time of Parsl
            "max_blocks": settings.cluster.max_cluster_jobs,
            "walltime": settings.cluster.walltime,
            "scheduler_options": f'{provider_header} ' + f'\n{provider_header} '.join(scheduler_opts) + '\n',
            "nodes_per_block": 1,
            "worker_init": '\n'.join(settings.cluster.task_startup_commands),
            **settings.parsl.provider.dict(skip_defaults=True, exclude={"partition"})
        }
        if settings.cluster.scheduler == "slurm":
            # The Parsl SLURM constructor has a strange set of arguments
            provider = provider_class(settings.parsl.provider.partition,
                                      exclusive=settings.cluster.node_exclusivity,
                                      **common_parsl_provider_construct)
        else:
            provider = provider_class(**common_parsl_provider_construct)

        parsl_executor_construct = {
            "label": "QCFractal_Parsl_{}_Executor".format(settings.cluster.scheduler.title()),
            "cores_per_worker": cores_per_task,
            "max_workers": settings.common.ntasks * settings.cluster.max_cluster_jobs,
            "provider": provider,
            "address": address_by_hostname(),
            **settings.parsl.executor.dict(skip_defaults=True)}

        queue_client = Config(
            executors=[HighThroughputExecutor(**parsl_executor_construct)])

    else:
        raise KeyError("Unknown adapter type '{}', available options: {}.\n"
                       "This code should also be unreachable with pydantic Validation, so if "
                       "you see this message, please report it to the QCFractal GitHub".format(
                           settings.common.adapter, [getattr(AdapterEnum, v).value for v in AdapterEnum]))

    # Build out the manager itself
    manager = qcfractal.queue.QueueManager(
        client,
        queue_client,
        max_tasks=settings.manager.max_tasks,
        queue_tag=settings.manager.queue_tag,
        manager_name=settings.manager.manager_name,
        update_frequency=settings.manager.update_frequency,
        cores_per_task=cores_per_task,
        memory_per_task=memory_per_task,
        scratch_directory=settings.common.scratch_directory,
        verbose=settings.common.verbose
    )

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


if __name__ == '__main__':
    main()
