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
    The Common settings are the settings most users will need to adjust regularly to control the nature of
    task execution and the hardware under which tasks are executed on. This block is often unique to each deployment,
    user, and manager and will be the most commonly updated options, even as config files are copied and reused, and
    even on the same platform/cluster.

    """
    adapter: AdapterEnum = Schema(
        AdapterEnum.pool,
        description="Which type of Distributed adapter to run tasks through."
    )
    tasks_per_worker: int = Schema(
        1,
        description="Number of concurrent tasks to run *per Worker* which is executed. Total number of concurrent "
                    "tasks is this value times max_workers, assuming the hardware is available. With the "
                    "pool adapter, and/or if max_workers=1, tasks_per_worker *is* the number of concurrent tasks."
    )
    cores_per_worker: int = Schema(
        qcng.config.get_global("ncores"),
        description="Number of cores to be consumed by the Worker and distributed over the tasks_per_worker. These "
                    "cores are divided evenly, so it is recommended that quotient of cores_per_worker/tasks_per_worker "
                    "be a whole number else the core distribution is left up to the logic of the adapter. The default "
                    "value is read from the number of detected cores on the system you are executing on.",
        gt=0
    )
    memory_per_worker: float = Schema(
        qcng.config.get_global("memory"),
        description="Amount of memory (in GB) to be consumed and distributed over the tasks_per_worker. This memory is "
                    "divided evenly, but is ultimately at the control of the adapter. Engine will only allow each of "
                    "its calls to consume memory_per_worker/tasks_per_worker of memory. Total memory consumed by this "
                    "manager at any one time is this value times max_workers. The default value is read "
                    "from the amount of memory detected on the system you are executing on.",
        gt=0
    )
    max_workers: int = Schema(
        1,
        description="The maximum number of Workers which are allowed to be run at the same time. The total number of "
                    "concurrent tasks will maximize at this quantity times tasks_per_worker."
                    "The total number "
                    "of Jobs on a cluster which will be started is equal to this parameter in most cases, and should "
                    "be assumed 1 Worker per Job. Any exceptions to this will be documented. "
                    "In node exclusive mode this is equivalent to the maximum number of nodes which you will consume. "
                    "This must be a positive, non zero integer.",
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


cli_utils.doc_formatter(CommonManagerSettings)


class FractalServerSettings(BaseSettings):
    """
    Settings pertaining to the Fractal Server you wish to pull tasks from and push completed tasks to. Each manager
    supports exactly 1 Fractal Server to be in communication with, and exactly 1 user on that Fractal Server. These
    can be changed, but only once the Manager is shutdown and the settings changed. Multiple Managers however can be
    started in parallel with each other, but must be done as separate calls to the CLI.

    Caution: The password here is written in plain text, so it is up to the owner/writer of the configuration file
    to ensure its security.
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


cli_utils.doc_formatter(FractalServerSettings)


class QueueManagerSettings(BaseSettings):
    """
    Fractal Queue Manger settings. These are options which control the setup and execution of the Fractal Manager
    itself.
    """
    manager_name: str = Schema(
        "unlabeled",
        description="Name of this scheduler to present to the Fractal Server. Descriptive names help the server "
                    "identify the manager resource and assists with debugging."
    )
    queue_tag: Optional[str] = Schema(
        None,
        description="Only pull tasks from the Fractal Server with this tag. If not set (None/null), then pull untagged "
                    "tasks, which should be the majority of tasks. This option should only be used when you want to "
                    "pull very specific tasks which you know have been tagged as such on the server. If the server has "
                    "no tasks with this tag, no tasks will be pulled (and no error is raised because this is intended "
                    "behavior)."
    )
    log_file_prefix: Optional[str] = Schema(
        None,
        description="Full path to save a log file to, including the filename. If not provided, information will still "
                    "be reported to terminal, but not saved. When set, logger information is sent both to this file "
                    "and the terminal."
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
                    "instead submits netsts worth trial tasks per quantum chemistry program it finds. These tasks are "
                    "generated locally and do not need a running Fractal Server to work. Helpful for ensuring the "
                    "Manager is configured correctly and the quantum chemistry codes are operating as expected."
    )
    ntests: int = Schema(
        5,
        description="Number of tests to run if the `test` flag is set to True. Total number of tests will be this "
                    "number times the number of found quantum chemistry programs. Does nothing if `test` is False",
        gt=0
    )
    max_queued_tasks: Optional[int] = Schema(
        None,
        description="Generally should not be set. Number of tasks to pull from the Fractal Server to keep locally at "
                    "all times. If `None`, this is automatically computed as "
                    "`ceil(common.tasks_per_worker*common.max_workers*1.2) + 1`. As tasks are completed, the "
                    "local pool is filled back up to this value. These tasks will all attempt to be run concurrently, "
                    "but concurrent tasks are limited by number of cluster jobs and tasks per job. Pulling too many of "
                    "these can result in under-utilized managers from other sites and result in less FIFO returns. As "
                    "such it is recommended not to touch this setting in general as you will be given enough tasks to "
                    "fill your maximum throughput with a buffer (assuming the queue has them).",
        gt=0
    )


cli_utils.doc_formatter(QueueManagerSettings)


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
    jobs you are submitting, separate from the nature of the compute tasks you will be running within them. As such,
    the options here are things like wall time (per job), which Scheduler your cluster has (like PBS or SLURM),
    etc. No additional options are allowed here.
    """
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
                    "should be a separate string entry in the list. No validation is done on this with respect to "
                    "valid directives so it is on the user to know what they need to set."
    )
    task_startup_commands: List[str] = Schema(
        [],
        description="Additional commands to be run before starting the Workers and the task distribution. This can "
                    "include commands needed to start things like conda environments or setting environment variables "
                    "before executing the Workers. These commands are executed first before any of the distributed "
                    "commands run and are added to the batch scripts as individual commands per entry, verbatim."
    )
    walltime: str = Schema(
        "06:00:00",
        description="Wall clock time of each cluster job started. Presented as a string in HH:MM:SS form, but your "
                    "cluster may have a different structural syntax. This number should be set high as there should "
                    "be a number of Fractal tasks which are run for each submitted cluster job. Ideally, the job "
                    "will start, the Worker will land, and the Worker will crunch through as many tasks as it can; "
                    "meaning the job which has a Worker in it must continue existing to minimize time spend "
                    "redeploying Workers."
    )
    adaptive: AdaptiveCluster = Schema(
        AdaptiveCluster.adaptive,
        description="Whether or not to use adaptive scaling of Workers or not. If set to 'static', a fixed number of "
                    "Workers will be started (and likely *NOT* restarted when the wall clock is reached). When set to "
                    "'adaptive' (the default), the distributed engine will try to adaptively scale the number of "
                    "Workers based on tasks in the queue. This is str instead of bool type variable in case more "
                    "complex adaptivity options are added in the future."
    )

    class Config(SettingsCommonConfig):
        pass

    @validator('scheduler', 'adaptive', pre=True)
    def things_to_lcase(cls, v):
        return v.lower()


cli_utils.doc_formatter(ClusterSettings)


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

    Valid values for this field are functions of your cluster.scheduler and no linting is done ahead of trying to pass
    these to Dask.

    NOTE: The parameters listed here are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered. If you try to set a value
    which is derived from other options in the YAML file, an error is raised and you are told exactly which one is
    forbidden.

    Please see the docs for the provider for more information.
    """
    interface: Optional[str] = Schema(
        None,
        description="Name of the network adapter to use as communication between the head node and the compute node."
                    "There are oddities of this when the head node and compute node use different ethernet adapter "
                    "names and we have not figured out exactly which combination is needed between this and the "
                    "poorly documented `ip` keyword which appears to be for Workers, but not the Client."
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


cli_utils.doc_formatter(DaskQueueSettings)


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
    address: Optional[str] = Schema(
        None,
        description="This only needs to be set in conditional cases when the head node and compute nodes use a "
                    "differently named ethernet adapter.\n\n"
                    "An address to connect to the main Parsl process which is reachable from the network in which "
                    "Workers will be running. This can be either a hostname as returned by hostname or an IP address. "
                    "Most login nodes on clusters have several network interfaces available, only some of which can be "
                    "reached from the compute nodes. Some trial and error might be necessary to identify what "
                    "addresses are reachable from compute nodes."
    )
    _forbidden_set = {"label", "provider", "cores_per_worker", "max_workers"}
    _forbidden_name = "the parsl executor"


cli_utils.doc_formatter(ParslExecutorSettings)


class ParslProviderSettings(SettingsBlocker):
    """
    Settings for the Parsl Provider class. Valid values for this field are functions of your cluster.scheduler and no
    linting is done ahead of trying to pass these to Parsl.
    Please see the docs for the provider information

    NOTE: The parameters listed here are a special exception for additional features Fractal has engineered or
    options which should be considered for some of the edge cases we have discovered. If you try to set a value
    which is derived from other options in the YAML file, an error is raised and you are told exactly which one is
    forbidden.

    SLURM: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.SlurmProvider.html
    PBS/Torque/Moba: https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.TorqueProvider.html
    SGE (Sun GridEngine): https://parsl.readthedocs.io/en/latest/stubs/parsl.providers.GridEngineProvider.html

    """
    partition: str = Schema(
        None,
        description="The name of the cluster.scheduler partition being submitted to. Behavior, valid values, and even"
                    "its validity as a set variable are a function of what type of queue scheduler your specific "
                    "cluster has (e.g. this variable should NOT be present for PBS clusters). "
                    "Check with your Sys. Admins and/or your cluster documentation."
    )
    _forbidden_set = {"nodes_per_block", "max_blocks", "worker_init", "scheduler_options", "wall_time"}
    _forbidden_name = "parsl's provider"


cli_utils.doc_formatter(ParslProviderSettings)


class ParslQueueSettings(BaseSettings):
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


cli_utils.doc_formatter(ParslQueueSettings)


class ManagerSettings(BaseModel):
    """
    The config file for setting up a QCFractal Manager, all sub fields of this model are at equal top-level of the
    YAML file. No additional top-level fields are permitted, but sub-fields may have their own additions.

    Not all fields are required and many will depend on the cluster you are running, and the adapter you choose
    to run on.

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


cli_utils.doc_formatter(ManagerSettings)


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
        "--tasks_per_worker",
        type=int,
        help="The number of simultaneous tasks for the executor to run, resources will be divided evenly.")
    common.add_argument("--cores-per-worker", type=int, help="The number of process for each executor's Workers")
    common.add_argument("--memory-per-worker", type=int, help="The total amount of memory on the system in GB")
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
        help="Do verify the SSL certificate, leave off (unset) for servers with custom SSL certificates.")

    # QueueManager options
    manager = parser.add_argument_group("QueueManager settings")
    manager.add_argument("--manager-name", type=str, help="The name of the manager to start")
    manager.add_argument("--queue-tag", type=str, help="The queue tag to pull from")
    manager.add_argument("--log-file-prefix", type=str, help="The path prefix of the logfile to write to.")
    manager.add_argument("--update-frequency", type=int, help="The frequency in seconds to check for complete tasks.")
    manager.add_argument("--max-queued-tasks", type=int, help="Maximum number of tasks to hold at any given time. "
                                                              "Generally should not be set.")

    # Additional args
    optional = parser.add_argument_group('Optional Settings')
    optional.add_argument("--test", action="store_true", help="Boot and run a short test suite to validate setup")
    optional.add_argument(
        "--ntests", type=int, help="How many tests per found program to run, does nothing without --test set")
    optional.add_argument("--schema", action="store_true", help="Display the current Schema (Pydantic) for the YAML "
                                                                "config file and exit. This will always show the "
                                                                "most up-to-date schema. It will be presented in a "
                                                                "JSON-like format.")

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
        "common": _build_subset(args, {"adapter", "tasks_per_worker", "cores_per_worker", "memory_per_worker",
                                       "scratch_directory", "verbose"}),
        "server": _build_subset(args, {"fractal_uri", "password", "username", "verify"}),
        "manager": _build_subset(args, {"max_queued_tasks", "manager_name", "queue_tag", "log_file_prefix",
                                        "update_frequency", "test", "ntests"}),
        # This set is for this script only, items here should not be passed to the ManagerSettings nor any other
        # classes
        "debug": _build_subset(args, {"schema"})
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

    try:
        if args["debug"]["schema"]:
            print(ManagerSettings.schema_json(indent=2))
            return  # We're done, exit normally
    except KeyError:
        pass  # Don't worry if schema isn't in the list
    finally:
        args.pop("debug", None)  # Ensure the debug key is not present

    # Construct object
    settings = ManagerSettings(**args)

    logger_map = {AdapterEnum.pool: "",
                  AdapterEnum.dask: "dask_jobqueue.core",
                  AdapterEnum.parsl: "parsl"}
    if settings.common.verbose:
        adapter_logger = logging.getLogger(logger_map[settings.common.adapter])
        adapter_logger.setLevel("DEBUG")
        logger.setLevel("DEBUG")

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
    cores_per_task = settings.common.cores_per_worker // settings.common.tasks_per_worker
    memory_per_task = settings.common.memory_per_worker / settings.common.tasks_per_worker
    if cores_per_task < 1:
        raise ValueError("Cores per task must be larger than one!")

    if settings.common.adapter == "pool":
        from concurrent.futures import ProcessPoolExecutor

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
        if settings.cluster.node_exclusivity and "--exclusive" not in scheduler_opts:
            scheduler_opts.append("--exclusive")

        _cluster_loaders = {"slurm": "SLURMCluster", "pbs": "PBSCluster", "moab": "MoabCluster", "sge": "SGECluster",
                            "lsf": "LSFCluster"}

        # Create one construct to quickly merge dicts with a final check
        dask_construct = {
            "name": "QCFractal_Dask_Compute_Executor",
            "cores": settings.common.cores_per_worker,
            "memory": str(settings.common.memory_per_worker) + "GB",
            "processes": settings.common.tasks_per_worker,  # Number of workers to generate == tasks in this construct
            "walltime": settings.cluster.walltime,
            "job_extra": scheduler_opts,
            "env_extra": settings.cluster.task_startup_commands,
            **dask_settings}

        try:
            # Import the dask things we need
            from dask.distributed import Client
            cluster_module = cli_utils.import_module("dask_jobqueue",
                                                     package=_cluster_loaders[settings.cluster.scheduler])
            cluster_class = getattr(cluster_module, _cluster_loaders[settings.cluster.scheduler])
        except ImportError:
            raise ImportError("You need both `dask` and `dask-jobqueue` to use the `dask` adapter")

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

        def lsf_format_bytes_ceil_with_unit(n: int, unit_str: str = "mb") -> str:
            """
            Special function we will use to monkey-patch as a partial into the dask_jobqueue.lsf file if need be
            Because the function exists as part of the lsf.py and not a staticmethod of the LSFCluster class, we have
            to do it this way.
            """
            unit_str = unit_str.lower()
            converter = {
                "b": 0,
                "kb": 1,
                "mb": 2,
                "gb": 3,
                "tb": 4,
                "pb": 5,
                "eb": 6
            }
            return "%d" % ceil(n / (1000 ** converter[unit_str]))

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
                    for conf_env in ["LSF_ENVDIR", "LSF_CONFDIR"]:
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
                        # Found the line, infer the unit, only first 2 chars after "="
                        unit = line.split("=")[1].lower()[:2]
                        break
                    logger.debug(f"Setting units to {unit} from the LSF config file at {conf_path}")
                # Trap the lsf.conf does not exist, and the conf file not setup right (i.e. "$VAR=xxx^" regex-form)
                except (FileNotFoundError, IndexError):
                    # No conf file found, assume defaults
                    logger.warning("Could not find lsf.conf file and LSF_UNIT_FOR_LIMITS variable within ")
            dask_construct.pop('lsf_units', None)  # Remove for integrity
            lsf_format_bytes_ceil = partial(lsf_format_bytes_ceil_with_unit, unit_str=unit)
            # Finally, monkey patch unit calculation routine with the partial function at fixed units
            lsf.lsf_format_bytes_ceil = lsf_format_bytes_ceil

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
        try:
            from parsl.config import Config
            from parsl.executors import HighThroughputExecutor
            from parsl.addresses import address_by_hostname
            provider_module = cli_utils.import_module("parsl.providers",
                                                      package=_provider_loaders[settings.cluster.scheduler])
            provider_class = getattr(provider_module, _provider_loaders[settings.cluster.scheduler])
            provider_header = _provider_headers[settings.cluster.scheduler]
        except ImportError:
            raise ImportError("You need the `parsl` package to use the `parsl` adapter")

        if _provider_loaders[settings.cluster.scheduler] == "moab":
            logger.warning("Parsl uses its TorqueProvider for Moab clusters due to the scheduler similarities. "
                           "However, if you find a bug with it, please report to the Parsl and QCFractal developers so "
                           "it can be fixed on each respective end.")

        # Setup the providers

        # Create one construct to quickly merge dicts with a final check
        common_parsl_provider_construct = {
            "init_blocks": 0,  # Update this at a later time of Parsl
            "max_blocks": settings.common.max_workers,
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
            "max_workers": settings.common.tasks_per_worker * settings.common.max_workers,
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
    # Compute max tasks
    if settings.manager.max_queued_tasks is None:
        # Tasks * jobs * buffer + 1
        max_queued_tasks = ceil(settings.common.tasks_per_worker * settings.common.max_workers * 1.20) + 1
    else:
        max_queued_tasks = settings.manager.max_queued_tasks

    manager = qcfractal.queue.QueueManager(
        client,
        queue_client,
        max_tasks=max_queued_tasks,
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
