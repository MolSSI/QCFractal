"""
A command line interface to the qcfractal server.
"""

import argparse
import inspect
import signal
import logging
from enum import Enum
from typing import List, Optional

import tornado.log

import qcengine as qcng
import qcfractal
from pydantic import BaseModel, BaseSettings, confloat, conint, validator

from . import cli_utils

__all__ = ["main"]

QCA_RESOURCE_STRING = '--resources process=1'


class SettingsCommonConfig:
    env_prefix = "QCA_"
    case_insensitive = True
    extra = "forbid"


class AdapterEnum(str, Enum):
    dask = "dask"
    pool = "pool"


class CommonManagerSettings(BaseSettings):
    # Task settings
    adapter: AdapterEnum = AdapterEnum.pool
    ntasks: int = 1
    ncores: int = qcng.config.get_global("ncores")
    memory: confloat(gt=0) = qcng.config.get_global("memory")
    scratch_directory: str = None
    verbose: bool = False

    class Config(SettingsCommonConfig):
        pass


class FractalServerSettings(BaseSettings):
    fractal_uri: str = "localhost:7777"
    username: str = None
    password: str = None
    verify: bool = None

    class Config(SettingsCommonConfig):
        pass


class QueueManagerSettings(BaseSettings):
    # General settings
    max_tasks: conint(gt=0) = 200
    manager_name: str = "unlabeled"
    queue_tag: str = None
    log_file_prefix: str = None
    update_frequency: float = 30
    test: bool = False
    ntests: int = 1


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
    max_nodes: conint(gt=0) = 1
    node_exclusivity: bool = True
    scheduler: SchedulerEnum = None
    scheduler_options: List[str] = []
    task_startup_commands: List[str] = []
    walltime: str = "06:00:00"
    adaptive: AdaptiveCluster = AdaptiveCluster.adaptive

    class Config(SettingsCommonConfig):
        pass

    @validator('scheduler', 'adaptive', pre=True)
    def things_to_lcase(cls, v):
        return v.lower()


class DaskQueueSettings(BaseSettings):
    """Pass through options beyond interface are permitted"""
    interface: str = None
    extra: List[str] = None

    def __init__(self, **kwargs):
        """Enforce that the keys we are going to set remain untouched"""
        # This set blocks the Dask Jobqueue `Cluster` keywords which we set, so the names of the keywords align
        # to those classes' kwargs, not whatever Fractal chooses to use as keywords.
        forbidden_set = {"name", "cores", "memory", "processes", "walltime", "env_extra", "qca_resource_string"}
        bad_set = set(kwargs.keys()) & forbidden_set
        if bad_set:
            raise KeyError("The following items were set as part of dask_jobqueue, however, "
                           "there are other config items which control these in more generic "
                           "settings locations: {}".format(bad_set))
        super().__init__(**kwargs)

    class Config(SettingsCommonConfig):
        # This overwrites the base config to allow other keywords to be fed in
        extra = "allow"


class ManagerSettings(BaseModel):
    common: CommonManagerSettings = CommonManagerSettings()
    server: FractalServerSettings = FractalServerSettings()
    manager: QueueManagerSettings = QueueManagerSettings()
    cluster: Optional[ClusterSettings] = None
    dask: Optional[DaskQueueSettings] = None

    class Config:
        extra = "forbid"


def parse_args():
    parser = argparse.ArgumentParser(
        description='A CLI for a QCFractal QueueManager with a ProcessPoolExecutor or a Dask backend. '
        'The Dask backend *requires* a config file due to the complexity of its setup. If a config '
        'file is specified, the remaining options serve as CLI overwrites of the config.')

    parser.add_argument("--config-file", type=str, default=None)

    # Common settings
    common = parser.add_argument_group('Common Adapter Settings')
    common.add_argument(
        "--adapter", type=str, help="The backend adapter to use, currently only {'dask', 'pool'} are valid.")
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

        for name in ["cluster", "dask"]:
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
                  AdapterEnum.dask: "dask_jobqueue.core"}
    if settings.common.verbose:
        logger = logging.getLogger(logger_map[settings.common.adapter])
        logger.setLevel("DEBUG")

    if settings.manager.log_file_prefix is not None:
        tornado.options.options['log_file_prefix'] = settings.common.log_file_prefix
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
            "env_exta": settings.cluster.task_startup_commands,
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

        # Temporary fix until Dask Jobqueue fixes #256
        if cluster_class is SGECluster and 'job_extra' not in inspect.getfullargspec(SGECluster.__init__).args:
            # Should the SGECluster ever get fixed, this if statement should automatically ensure we stop
            # using the custom class
            cluster_class = SGEClusterWithJobQueue

        cluster = cluster_class(**dask_construct)

        # Setup up adaption
        # Workers are distributed down to the cores through the sub-divided processes
        # Optimization may be needed
        workers = settings.common.ntasks * settings.cluster.max_nodes
        if settings.cluster.adaptive == AdaptiveCluster.adaptive:
            cluster.adapt(minimum=0, maximum=workers, interval="10s")
        else:
            cluster.scale(workers)

        queue_client = Client(cluster)

        # Make sure tempdir gets assigned correctly

        # Dragonstooth has the low priority queue

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
