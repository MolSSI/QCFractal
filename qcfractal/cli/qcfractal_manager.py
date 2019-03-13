"""
A command line interface to the qcfractal server.
"""

import argparse
from enum import Enum
from typing import List, Optional

from pydantic import BaseSettings, BaseModel, conint, confloat
import qcfractal
import tornado.log
import qcengine as qcng

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
    cores: int = qcng.config.get_global("ncores")
    memory: confloat(gt=0) = qcng.config.get_global("memory")

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


class SchedulerEnum(str, Enum):
    slurm = "slurm"
    pbs = "pbs"
    sge = "sge"
    moab = "moab"


class ClusterSettings(BaseSettings):
    max_nodes: conint(gt=0) = 1
    node_exclusivity: bool = True
    scheduler: SchedulerEnum = None
    scheduler_options: List[str] = []
    task_startup_commands: List[str] = []
    walltime: str = "00:10:00"

    class Config(SettingsCommonConfig):
        pass


class DaskQueueSettings(BaseSettings):
    """Pass through options beyond interface are permitted"""
    interface: str = None
    extra: List[str] = None

    def __init__(self, **kwargs):
        """Enforce that the keys we are going to set remain untouched"""
        bad_set = set(kwargs.keys()) - {
            "name", "cores", "memory", "queue", "processes", "walltime", "env_extra", "qca_resource_string"
        }
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
    common.add_argument("--cores", type=int, help="The number of process for the executor")
    common.add_argument("--memory", type=int, help="The total amount of memory on the system in GB")

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
        "common": _build_subset(args, {"adapter", "ntasks", "cores", "memory"}),
        "server": _build_subset(args, {"fractal_uri", "password", "username", "verify"}),
        "manager": _build_subset(args, {"max_tasks", "manager_name", "queue_tag", "log_file_prefix", "update_frequency", "test"}),
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

    if settings.manager.log_file_prefix is not None:
        tornado.options.options['log_file_prefix'] = settings.common.log_file_prefix
    tornado.log.enable_pretty_logging()

    if settings.manager.test:
        # Test this manager, no client needed
        client = None
    else:
        # Connect to a specified fractal server
        print(settings.server.fractal_uri, settings.server.dict(skip_defaults=True, exclude={"fractal_uri"}))
        client = qcfractal.interface.FractalClient(
            address=settings.server.fractal_uri, **settings.server.dict(skip_defaults=True, exclude={"fractal_uri"}))

    # Figure out per-task data
    cores_per_task = settings.common.cores // settings.common.ntasks
    memory_per_task = settings.common.memory / settings.common.ntasks
    if cores_per_task < 1:
        raise ValueError("Cores per task must be larger than one!")

    if settings.common.adapter == "pool":
        from concurrent.futures import ProcessPoolExecutor

        queue_client = ProcessPoolExecutor(max_workers=settings.common.ntasks)

    elif settings.common.adapter == "dask":

        dask_settings = settings.dask_jobqueue.dict(skip_defaults=True)
        # Checks
        if "extra" not in dask_settings:
            dask_settings["extra"] = []
        if QCA_RESOURCE_STRING not in dask_settings["extra"]:
            dask_settings["extra"].append(QCA_RESOURCE_STRING)
        # Scheduler opts
        scheduler_opts = settings.cluster.scheduler_options.copy()
        if settings.cluster.node_exclusivity and "--exclusive" not in scheduler_opts:
            scheduler_opts.append("--exclusive")

        _cluster_loaders = {"slurm": "SLURMCluster", "pbs": "PBSCluster", "moab": "MoabCluster", "sge": "SGECluster"}

        # Create one construct to quickly merge dicts with a final check
        dask_construct = {
            "name": "QCFractal_Dask_Compute_Executor",
            "cores": settings.common.cores,
            "memory": str(settings.common.memory) + "GB",
            "processes": settings.common.ntasks,
            "walltime": settings.cluster.walltime,
            "job_extra": scheduler_opts,
            "env_exta": settings.cluster.task_startup_commands,
            **dask_settings}

        # Import the dask things we need
        from dask.distributed import Client
        cluster_class = cli_utils.import_module("dask_jobqueue", package=_cluster_loaders[settings.cluster.scheduler])

        cluster = cluster_class(**dask_construct)

        # Setup up adaption
        # Workers are distributed down to the cores through the sub-divided processes
        # Optimization may be needed
        cluster.adapt(minimum=0, maximum=settings.cluster.max_nodes)

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
        memory_per_task=memory_per_task)

    # Add exit callbacks
    for cb in exit_callbacks:
        manager.add_exit_callback(cb[0], *cb[1], **cb[2])

    # Either startup the manager or run until complete
    if settings.manager.test:
        success = manager.test()
        if success is False:
            raise ValueError("Testing was not successful, failing.")
    else:

        cli_utils.install_signal_handlers(manager.loop, manager.stop)

        # Blocks until keyboard interrupt
        manager.start()


if __name__ == '__main__':
    main()
