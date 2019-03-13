"""
A command line interface to the qcfractal server.
"""

import argparse
from enum import Enum
from typing import List, Optional

from pydantic import BaseSettings, validator, BaseModel, conint, confloat
import qcfractal
import tornado.log
import qcengine as qcng

from . import cli_utils

__all__ = ["main"]

QCA_RESOURCE_STRING = '--resources process=1'
MANAGER_CONFIG_NAME = "qcf_manager_config.yaml"


class SettingsCommonConfig:
    env_prefix = "QCA_"
    case_insensitive = True


class AdapterEnum(str, Enum):
    dask = "dask"
    pool = "pool"


class CommonManagerSettings(BaseSettings):
    ntasks: int = 1
    cores: int = qcng.config.get_global("ncores")
    memory: confloat(gt=0) = qcng.config.get_global("memory")
    max_tasks: conint(gt=0) = 200
    manager_name: str = "unlabeled"
    update_frequency: float = 30
    test: bool = False
    adapter: AdapterEnum = AdapterEnum.pool
    log_file_prefix: str = None
    queue_tag: str = None

    class Config(SettingsCommonConfig):
        pass


class FractalServerSettings(BaseSettings):
    uri: str = "localhost:7777"
    username: str = None
    password: str = None
    verify: bool = None

    class Config(SettingsCommonConfig):
        pass

    @property
    def client_kwargs_map(self):
        """Helper function to generate the client kwargs"""
        kwargs = self.dict()
        construct = {"address": kwargs["uri"],
                     "username": kwargs["username"],
                     "password": kwargs["password"]}
        if kwargs["verify"] is not None:  # Allow client to accept the default
            construct["verify"] = kwargs["verify"]
        return construct


class SchedulerEnum(str, Enum):
    slurm = "slurm"
    torque = "torque"
    pbs = "pbs"


class ClusterSettings(BaseSettings):
    max_nodes: conint(gt=0) = 1
    node_exclusivity: bool = True
    scheduler: SchedulerEnum = None
    scheduler_options: List[str] = []
    task_startup_commands: List[str] = []
    walltime: str = "00:10:00"

    @validator("scheduler")
    def remap_pbs_to_torque(cls, v):
        if v == "pbs":
            print('Remapping `scheduler` option "pbs" to "torque"')
            v = 'torque'
        return v

    class Config(SettingsCommonConfig):
        pass


class _DaskJobQueueSettingsNoCheck(BaseSettings):
    interface: str = None
    extra: List[str] = None

    class Config(SettingsCommonConfig):
        extra = "allow"


class DaskJobQueueSettings(_DaskJobQueueSettingsNoCheck):
    """Pass through options beyond interface are permitted"""

    def __init__(self, **kwargs):
        """Enforce that the keys we are going to set remain untouched"""
        bad_set = set(kwargs.keys()) - {"name",
                                        "cores",
                                        "memory",
                                        "queue",
                                        "processes",
                                        "walltime",
                                        "env_extra",
                                        "qca_resource_string"
                                        }
        if bad_set:
            raise KeyError("The following items were set as part of dask_jobqueue, however, "
                           "there are other config items which control these in more generic "
                           "settings locations: {}".format(bad_set))
        super().__init__(**kwargs)


class ManagerSettings(BaseModel):
    common: CommonManagerSettings = CommonManagerSettings()
    server: FractalServerSettings = FractalServerSettings()
    cluster: Optional[ClusterSettings] = None
    dask_jobqueue: Optional[DaskJobQueueSettings] = None


def parse_args():
    parser = argparse.ArgumentParser(
        description='A CLI for a QCFractal QueueManager with a ProcessPoolExecutor or a Dask backend. '
                    'The Dask backend *requires* a config file due to the complexity of its setup. If a config '
                    'file is specified, the remaining options serve as CLI overwrites of the config.')

    parser.add_argument("config_file", type=str, nargs="?", default=None)
    # Keywords for ProcessPoolExecutor
    executor = parser.add_argument_group('Executor settings')
    executor.add_argument(
        "--ntasks",
        type=int,
        help="The number of simultaneous tasks for the executor to run, resources will be divided evenly.")
    executor.add_argument("--cores", type=int, help="The number of process for the executor")
    executor.add_argument("--memory", type=int, help="The total amount of memory on the system in GB")

    # FractalClient options
    server = parser.add_argument_group('FractalServer connection settings')
    server.add_argument(
        "--fractal-uri", type=str, help="FractalServer location to pull from")
    server.add_argument("-u", "--username", type=str, help="FractalServer username")
    server.add_argument("-p", "--password", type=str, help="FractalServer password")
    verify = server.add_mutually_exclusive_group()
    verify.add_argument("--no-verify", action="store_true",
                        help="Don't verify the SSL certificate, exclusive with `verify`")
    verify.add_argument("--verify", action="store_true",
                        help="Do verify the SSL certificate, exclusive with `no-verify`")

    # QueueManager options
    manager = parser.add_argument_group("QueueManager settings")
    manager.add_argument(
        "--max-tasks", type=int, help="Maximum number of tasks to hold at any given time.")
    manager.add_argument(
        "--manager-name", type=str, help="The name of the manager to start")
    manager.add_argument("--queue-tag", type=str, help="The queue tag to pull from")
    manager.add_argument("--log-file-prefix", type=str, help="The path prefix of the logfile to write to.")
    manager.add_argument(
        "--update-frequency", type=int, help="The frequency in seconds to check for complete tasks.")

    # Additional args
    optional = parser.add_argument_group('Optional Settings')
    optional.add_argument("--test", action="store_true", help="Boot and run a short test suite to validate setup")

    args = vars(parser.parse_args())

    return args


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()
    exit_callbacks = []

    # Try to read a config file first
    config_file = args["config_file"]
    data = {}
    if config_file is not None:
        try:
            data = cli_utils.read_config_file(config_file)
            print("Found config file at {}".format(config_file))
            if data == {} or data is None:
                print("Found a config file found at {}, but it appeared empty. Relying on CLI input only.\n"
                      "This can only create Pool Executors with limited options".format(config_file))
                data = {}  # Ensures data is a dict in case empty yaml, which returns None
        except FileNotFoundError:
            print("No config file found at {}, relying on CLI input only.\n"
                  "This can only create Pool Executors with limited options".format(config_file))

    # Handle CLI/args mappings
    # Handle the Manager settings
    if "common" not in data:
        data["common"] = {}
    for arg in ["cores", "memory", "ntasks", "max_tasks", "manager_name", "update_frequency", "queue_tag",
                "log_file_prefix", "test"]:
        # Overwrite the args from the config file
        if arg in args and args[arg] is not None:
            data["common"][arg] = args[arg]
    # Handle Fractal Server (have to map the CLI onto the Pydantic)
    if "server" not in data:
        data["server"] = {}
    for arg, var in [("fractal_uri", "uri"),
                     ("username", "username"),
                     ("password", "password")]:
        if arg in args and args[arg] is not None:
            data["server"][var] = args[arg]
    # Handle the verify, don't need to check both true as argparse handled that
    if args["no_verify"]:
        data["server"]["verify"] = False
    elif args["verify"]:
        data["server"]["verify"] = True
    # Else case here is default of None,

    # Construct object
    settings = ManagerSettings(**data)

    if settings.common.log_file_prefix is not None:
        tornado.options.options['log_file_prefix'] = settings.common.log_file_prefix
    tornado.log.enable_pretty_logging()

    if settings.common.test:
        # Test this manager, no client needed
        client = None
    else:
        # Connect to a specified fractal server
        # somewhere that FractalClient knows about!
        client = qcfractal.interface.FractalClient(**settings.server.client_kwargs_map)

    # Figure out per-task data
    cores_per_task = settings.common.cores // settings.common.ntasks
    memory_per_task = settings.common.memory / settings.common.ntasks
    if cores_per_task < 1:
        raise ValueError("Cores per task must be larger than one!")

    if settings.common.adapter == "pool":
        from concurrent.futures import ProcessPoolExecutor

        queue_client = ProcessPoolExecutor(max_workers=args["ntasks"])

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

        _cluster_loaders = {"slurm": "SLURMCluster",
                            "pbs": "PBSCluster",
                            "torque": "PBSCluster"}

        # Create one construct to quickly merge dicts with a final check
        dask_construct = _DaskJobQueueSettingsNoCheck(
            name="QCFractal_Dask_Compute_Executor",
            cores=settings.common.cores,
            memory=str(settings.common.memory) + "GB",
            processes=settings.common.ntasks,
            walltime=settings.cluster.walltime,
            job_extra=scheduler_opts,
            env_exta=settings.cluster.task_startup_commands,
            **dask_settings
        )

        # Import the dask things we need
        from dask.distributed import Client
        cluster_class = cli_utils.import_module("dask_jobqueue", package=_cluster_loaders[settings.cluster.scheduler])

        cluster = cluster_class(**dask_construct.dict())

        # Setup up adaption
        # Workers are distributed down to the cores through the sub-divided processes
        # Optimization may be needed
        cluster.adapt(minimum=0, maximum=settings.cluster.max_nodes)

        queue_client = Client(cluster)

        # Make sure tempdir gets assigned correctly

        # Dragonstooth has the low priority queue

    else:
        raise KeyError(
            "Unknown adapter type '{}', available options: {}.\n"
            "This code should also be unreachable with pydantic Validation, so if "
            "you see this message, please report it to the QCFractal GitHub".format(
                settings.common.adapter,
                [getattr(AdapterEnum, v).value for v in AdapterEnum]
            )
        )

    # Build out the manager itself
    manager = qcfractal.queue.QueueManager(
        client,
        queue_client,
        max_tasks=settings.common.max_tasks,
        queue_tag=settings.common.queue_tag,
        manager_name=settings.common.manager_name,
        update_frequency=settings.common.update_frequency,
        cores_per_task=cores_per_task,
        memory_per_task=memory_per_task)

    # Add exit callbacks
    for cb in exit_callbacks:
        manager.add_exit_callback(cb[0], *cb[1], **cb[2])

    # Either startup the manager or run until complete
    if settings.common.test:
        success = manager.test()
        if success is False:
            raise ValueError("Testing was not successful, failing.")
    else:

        cli_utils.install_signal_handlers(manager.loop, manager.stop)

        # Blocks until keyboard interrupt
        manager.start()


if __name__ == '__main__':
    main()
