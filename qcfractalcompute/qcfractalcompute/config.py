import logging
import os
from typing import List, Optional, Union, Dict, Any

import yaml

try:
    from pydantic.v1 import BaseModel, Field, validator, root_validator
except ImportError:
    from pydantic import BaseModel, Field, validator, root_validator
from typing_extensions import Literal

from qcportal.utils import seconds_to_hms, duration_to_seconds, update_nested_dict


def _make_abs_path(path: Optional[str], base_folder: str, default_filename: Optional[str]) -> Optional[str]:
    # No path specified, no default
    if path is None and default_filename is None:
        return None

    # Path isn't specified, but default is given
    if path is None:
        path = default_filename

    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    if os.path.isabs(path):
        return path
    else:
        path = os.path.join(base_folder, path)
        return os.path.abspath(path)


class PackageEnvironmentSettings(BaseModel):
    """
    Environments with installed packages that can be used to run calculations

    The compute manager will query these environments to see what packages are installed, and
    direct appropriate calculations to them.
    """

    use_manager_environment: bool = True
    conda: List[str] = Field([], description="List of conda environments to query for installed packages")
    apptainer: List[str] = Field(
        [], description="List of paths to apptainer/singularity files to query for installed packages"
    )


class ExecutorConfig(BaseModel):
    type: str
    compute_tags: List[str]
    worker_init: List[str] = []

    scratch_directory: Optional[str] = None
    bind_address: Optional[str] = None

    cores_per_worker: int
    memory_per_worker: float

    extra_executor_options: Dict[str, Any] = {}

    environments: PackageEnvironmentSettings = PackageEnvironmentSettings()

    class Config(BaseModel.Config):
        case_insensitive = True
        extra = "forbid"

    # TODO - DEPRECATED - REMOVE EVENTUALLY
    @root_validator(pre=True)
    def _old_queue_tag(cls, values):
        if "queue_tags" in values:
            values["compute_tags"] = values.pop("queue_tags")

        return values


class CustomExecutorConfig(ExecutorConfig):
    type: Literal["custom"] = "custom"
    path: str


class LocalExecutorConfig(ExecutorConfig):
    type: Literal["local"] = "local"
    max_workers: int


class SlurmExecutorConfig(ExecutorConfig):
    type: Literal["slurm"] = "slurm"

    walltime: str
    exclusive: bool = True
    partition: Optional[str] = None
    account: Optional[str] = None

    workers_per_node: int
    max_nodes: int

    scheduler_options: List[str] = []

    @validator("walltime", pre=True)
    def walltime_must_be_str(cls, v):
        if isinstance(v, int):
            return seconds_to_hms(v)
        else:
            return v


class TorqueExecutorConfig(ExecutorConfig):
    type: Literal["torque"] = "torque"

    walltime: str
    account: Optional[str] = None
    queue: Optional[str] = None

    workers_per_node: int
    max_nodes: int

    scheduler_options: List[str] = []

    @validator("walltime", pre=True)
    def walltime_must_be_str(cls, v):
        return seconds_to_hms(duration_to_seconds(v))


class LSFExecutorConfig(ExecutorConfig):
    type: Literal["lsf"] = "lsf"

    walltime: str
    project: Optional[str] = None
    queue: Optional[str] = None

    workers_per_node: int
    max_nodes: int

    request_by_nodes: bool = True
    bsub_redirection: bool = True

    scheduler_options: List[str] = []

    @validator("walltime", pre=True)
    def walltime_must_be_str(cls, v):
        return seconds_to_hms(duration_to_seconds(v))


AllExecutorTypes = Union[
    CustomExecutorConfig, LocalExecutorConfig, SlurmExecutorConfig, TorqueExecutorConfig, LSFExecutorConfig
]


class FractalServerSettings(BaseModel):
    """
    Settings pertaining to the Fractal Server you wish to pull tasks from and push completed tasks to. Each manager
    supports exactly 1 Fractal Server to be in communication with, and exactly 1 user on that Fractal Server. These
    can be changed, but only once the Manager is shutdown and the settings changed. Multiple Managers however can be
    started in parallel with each other, but must be done as separate calls to the CLI.

    Caution: The password here is written in plain text, so it is up to the owner/writer of the configuration file
    to ensure its security.
    """

    fractal_uri: str = Field(..., description="Full URI to the Fractal Server you want to connect to")
    username: Optional[str] = Field(
        None,
        description="Username to connect to the Fractal Server with. When not provided, a connection is attempted "
        "as a guest user, which in most default Servers will be unable to return results.",
    )
    password: Optional[str] = Field(
        None, description="Password to authenticate to the Fractal Server with (alongside the `username`)"
    )
    verify: Optional[bool] = Field(None, description="Use Server-side generated SSL certification or not.")

    class Config(BaseModel.Config):
        case_insensitive = True
        extra = "forbid"


class FractalComputeConfig(BaseModel):
    base_folder: str = Field(
        ...,
        description="The base folder to use as the default for some options (logs, etc). Default is the location of the config file.",
    )

    cluster: str = Field(
        ...,
        description="Name of this scheduler to present to the Fractal Server. Descriptive names help the server "
        "identify the manager resource and assists with debugging.",
    )
    loglevel: str = "INFO"
    logfile: Optional[str] = Field(
        None,
        description="Full path to save a log file to, including the filename. If not provided, information will still "
        "be reported to terminal, but not saved. When set, logger information is sent to this file.",
    )
    update_frequency: float = Field(
        30,
        description="Time between heartbeats/update checks between this Manager and the Fractal Server. The lower this "
        "value, the shorter the intervals. If you have an unreliable network connection, consider "
        "increasing this time as repeated, consecutive network failures will cause the Manager to shut "
        "itself down to maintain integrity between it and the Fractal Server. Units of seconds",
        gt=0,
    )
    update_frequency_jitter: float = Field(
        0.1,
        description="The update frequency will be modified by up to a certain amount for each request. The "
        "update_frequency_jitter represents a fraction of the update_frequency to allow as a max. "
        "Ie, update_frequency=60, and jitter=0.1, updates will happen between 54 and 66 seconds. "
        "This helps with spreading out server load.",
        ge=0,
    )

    max_idle_time: Optional[int] = Field(
        None,
        description="Maximum consecutive time in seconds that the manager "
        "should be allowed to run. If this is reached, the manager will shutdown.",
    )

    parsl_run_dir: str = "parsl_run_dir"
    parsl_usage_tracking: int = 0

    server: FractalServerSettings = Field(...)
    environments: PackageEnvironmentSettings = PackageEnvironmentSettings()
    executors: Dict[str, AllExecutorTypes] = Field(...)

    class Config(BaseModel.Config):
        case_insensitive = True
        extra = "forbid"

    @validator("logfile")
    def _check_logfile(cls, v, values):
        return _make_abs_path(v, values["base_folder"], None)

    @validator("parsl_run_dir")
    def _check_run_dir(cls, v, values):
        return _make_abs_path(v, values["base_folder"], "parsl_run_dir")

    @validator("update_frequency", "max_idle_time", pre=True)
    def _convert_durations(cls, v):
        return duration_to_seconds(v)


def read_configuration(file_paths: List[str], extra_config: Optional[Dict[str, Any]] = None) -> FractalComputeConfig:
    logger = logging.getLogger(__name__)
    config_data: Dict[str, Any] = {}

    # Read all the files, in order
    for path in file_paths:
        with open(path, "r") as yf:
            logger.info(f"Reading configuration data from {path}")
            file_data = yaml.safe_load(yf)
            update_nested_dict(config_data, file_data)

    if extra_config:
        update_nested_dict(config_data, extra_config)

    # Find the base folder
    # 1. If specified in the environment, use that
    # 2. Use any specified in a config file
    # 3. Use the path of the last (highest-priority) file given
    if "QCF_COMPUTE_BASE_FOLDER" in os.environ:
        base_dir = os.getenv("QCF_COMPUTE_BASE_FOLDER")
    elif config_data.get("base_folder") is not None:
        base_dir = config_data["base_folder"]
    elif len(file_paths) > 0:
        # use the location of the last file as the base directory
        base_dir = os.path.dirname(file_paths[-1])
    else:
        raise RuntimeError(
            "Base folder must be specified somehow. Maybe set QCF_COMPUTE_BASE_FOLDER in the environment?"
        )

    config_data["base_folder"] = os.path.abspath(base_dir)

    try:
        return FractalComputeConfig(**config_data)
    except Exception as e:
        if len(file_paths) == 0:
            raise RuntimeError(f"Could not assemble a working configuration from environment variables:\n{str(e)}")
        raise
