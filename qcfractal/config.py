"""
The global qcfractal config file specification.


"""

from __future__ import annotations
import urllib.parse
import os
import logging
from typing import Optional, Dict, Any
import yaml
from pydantic import Field, validator, root_validator, ValidationError

from .interface.models import AutodocBaseSettings


def update_nested_dict(d, u):
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = update_nested_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d


class ConfigCommon:
    case_insensitive = True
    extra = "forbid"


class ConfigBase(AutodocBaseSettings):

    _type_map = {"string": str, "integer": int, "float": float, "boolean": bool}

    @classmethod
    def field_names(cls):
        return list(cls.schema()["properties"].keys())

    @classmethod
    def help_info(cls, field):
        """
        Create 'help' information for use by argparse from a field in a settings class
        """
        info = cls.schema()["properties"][field]

        ret = {"type": cls._type_map[info["type"]]}

        # Don't add defaults here. Argparse would then end up using thses
        # defaults on the command line, overriding values specified in the config
        # if "default" in info:
        #     ret["default"] = info["default"]

        if "description" in info:
            ret["help"] = info["description"]

        return ret


class DatabaseConfig(ConfigBase):
    """
    Settings for the database used by QCFractal
    """

    base_folder: str = Field(
        ...,
        description="The base folder to use as the default for some options (logs, etc). Default is the location of the config file.",
    )

    host: str = Field(
        "localhost",
        description="The hostname and ip address the database is running on. If own = True, this must be localhost",
    )
    port: int = Field(
        5432,
        description="The port the database is running on. If own = True, a database will be started, binding to this port",
    )
    database_name: str = Field("qcfractal_default", description="The database name to connect to.")
    username: Optional[str] = Field(None, description="The database username to connect with")
    password: Optional[str] = Field(None, description="The database password to connect with")

    own: bool = Field(
        True,
        description="If True, QCFractal will control the database instance. If False, you must start and manage the database yourself",
    )

    data_directory: str = Field(
        None, description="Location to place the database if own == True. Default is [base_folder]/database"
    )
    logfile: str = Field(
        None,
        description="Path to a file to use as the database logfile (if own == True). Default is [base_folder]/qcfractal_database.log",
    )

    echo_sql: bool = Field(False, description="[ADVANCED] output raw SQL queries being run")
    skip_version_check: bool = Field(
        False,
        description="[ADVANCED] Do not check that the version of QCFractal matches that of the version of the database. ONLY RECOMMENDED FOR DEVELOPMENT PURPOSES",
    )
    pg_tool_dir: Optional[str] = Field(
        None,
        description="Directory containing Postgres tools such as psql and pg_ctl (ie, /usr/bin, or /usr/lib/postgresql/13/bin). If not specified, an attempt to find them will be made. This field is only required if autodetection fails and own == True",
    )

    pool_size: int = Field(
        5,
        description="[ADVANCED] set the size of the connection pool to use in SQLAlchemy. Set to zero to disable pooling",
    )

    class Config(ConfigCommon):
        env_prefix = "QCF_DB_"

    @validator("data_directory")
    def _check_data_directory(cls, v, values):
        if v is None:
            ret = os.path.join(values["base_folder"], "postgres")
        else:
            ret = v

        ret = os.path.expanduser(ret)
        return ret

    @validator("logfile")
    def _check_logfile(cls, v, values):
        if v is None:
            ret = os.path.join(values["base_folder"], "qcfractal_database.log")
        else:
            ret = v

        ret = os.path.expanduser(ret)
        return ret

    @property
    def uri(self):
        # Hostname can be a directory (unix sockets). But we need to escape some stuff
        host = urllib.parse.quote(self.host, safe="")
        username = self.username if self.username is not None else ""
        password = f":{self.password}" if self.password is not None else ""
        sep = "@" if username != "" or password != "" else ""
        return f"postgres://{username}{password}{sep}{host}:{self.port}/{self.database_name}"

    @property
    def safe_uri(self):
        host = urllib.parse.quote(self.host, safe="")
        username = self.username if self.username is not None else ""
        password = ":********" if self.password is not None else ""
        sep = "@" if username != "" or password != "" else ""
        return f"postgres://{username}{password}{sep}{host}:{self.port}/{self.database_name}"


class ResponseLimitConfig(ConfigBase):
    """
    Limits on the number of records returned per query. This can be specified per object (molecule, etc)
    """

    molecule: int = Field(5000, description="Limit on the number of molecules returned")
    output_store: int = Field(100, description="Limit on the number of program outputs returned")
    manager: int = Field(5000, description="Limit on the number of manager records to return")
    manager_log: int = Field(10000, description="Limit on the number of manager log records to return")
    result: int = Field(2000, description="Limit on the number of computation records to return")
    keyword: int = Field(1000, description="Limit on the number of keywords to return")
    collection: int = Field(25, description="Limit on the number of collections to return")
    task: int = Field(1000, description="Limit on the number of tasks to return")
    service: int = Field(1000, description="Limit on the number of service to return")
    manager_task: int = Field(100, description="Limit on the number of tasks a single manager can pull down")
    wavefunction: int = Field(25, description="Limit on the number of wavefunctions to return")
    server_logs: int = Field(25, description="Limit on the number of server log records to return")
    access_logs: int = Field(10000, description="Limit on the number of access log records to return")

    class Config(ConfigCommon):
        env_prefix = "QCF_RESPONSELIMIT_"


class FlaskConfig(ConfigBase):
    """
    Settings for the Flask REST interface
    """

    config_name: str = Field("production", description="Flask configuration to use (default, debug, production, etc)")
    num_workers: int = Field(1, description="Number of workers to spawn in Gunicorn")
    host: str = Field("127.0.0.1", description="The IP address or hostname to bind to")
    port: int = Field(7777, description="The port on which to run the REST interface.")

    class Config(ConfigCommon):
        env_prefix = "QCF_FLASK_"


class FractalConfig(ConfigBase):
    """
    Fractal Server settings
    """

    base_folder: str = Field(
        ...,
        description="The base directory to use as the default for some options (logs, views, etc). Default is the location of the config file.",
    )

    # Info for the REST interface
    name: str = Field("QCFractal Server", description="The QCFractal server name")

    enable_security: bool = Field(True, description="Enable user authentication and authorization")
    allow_unauthenticated_read: bool = Field(
        True,
        description="Allows unauthenticated read access to this instance. This does not extend to sensitive tables (such as user information)",
    )

    # Logging and profiling
    logfile: Optional[str] = Field(
        None,
        description="Path to a file to use for server logging. If not specified, logs will be printed to standard output",
    )
    loglevel: str = Field(
        "INFO", description="Level of logging to enable (debug, info, warning, error, critical). Case insensitive"
    )
    cprofile: Optional[str] = Field(
        None,
        description="Enable profiling via cProfile, and output cprofile data to this directory. Multiple files will be created",
    )

    # Periodics
    service_frequency: int = Field(60, description="The frequency to update services (in seconds)")
    max_active_services: int = Field(20, description="The maximum number of concurrent active services")
    heartbeat_frequency: int = Field(
        1800, description="The frequency (in seconds) to check the heartbeat of compute managers"
    )
    heartbeat_max_missed: int = Field(
        5,
        description="The maximum number of heartbeats that a compute manager can miss. If more are missed, the worker is considered dead",
    )

    # Access logging
    log_access: bool = Field(False, description="Store API access in the Database")
    geo_file_path: Optional[str] = Field(
        None,
        description="Geoip2 cites file path (.mmdb) for geolocating IP addresses. Defaults to [base_folder]/GeoLite2-City.mmdb. If this file is not available, geo-ip lookup will not be enabled",
    )

    # Settings for views
    enable_views: bool = Field(True, description="Enable frozen-views")
    views_directory: Optional[str] = Field(
        None, description="Location of frozen-view data. If None, defaults to [base_folder]/views"
    )

    # Other settings blocks
    database: DatabaseConfig = Field(..., description="Configuration of the settings for the database")
    flask: FlaskConfig = Field(..., description="Configuration of the REST interface")
    response_limits: ResponseLimitConfig = Field(..., description="Configuration of the limits to REST responses")

    @root_validator(pre=True)
    def _root_validator(cls, values):
        values.setdefault("database", dict())
        if "base_folder" not in values["database"]:
            values["database"]["base_folder"] = values.get("base_folder")

        values.setdefault("response_limits", dict())
        values.setdefault("flask", dict())
        return values

    @validator("views_directory")
    def _check_views_directory(cls, v, values):
        if v is None:
            ret = os.path.join(values["base_folder"], "views")
        else:
            ret = v

        ret = os.path.expanduser(ret)
        return ret

    @validator("geo_file_path")
    def _check_geo_file_path(cls, v, values):
        if v is None:
            ret = os.path.join(values["base_folder"], "GeoLite2-City.mmdb")
        else:
            ret = v

        ret = os.path.expanduser(ret)
        return ret

    @validator("loglevel")
    def _check_loglevel(cls, v):
        v = v.upper()
        if v not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValidationError(f"{v} is not a valid loglevel. Must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return v

    class Config(ConfigCommon):
        env_prefix = "QCF_"


def read_configuration(file_paths: list[str], extra_config: Optional[Dict[str, Any]] = None) -> FractalConfig:
    """
    Reads QCFractal configuration from YAML files

    This
    """
    logger = logging.getLogger(__name__)
    config_data: Dict[str, Any] = {}

    if len(file_paths) == 0:
        raise RuntimeError("Cannot read configurations without any file paths!")

    # Read all the files, in order
    for path in file_paths:
        with open(path, "r") as yf:
            logger.info(f"Reading configuration data from {path}")
            file_data = yaml.safe_load(yf)
            update_nested_dict(config_data, file_data)

    if extra_config:
        update_nested_dict(config_data, extra_config)

    # use the location of the last file as the base directory
    base_dir = os.path.dirname(file_paths[-1])

    # convert relative paths to full, absolute paths
    base_dir = os.path.abspath(base_dir)

    config_data["base_folder"] = base_dir

    # Handle an old configuration
    if "fractal" in config_data:
        logger.warning(f"Found old configuration format. Reading this format will be deprecated in the future.")
        raise RuntimeError("TODO")

    return FractalConfig(**config_data)
