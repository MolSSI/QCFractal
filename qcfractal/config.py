"""
The global qcfractal config file specification.
"""

from __future__ import annotations

import logging
import secrets
import os
import urllib.parse
from typing import Optional, Dict, Any

import yaml
from pydantic import BaseSettings, Field, validator, root_validator, ValidationError


def update_nested_dict(d, u):
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = update_nested_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d


class ConfigCommon:
    case_sensitive = False
    extra = "forbid"


class ConfigBase(BaseSettings):

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

    data_directory: Optional[str] = Field(
        None,
        description="Location to place the database if own == True. Default is [base_folder]/database if we own the databse",
    )
    logfile: str = Field(
        None,
        description="Path to a file to use as the database logfile (if own == True). Default is [base_folder]/qcfractal_database.log",
    )

    echo_sql: bool = Field(False, description="[ADVANCED] output raw SQL queries being run")
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
        if v is None and values["own"] is False:
            return None
        elif v is None and values["own"] is True:
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
        return f"postgresql://{username}{password}{sep}{host}:{self.port}/{self.database_name}"

    @property
    def safe_uri(self):
        host = urllib.parse.quote(self.host, safe="")
        username = self.username if self.username is not None else ""
        password = ":********" if self.password is not None else ""
        sep = "@" if username != "" or password != "" else ""
        return f"postgresql://{username}{password}{sep}{host}:{self.port}/{self.database_name}"


class AutoResetConfig(ConfigBase):
    """
    Limits on the number of records returned per query. This can be specified per object (molecule, etc)
    """

    enabled: bool = Field(False, description="Enable/disable automatic restart. True = enabled")
    unknown_error: int = Field(2, description="Max restarts for unknown errors")
    compute_lost: int = Field(5, description="Max restarts for computations where the compute resource disappeared")
    random_error: int = Field(5, description="Max restarts for random errors")

    class Config(ConfigCommon):
        env_prefix = "QCF_AUTORESET_"


class APILimitConfig(ConfigBase):
    """
    Limits on the number of records returned per query. This can be specified per object (molecule, etc)
    """

    get_records: int = Field(1000, description="Number of calculation records that can be retrieved")
    add_records: int = Field(500, description="Number of calculation records that can be added")

    get_dataset_entries: int = Field(2000, description="Number of dataset entries that can be retrieved")

    get_molecules: int = Field(1000, description="Number of molecules that can be retrieved")
    add_molecules: int = Field(1000, description="Number of molecules that can be added")

    get_managers: int = Field(1000, description="Number of manager records to return")

    manager_tasks_claim: int = Field(200, description="Number of tasks a single manager can pull down")
    manager_tasks_return: int = Field(10, description="Number of tasks a single manager can return at once")

    get_server_stats: int = Field(25, description="Number of server statistics records to return")
    get_access_logs: int = Field(1000, description="Number of access log records to return")
    get_error_logs: int = Field(100, description="Number of error log records to return")

    class Config(ConfigCommon):
        env_prefix = "QCF_APILIMIT_"


class WebAPIConfig(ConfigBase):
    """
    Settings for the Web API (api) interface
    """

    config_name: str = Field("production", description="Flask configuration to use (default, debug, production, etc)")
    num_workers: int = Field(1, description="Number of workers to spawn in Gunicorn")
    worker_timeout: int = Field(
        120,
        description="If the master process does not hear from a worker for the given amount of time (in seconds),"
        "kill it. This effectively limits the time a worker has to respond to a request",
    )
    host: str = Field("127.0.0.1", description="The IP address or hostname to bind to")
    port: int = Field(7777, description="The port on which to run the REST interface.")

    secret_key: str = Field("default_key_PLEASE_CHANGE_ME", description="Secret key for flask api. See documentation")
    jwt_secret_key: str = Field(
        "default_key_PLEASE_CHANGE_ME", description="Secret key for web tokens. See documentation"
    )
    jwt_access_token_expires: int = Field(
        60 * 60 * 24 * 7, description="The time (in seconds) an access token is valid for. Default is 1 week"
    )
    jwt_refresh_token_expires: int = Field(
        60 * 60 * 24 * 30, description="The time (in seconds) a refresh token is valid for. Default is 30 days"
    )
    keepalive: int = Field(5, description="Time (in seconds) to wait for requests from a Keep-Alive connection")

    class Config(ConfigCommon):
        env_prefix = "QCF_API_"


class FractalConfig(ConfigBase):
    """
    Fractal Server settings
    """

    base_folder: str = Field(
        ...,
        description="The base directory to use as the default for some options (logs, etc). Default is the location of the config file.",
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

    hide_internal_errors: bool = Field(
        True,
        description="If True, internal errors will only be reported as an error "
        "number to the user. If False, the entire error/backtrace "
        "will be sent (which could rarely contain sensitive info). "
        "In either case, errors will be stored in the database",
    )

    # Periodics
    statistics_frequency: int = Field(
        3600, description="The frequency at which to update servre statistics (in seconds)"
    )
    service_frequency: int = Field(60, description="The frequency at which to update services (in seconds)")
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

    # Other settings blocks
    database: DatabaseConfig = Field(..., description="Configuration of the settings for the database")
    api: WebAPIConfig = Field(..., description="Configuration of the REST interface")
    api_limits: APILimitConfig = Field(..., description="Configuration of the limits to the api")
    auto_reset: AutoResetConfig = Field(..., description="Configuration for automatic resetting of tasks")

    @root_validator(pre=True)
    def _root_validator(cls, values):
        values.setdefault("database", dict())
        if "base_folder" not in values["database"]:
            values["database"]["base_folder"] = values.get("base_folder")

        values.setdefault("api_limits", dict())
        values.setdefault("api", dict())
        values.setdefault("auto_reset", dict())
        return values

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


def convert_old_configuration(old_config):
    cfg_dict = {}

    cfg_dict["base_folder"] = old_config.base_folder

    # Database settings
    cfg_dict["database"] = {}
    cfg_dict["database"]["own"] = old_config.database.own
    cfg_dict["database"]["host"] = old_config.database.host
    cfg_dict["database"]["port"] = old_config.database.port
    cfg_dict["database"]["username"] = old_config.database.username
    cfg_dict["database"]["password"] = old_config.database.password
    cfg_dict["database"]["database_name"] = old_config.database.database_name
    cfg_dict["database"]["logfile"] = old_config.database.logfile

    if old_config.database.own:
        cfg_dict["database"]["data_directory"] = old_config.database.directory

    # Response limits. The old config only had one. Set all the possible
    # limits to that value
    response_limit = old_config.fractal.query_limit
    field_list = APILimitConfig.field_names()
    cfg_dict["api_limits"] = {k: response_limit for k in field_list}

    # Flask server settings
    cfg_dict["api"] = {}
    cfg_dict["api"]["port"] = old_config.fractal.port

    # Now general fractal settings. Before these were in a
    # separate config class, but now they are in the top level
    cfg_dict["name"] = old_config.fractal.name
    cfg_dict["enable_security"] = old_config.fractal.security == "local"
    cfg_dict["allow_unauthenticated_read"] = old_config.fractal.allow_read
    cfg_dict["logfile"] = old_config.fractal.logfile
    cfg_dict["loglevel"] = old_config.fractal.loglevel
    cfg_dict["service_frequency"] = old_config.fractal.service_frequency
    cfg_dict["max_active_services"] = old_config.fractal.max_active_services
    cfg_dict["heartbeat_frequency"] = old_config.fractal.heartbeat_frequency
    cfg_dict["log_access"] = old_config.fractal.log_apis
    cfg_dict["geo_file_path"] = old_config.fractal.geo_file_path

    return FractalConfig(**cfg_dict)


def read_configuration(file_paths: list[str], extra_config: Optional[Dict[str, Any]] = None) -> FractalConfig:
    """
    Reads QCFractal configuration from YAML files
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

    if config_data.get("base_folder") is None:
        config_data["base_folder"] = base_dir

    # Handle an old configuration
    if "fractal" in config_data:
        raise RuntimeError("Found an old configuration. Please migrate with qcfractal-server upgrade-config")

    return FractalConfig(**config_data)


def write_initial_configuration(file_path: str, full_config: bool = True):
    base_folder = os.path.dirname(file_path)
    default_config = FractalConfig(base_folder=base_folder)

    # Generate two secret keys for flask/jwt
    default_config.api.secret_key = secrets.token_urlsafe(32)
    default_config.api.jwt_secret_key = secrets.token_urlsafe(32)

    include = None
    if not full_config:
        include = {
            "name": True,
            "enable_security": True,
            "log_access": True,
            "allow_unauthenticated_read": True,
            "logfile": True,
            "loglevel": True,
            "service_frequency": True,
            "statistics_frequency": True,
            "max_active_services": True,
            "heartbeat_frequency": True,
            "database": {"own", "host", "port", "database_name", "base_folder"},
            "api": {"secret_key", "jwt_secret_key", "host", "port"},
        }

    with open(file_path, "x") as f:
        yaml.dump(default_config.dict(include=include), f, sort_keys=False)
