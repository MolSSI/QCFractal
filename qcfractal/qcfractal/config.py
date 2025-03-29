"""
The global qcfractal config file specification.
"""

from __future__ import annotations

import logging
import os
import secrets
import tempfile
from typing import Optional, Dict, Union, List, Any

import yaml
from psycopg2.extensions import make_dsn, parse_dsn

try:
    from pydantic.v1 import BaseSettings, Field, validator, root_validator, ValidationError
    from pydantic.v1.env_settings import SettingsSourceCallable
except ImportError:
    from pydantic import BaseSettings, Field, validator, root_validator, ValidationError
    from pydantic.env_settings import SettingsSourceCallable
from sqlalchemy.engine.url import URL, make_url

from qcfractal.port_util import find_open_port
from qcportal.utils import duration_to_seconds, update_nested_dict


def _make_abs_path(path: Optional[str], base_folder: str, default_filename: Optional[str]) -> Optional[str]:
    # No path specified, no default
    if path is None and default_filename is None:
        return None

    # Path isn't specified, but default is given
    if path is None:
        path = default_filename

    path = os.path.expanduser(path)
    if os.path.isabs(path):
        return path
    else:
        path = os.path.join(base_folder, path)
        return os.path.abspath(path)


def make_uri_string(
    host: Optional[str],
    port: Optional[Union[int, str]],
    username: Optional[str],
    password: Optional[str],
    dbname: Optional[str],
    query: Optional[Dict[str, str]],
) -> str:
    username = username if username is not None else ""
    password = ":" + password if password is not None else ""
    sep = "@" if username != "" or password != "" else ""
    query_str = "" if query is None else "&".join(f"{k}={v}" for k, v in query.items())

    # If this is a socket file, move the host to the query params
    if host.startswith("/"):
        query_str = "&" + query_str if query_str != "" else ""
        return f"postgresql://{username}{password}{sep}:{port}/{dbname}?host={host}{query_str}"
    else:
        query_str = "?" + query_str if query_str != "" else ""
        return f"postgresql://{username}{password}{sep}{host}:{port}/{dbname}{query_str}"


class ConfigCommon:
    case_sensitive = False
    extra = "forbid"

    # Forces environment variables to take precedent over values
    # passed to init (which usually come from a file)
    @classmethod
    def customise_sources(
        cls,
        init_settings: SettingsSourceCallable,
        env_settings: SettingsSourceCallable,
        file_secret_settings: SettingsSourceCallable,
    ) -> tuple[SettingsSourceCallable, ...]:
        return env_settings, init_settings, file_secret_settings


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

    full_uri: Optional[str] = Field(
        None, description="Full connection URI. This overrides host,username,password,port, etc"
    )

    host: str = Field(
        "localhost",
        description="The hostname or ip address the database is running on. If own = True, this must be localhost. May also be a path to a directory containing the database socket file",
    )
    port: int = Field(
        5432,
        description="The port the database is running on. If own = True, a database will be started, binding to this port",
    )
    database_name: str = Field("qcfractal_default", description="The database name to connect to.")
    username: str = Field(..., description="The database username to connect with")
    password: str = Field(..., description="The database password to connect with")
    query: Dict[str, str] = Field({}, description="Extra connection query parameters at the end of the URL string")

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

    maintenance_db: str = Field(
        "postgres",
        description="[ADVANCED] An existing database (not the one you want to use/create). This is used for database management",
    )

    class Config(ConfigCommon):
        env_prefix = "QCF_DB_"

    @validator("data_directory")
    def _check_data_directory(cls, v, values):
        if values["own"] is True:
            return _make_abs_path(v, values["base_folder"], "postgres")
        else:
            return None

    @validator("logfile")
    def _check_logfile(cls, v, values):
        return _make_abs_path(v, values["base_folder"], "qcfractal_database.log")

    @property
    def database_uri(self) -> str:
        """
        Returns the real database URI as a string

        It does not hide the password, so is not suitable for logging
        """
        if self.full_uri is not None:
            return self.full_uri
        else:
            return make_uri_string(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                dbname=self.database_name,
                query=self.query,
            )

    @property
    def sqlalchemy_url(self) -> URL:
        """Returns the SQLAlchemy URL for this database"""

        url = make_url(self.database_uri)
        return url.set(drivername="postgresql+psycopg2")

    @property
    def psycopg2_dsn(self) -> str:
        """
        Returns a string suitable for use as a psycopg2 connection string
        """
        dsn_dict = parse_dsn(self.database_uri)
        return make_dsn(**dsn_dict)

    @property
    def psycopg2_maintenance_dsn(self) -> str:
        dsn_dict = parse_dsn(self.database_uri)
        dsn_dict["dbname"] = self.maintenance_db
        return make_dsn(**dsn_dict)

    @property
    def safe_uri(self) -> str:
        """
        Returns a user-readable version of the URI for logging, etc.
        """

        dsn = parse_dsn(self.database_uri)

        host = dsn.pop("host")
        port = dsn.pop("port", None)
        user = dsn.pop("user", None)
        password = dsn.pop("password", None)
        dbname = dsn.pop("dbname")

        # SQLAlchemy render_string has some problems sometimes, so use our own
        return make_uri_string(
            host=host, port=port, username=user, password="********" if password else None, dbname=dbname, query=dsn
        )  # everything left over


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

    get_access_logs: int = Field(1000, description="Number of access log records to return")
    get_error_logs: int = Field(100, description="Number of error log records to return")
    get_internal_jobs: int = Field(1000, description="Number of internal jobs to return")

    class Config(ConfigCommon):
        env_prefix = "QCF_APILIMIT_"


class WebAPIConfig(ConfigBase):
    """
    Settings for the Web API (api) interface
    """

    num_threads_per_worker: int = Field(4, description="Number of threads per worker")
    worker_timeout: int = Field(
        120,
        description="If the master process does not hear from a worker for the given amount of time (in seconds),"
        "kill it. This effectively limits the time a worker has to respond to a request",
    )
    host: str = Field("localhost", description="The IP address or hostname to bind to")
    port: int = Field(7777, description="The port on which to run the REST interface.")

    secret_key: str = Field(..., description="Secret key for flask api. See documentation")
    jwt_secret_key: str = Field(..., description="Secret key for web tokens. See documentation")
    jwt_access_token_expires: int = Field(
        60 * 60, description="The time (in seconds) an access token is valid for. Default is 1 hour"
    )
    jwt_refresh_token_expires: int = Field(
        60 * 60 * 24, description="The time (in seconds) a refresh token is valid for. Default is 1 day"
    )
    user_session_max_age: int = Field(
        60 * 60 * 24, description="The time (in seconds) that a user session can be idle (for browser-based sessions)"
    )
    user_session_cookie_name: str = Field(
        "qcf_session", description="Name to use for a session cookie (for browser-based sessions)"
    )
    user_session_cookie_domain: Optional[str] = Field(
        None, description="Domain to use for the user-session cookie (for browser-based sessions)"
    )
    user_session_cookie_samesite: Optional[str] = Field(
        None, description="Set the SameSite flag for the user-session cookie (for browser-based sessions)"
    )
    user_session_cookie_partitioned: bool = Field(
        False, description="Use the Partitioned flag for the user-session cookie (for browser-based sessions)"
    )
    user_session_cookie_secure: bool = Field(
        False, description="Use Secure flag for the user-session cookie (for browser-based sessions)"
    )
    user_session_cookie_httponly: bool = Field(
        False, description="Use Secure flag for the user-session cookie (for browser-based sessions)"
    )

    extra_flask_options: Optional[Dict[str, Any]] = Field(
        None, description="Any additional options to pass directly to flask"
    )
    extra_waitress_options: Optional[Dict[str, Any]] = Field(
        None, description="Any additional options to pass directly to the waitress serve function"
    )

    @validator(
        "jwt_access_token_expires",
        "jwt_refresh_token_expires",
        "user_session_max_age",
        pre=True,
    )
    def _convert_durations(cls, v):
        return duration_to_seconds(v)

    class Config(ConfigCommon):
        env_prefix = "QCF_API_"


class S3BucketMap(ConfigBase):
    dataset_attachment: str = Field("dataset_attachment", description="Bucket to hold dataset views")


class S3Config(ConfigBase):
    """
    Settings for using external files with S3
    """

    enabled: bool = False
    verify: bool = True
    passthrough: bool = False
    endpoint_url: Optional[str] = Field(None, description="S3 endpoint URL")
    access_key_id: Optional[str] = Field(None, description="AWS/S3 access key")
    secret_access_key: Optional[str] = Field(None, description="AWS/S3 secret key")

    bucket_map: S3BucketMap = Field(S3BucketMap(), description="Configuration for where to store various files")

    class Config(ConfigCommon):
        env_prefix = "QCF_S3_"

    @root_validator()
    def _check_enabled(cls, values):
        if values.get("enabled", False) is True:
            for key in ["endpoint_url", "access_key_id", "secret_access_key"]:
                if values.get(key, None) is None:
                    raise ValueError(f"S3 enabled but {key} not set")

        return values


class CORSconfig(ConfigBase):
    """
    Settings for using CORS
    """

    enabled: bool = False
    origins: List[str] = Field([])
    supports_credentials: bool = False
    headers: List[str] = Field([])
    methods: List[str] = Field([])


class FractalConfig(ConfigBase):
    """
    Fractal Server settings
    """

    base_folder: str = Field(
        ...,
        description="The base directory to use as the default for some options (logs, etc). Default is the location of the config file.",
    )

    temporary_dir: Optional[str] = Field(
        None,
        description="Temporary directory to use for things such as view creation. If None, uses system default. This may require a lot of space!",
    )

    # Info for the REST interface
    name: str = Field("QCFractal Server", description="The QCFractal server name")

    enable_security: bool = Field(True, description="Enable user authentication and authorization")
    allow_unauthenticated_read: bool = Field(
        True,
        description="Allows unauthenticated read access to this instance. This does not extend to sensitive tables (such as user information)",
    )
    strict_queue_tags: bool = Field(
        False,
        description="If True, disables wildcard behavior for queue tags. This disables managers from claiming all "
        "tags if they specify a wildcard ('*') tag. Managers will still be able to claim tasks with an "
        "explicit '*' tag if they specifiy the '*' queue tag in their config",
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
    service_frequency: int = Field(60, description="The frequency at which to update services (in seconds)")
    max_active_services: int = Field(20, description="The maximum number of concurrent active services")
    heartbeat_frequency: int = Field(
        1800,
        description="The frequency (in seconds) to check the heartbeat of compute managers",
        gt=0,
    )
    heartbeat_frequency_jitter: int = Field(
        0.1, description="Jitter fraction to be applied to the heartbeat frequency", ge=0
    )
    heartbeat_max_missed: int = Field(
        5,
        description="The maximum number of heartbeats that a compute manager can miss. If more are missed, the worker is considered dead",
        ge=0,
    )

    # Access logging
    log_access: bool = Field(False, description="Store API access in the database")
    access_log_keep: int = Field(
        0, description="How far back to keep access logs (in days or as a duration string). 0 means keep all"
    )

    # maxmind_account_id: Optional[int] = Field(None, description="Account ID for MaxMind GeoIP2 service")
    maxmind_license_key: Optional[str] = Field(
        None,
        description="License key for MaxMind GeoIP2 service. If provided, the GeoIP2 database will be downloaded and updated automatically",
    )

    geoip2_dir: Optional[str] = Field(
        None,
        description="Directory containing the Maxmind GeoIP2 Cities file (GeoLite2-City.mmdb) Defaults to [base_folder]/geoip2. This directory will be created if needed.",
    )

    geoip2_filename: str = Field(
        "GeoLite2-City.mmdb", description="Filename of the Maxmind GeoIP2 Cities file (GeoLite2-City.mmdb)"
    )

    # Internal jobs
    internal_job_processes: int = Field(
        1, description="Number of processes for processing internal jobs and async requests"
    )
    internal_job_keep: int = Field(
        0, description="How far back to keep finished internal jobs (in days or as a duration string). 0 means keep all"
    )

    # Homepage settings
    homepage_redirect_url: Optional[str] = Field(None, description="Redirect to this URL when going to the root path")
    homepage_directory: Optional[str] = Field(None, description="Use this directory to serve the homepage")

    # Other settings blocks
    database: DatabaseConfig = Field(..., description="Configuration of the settings for the database")
    api: WebAPIConfig = Field(..., description="Configuration of the REST interface")
    s3: S3Config = Field(S3Config(), description="Configuration of the S3 file storage (optional)")
    api_limits: APILimitConfig = Field(..., description="Configuration of the limits to the api")
    cors: CORSconfig = Field(..., description="Configuration Cross Origin Resource sharing (advanced)")
    auto_reset: AutoResetConfig = Field(..., description="Configuration for automatic resetting of tasks")

    @root_validator(pre=True)
    def _root_validator(cls, values):
        logger = logging.getLogger("config_validation")

        values.setdefault("database", dict())
        if "base_folder" not in values["database"]:
            values["database"]["base_folder"] = values.get("base_folder")

        values.setdefault("api_limits", dict())
        values.setdefault("api", dict())
        values.setdefault("auto_reset", dict())
        values.setdefault("cors", dict())

        if "statistics_frequency" in values:
            values.pop("statistics_frequency")
            logger.warning("The 'statistics_frequency' setting is no longer used and is now ignored")

        if "num_workers" in values["api"]:
            values["api"].pop("num_workers")
            logger.warning("The 'num_workers' setting is no longer used and is now ignored")

        if "get_server_stats" in values["api_limits"]:
            values["api_limits"].pop("get_server_stats")
            logger.warning("The 'get_server_stats' setting in 'api_limits' is no longer used and is now ignored")

        return values

    @validator("geoip2_dir")
    def _check_geoip2_dir(cls, v, values):
        return _make_abs_path(v, values["base_folder"], "geoip2")

    @validator("homepage_directory")
    def _check_hompepage_directory_path(cls, v, values):
        return _make_abs_path(v, values["base_folder"], None)

    @validator("logfile")
    def _check_logfile_path(cls, v, values):
        return _make_abs_path(v, values["base_folder"], None)

    @validator("loglevel")
    def _check_loglevel(cls, v):
        v = v.upper()
        if v not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValidationError(f"{v} is not a valid loglevel. Must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return v

    @validator("service_frequency", "heartbeat_frequency", pre=True)
    def _convert_durations(cls, v):
        return duration_to_seconds(v)

    @validator("access_log_keep", "internal_job_keep", pre=True)
    def _convert_durations_days(cls, v):
        if isinstance(v, int) or (isinstance(v, str) and v.isdigit()):
            return int(v) * 86400
        return duration_to_seconds(v)

    @validator("temporary_dir", pre=True)
    def _create_temporary_directory(cls, v, values):
        v = _make_abs_path(v, values["base_folder"], tempfile.gettempdir())

        if v is not None and not os.path.exists(v):
            os.makedirs(v)

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
    cfg_dict["api"]["secret_key"] = secrets.token_urlsafe(32)
    cfg_dict["api"]["jwt_secret_key"] = secrets.token_urlsafe(32)

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

    if old_config.fractal.geo_file_path:
        cfg_dict["geoip2_dir"] = os.path.basename(old_config.fractal.geo_file_path)

    return FractalConfig(**cfg_dict)


def read_configuration(file_paths: list[str], extra_config: Optional[Dict[str, Any]] = None) -> FractalConfig:
    """
    Reads QCFractal configuration from YAML files
    """
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
    if "QCF_BASE_FOLDER" in os.environ:
        base_dir = os.getenv("QCF_BASE_FOLDER")
    elif config_data.get("base_folder") is not None:
        base_dir = config_data["base_folder"]
    elif len(file_paths) > 0:
        # use the location of the last file as the base directory
        base_dir = os.path.dirname(file_paths[-1])
    else:
        raise RuntimeError("Base folder must be specified somehow. Maybe set QCF_BASE_FOLDER in the environment?")

    config_data["base_folder"] = os.path.abspath(base_dir)

    # Handle an old configuration
    if "fractal" in config_data:
        raise RuntimeError("Found an old configuration. Please migrate with qcfractal-server upgrade-config")

    # Pydantic will handle reading from environment variables
    # See if it can assemble a config. If there was a problem, and no
    # config files specified, mention that
    try:
        return FractalConfig(**config_data)
    except Exception as e:
        if len(file_paths) == 0:
            raise RuntimeError(f"Could not assemble a working configuration from environment variables:\n{str(e)}")
        raise


def write_initial_configuration(file_path: str, full_config: bool = True):
    base_folder = os.path.dirname(file_path)

    # Generate two secret keys for flask/jwt
    secret_key = secrets.token_urlsafe(32)
    jwt_secret_key = secrets.token_urlsafe(32)

    db_config = {
        "username": "qcfractal",
        "password": secrets.token_urlsafe(32),
    }

    default_config = FractalConfig(
        base_folder=base_folder, api={"secret_key": secret_key, "jwt_secret_key": jwt_secret_key}, database=db_config
    )

    default_config.database.port = find_open_port(starting_port=5432)
    default_config.api.port = find_open_port(starting_port=7777)

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
            "max_active_services": True,
            "heartbeat_frequency": True,
            "database": {"own", "host", "port", "database_name", "base_folder", "username", "password"},
            "api": {"secret_key", "jwt_secret_key", "host", "port"},
        }

    with open(file_path, "x") as f:
        yaml.dump(default_config.dict(include=include), f, sort_keys=False)
