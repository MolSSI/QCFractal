"""
The global qcfractal config file specification.
"""

import logging
import os
import secrets
import tempfile
from typing import Any, Annotated

import yaml
from psycopg2.extensions import make_dsn, parse_dsn
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationError, ConfigDict, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource
from sqlalchemy.engine.url import URL, make_url

from qcfractal.port_util import find_open_port
from qcportal.utils import duration_to_seconds, update_nested_dict


def _make_abs_path(path: str | None, base_folder: str, default_filename: str | None) -> str | None:
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
    host: str | None,
    port: int | str | None,
    username: str | None,
    password: str | None,
    dbname: str | None,
    query: dict[str, str] | None,
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


class QCFConfigBase(BaseModel):
    model_config = ConfigDict(extra="forbid", use_attribute_docstrings=True)


class DatabaseConfig(QCFConfigBase):
    """
    Settings for the database used by QCFractal
    """

    base_folder: str
    """The base folder to use as the default for some options (logs, etc). Default is the location of the config
    file.
    """

    full_uri: str | None = None
    """Full connection URI. This overrides host,username,password,port, etc"""

    host: str = "localhost"
    """The hostname or ip address the database is running on. If own = True, this must be localhost. May also be a
    path to a directory containing the database socket file
    """

    port: int = 5432
    """The port the database is running on. If own = True, a database will be started, binding to this port"""

    database_name: str = "qcfractal_default"
    """The database name to connect to."""

    username: str
    """The database username to connect with"""

    password: str
    """The database password to connect with"""

    query: dict[str, str | int] = {}
    """Extra connection query parameters at the end of the URL string"""

    own: bool = True
    """If True, QCFractal will control the database instance. If False, you must start and manage the database
    yourself
    """

    data_directory: str | None = None
    """Location to place the database if own == True. Default is [base_folder]/database if we own the database"""

    logfile: str | None = None
    """Path to a file to use as the database logfile (if own == True). Default is
    [base_folder]/qcfractal_database.log
    """

    echo_sql: bool = False
    """[ADVANCED] output raw SQL queries being run"""

    pg_tool_dir: str | None = None
    """Directory containing Postgres tools such as psql and pg_ctl (ie, /usr/bin, or /usr/lib/postgresql/13/bin). If
    not specified, an attempt to find them will be made. This field is only required if autodetection fails and
    own == True
    """

    pool_size: int = 5
    """[ADVANCED] set the size of the connection pool to use in SQLAlchemy. Set to zero to disable pooling"""

    maintenance_db: str = "postgres"
    """[ADVANCED] An existing database (not the one you want to use/create). This is used for database management"""

    @model_validator(mode="after")
    def _check_paths(self):
        if self.own:
            self.data_directory = _make_abs_path(self.data_directory, self.base_folder, "postgres")
            self.logfile = _make_abs_path(self.logfile, self.base_folder, "qcfractal_database.log")
        return self

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


class AutoResetConfig(QCFConfigBase):
    """
    How many times the server will automatically retry a failed computation
    """

    enabled: bool = False
    """Whether to automatically reset errored records at all.

    With this disabled, every errored record waits for someone to reset it by hand.
    """

    unknown_error: int = 2
    """Maximum automatic restarts for errors that could not be classified."""

    compute_lost: int = 5
    """Maximum automatic restarts for computations whose compute resource disappeared.

    This covers the ordinary ways a batch job ends without reporting back - a walltime kill,
    a preempted node, a manager killed mid-task. These failures say nothing about whether the computation
    itself is sound, which is why the default is more generous than for
    ``unknown_error``.
    """

    random_error: int = 5
    """Maximum automatic restarts for errors the server recognises as intermittent."""


class APILimitConfig(QCFConfigBase):
    """
    Limits on the number of records returned per query. This can be specified per object (molecule, etc)
    """

    get_records: int = 1000
    """Number of calculation records that can be retrieve in a single request"""

    add_records: int = 500
    """Number of calculation records that can be added in a single request"""

    get_dataset_entries: int = 2000
    """Number of dataset entries that can be retrieved in a single request"""

    get_molecules: int = 1000
    """Number of molecules that can be retrieved in a single request"""

    add_molecules: int = 1000
    """Number of molecules that can be added in a single request"""

    get_managers: int = 1000
    """Number of manager records to return"""

    manager_tasks_claim: int = 200
    """Number of tasks a single manager can pull down"""

    manager_tasks_return: int = 10
    """Number of tasks a single manager can return at once"""

    get_access_logs: int = 1000
    """Number of access log records to return"""

    get_error_logs: int = 100
    """Number of error log records to return"""

    get_internal_jobs: int = 1000
    """Number of internal jobs to return"""


class WebAPIConfig(QCFConfigBase):
    """
    Settings for the Web API (api) interface
    """

    num_threads_per_worker: int = 4
    """Number of threads per worker"""

    worker_timeout: int = 120
    """If the master process does not hear from a worker for the given amount of time (in seconds),kill it. This
    effectively limits the time a worker has to respond to a request
    """

    host: str = "localhost"
    """The IP address or hostname to bind to"""

    port: int = 7777
    """The port on which to run the REST interface."""

    secret_key: str
    """Secret key for flask api. See documentation"""

    jwt_secret_key: str
    """Secret key for web tokens. See documentation"""

    jwt_access_token_expires: int = 60 * 60
    """The time (in seconds) an access token is valid for. Default is 1 hour"""

    jwt_refresh_token_expires: int = 60 * 60 * 24
    """The time (in seconds) a refresh token is valid for. Default is 1 day"""

    user_session_max_age: int = 60 * 60 * 24
    """The time (in seconds) that a user session can be idle (for browser-based sessions)"""

    user_session_cookie_name: str = "qcf_session"
    """Name to use for a session cookie (for browser-based sessions)"""

    user_session_cookie_domain: str | None = None
    """Domain to use for the user-session cookie (for browser-based sessions)"""

    user_session_cookie_samesite: str | None = None
    """Set the SameSite flag for the user-session cookie (for browser-based sessions)"""

    user_session_cookie_partitioned: bool = False
    """Use the Partitioned flag for the user-session cookie (for browser-based sessions)"""

    user_session_cookie_secure: bool = False
    """Use Secure flag for the user-session cookie (for browser-based sessions)"""

    user_session_cookie_httponly: bool = False
    """Use Secure flag for the user-session cookie (for browser-based sessions)"""

    extra_flask_options: dict[str, Any] | None = None
    """Any additional options to pass directly to flask"""

    extra_waitress_options: dict[str, Any] | None = None
    """Any additional options to pass directly to the waitress serve function"""

    @field_validator(
        "jwt_access_token_expires",
        "jwt_refresh_token_expires",
        "user_session_max_age",
        mode="before",
    )
    @classmethod
    def _convert_durations(cls, v):
        return duration_to_seconds(v)


# S3 bucket names are all lowercase characters and numbers
S3BucketName = Annotated[str, StringConstraints(min_length=3, max_length=63, pattern=r"^[a-z0-9\-]+[a-z0-9]$")]

class S3BucketMap(QCFConfigBase):
    dataset_attachment: S3BucketName = "dataset-attachments"
    """Bucket to hold dataset views"""

    project_attachment: S3BucketName = "project-attachments"
    """Bucket to hold project attachments"""


class S3Config(QCFConfigBase):
    """
    Settings for using external files with S3
    """

    enabled: bool = False
    """Whether to store large external files (dataset views, attachments) in S3.

    When enabled, ``endpoint_url``, ``access_key_id`` and ``secret_access_key`` are all
    required; the server refuses to start otherwise.
    """

    verify: bool = True
    """Verify TLS certificates when connecting to S3."""

    passthrough: bool = False
    """Whether clients may download directly from the S3 endpoint.

    With this off, file contents are proxied through the server, so clients never need
    to reach S3 themselves.
    """

    endpoint_url: str | None = None
    """S3 endpoint URL"""

    access_key_id: str | None = None
    """AWS/S3 access key"""

    secret_access_key: str | None = None
    """AWS/S3 secret key"""

    auto_create_buckets: bool = False
    """Create the buckets named in ``bucket_map`` at startup if they do not already exist."""

    bucket_map: S3BucketMap = Field(default_factory=S3BucketMap)
    """Configuration for where to store various files"""

    @model_validator(mode="after")
    def _check_enabled(self):
        if self.enabled:
            for key in ["endpoint_url", "access_key_id", "secret_access_key"]:
                if getattr(self, key) is None:
                    raise ValueError(f"S3 enabled but {key} not set")
        return self


class CORSconfig(QCFConfigBase):
    """
    Settings for using CORS
    """

    enabled: bool = False
    """Whether to send CORS headers at all. With this off the other options here do nothing."""

    origins: list[str] = Field([])
    """Origins permitted to make cross-origin requests. Use ``["*"]`` to allow any origin."""

    supports_credentials: bool = False
    """Whether cross-origin requests may carry credentials (cookies, authorization headers)."""

    headers: list[str] = Field([])
    """Request headers a cross-origin request is allowed to set, such as ``Authorization``."""

    methods: list[str] = Field([])
    """HTTP methods permitted for cross-origin requests. Empty means the CORS default."""


class FractalConfig(BaseSettings):
    """
    Fractal Server settings
    """

    base_folder: str
    """The base directory to use as the default for some options (logs, etc). Default is the location of the config
    file.
    """

    temporary_dir: str | None = None
    """Temporary directory to use for things such as view creation. If None, uses system default. This may require a
    lot of space!
    """

    # Info for the REST interface
    name: str = "QCFractal Server"
    """The QCFractal server name"""

    enable_security: bool = True
    """Enable user authentication and authorization"""

    allow_unauthenticated_read: bool = True
    """Allows unauthenticated read access to this instance. This does not extend to sensitive tables (such as user
    information)
    """

    strict_compute_tags: bool = False
    """If True, disables wildcard behavior for compute tags. This disables managers from claiming all tags if they
    specify a wildcard ('*') tag. Managers will still be able to claim tasks with an explicit '*' tag if they
    specify the '*' queue tag in their config
    """

    # Logging and profiling
    logfile: str | None = None
    """Path to a file to use for server logging. If not specified, logs will be printed to standard output"""

    loglevel: str = "INFO"
    """Level of logging to enable (debug, info, warning, error, critical). Case insensitive"""

    hide_internal_errors: bool = True
    """If True, internal errors will only be reported as an error number to the user. If False, the entire
    error/backtrace will be sent (which could rarely contain sensitive info). In either case, errors will be
    stored in the database
    """

    # Periodics
    service_frequency: int = 60
    """The frequency at which to update services (in seconds)"""

    max_active_services: int = 20
    """The maximum number of concurrent active services"""

    heartbeat_frequency: int = Field(1800, gt=0)
    """The frequency (in seconds) to check the heartbeat of compute managers"""

    heartbeat_frequency_jitter: float = Field(0.1, ge=0)
    """Jitter fraction to be applied to the heartbeat frequency"""

    heartbeat_max_missed: int = Field(5, ge=0)
    """The maximum number of heartbeats that a compute manager can miss. If more are missed, the worker is considered
    dead
    """

    # Access logging
    log_access: bool = False
    """Store API access in the database"""

    access_log_keep: int = 0
    """How far back to keep access logs (in days or as a duration string). 0 means keep all"""

    # maxmind_account_id: int | None = Field(None, description="Account ID for MaxMind GeoIP2 service")
    maxmind_license_key: str | None = None
    """License key for MaxMind GeoIP2 service. If provided, the GeoIP2 database will be downloaded and updated
    automatically
    """

    geoip2_dir: str | None = None
    """Directory containing the Maxmind GeoIP2 Cities file (GeoLite2-City.mmdb) Defaults to [base_folder]/geoip2.
    This directory will be created if needed.
    """

    geoip2_filename: str = "GeoLite2-City.mmdb"
    """Filename of the Maxmind GeoIP2 Cities file (GeoLite2-City.mmdb)"""

    # Internal jobs
    internal_job_processes: int = 1
    """Number of processes for processing internal jobs and async requests"""

    internal_job_keep: int = 0
    """How far back to keep finished internal jobs (in days or as a duration string). 0 means keep all"""

    # Homepage settings
    homepage_redirect_url: str | None = None
    """Redirect to this URL when going to the root path"""

    homepage_directory: str | None = None
    """Use this directory to serve the homepage"""

    # File uploads
    upload_directory: str | None = None
    """Directory to store user-uploaded files for processing"""

    # Other settings blocks
    database: DatabaseConfig
    """Configuration of the settings for the database"""

    api: WebAPIConfig
    """Configuration of the REST interface"""

    s3: S3Config = Field(default_factory=S3Config)
    """Configuration of the S3 file storage (optional)"""

    api_limits: APILimitConfig = Field(default_factory=APILimitConfig)
    """Configuration of the limits to the api"""

    cors: CORSconfig = Field(default_factory=CORSconfig)
    """Configuration Cross Origin Resource sharing (advanced)"""

    auto_reset: AutoResetConfig = Field(default_factory=AutoResetConfig)
    """Configuration for automatic resetting of tasks"""

    @field_validator("loglevel", mode="after")
    @classmethod
    def _check_loglevel(cls, v):
        v = v.upper()
        if v not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValidationError(f"{v} is not a valid loglevel. Must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return v

    @field_validator("service_frequency", "heartbeat_frequency", mode="before")
    @classmethod
    def _convert_durations(cls, v):
        return duration_to_seconds(v)

    @field_validator("access_log_keep", "internal_job_keep", mode="before")
    def _convert_durations_days(cls, v):
        if isinstance(v, int) or (isinstance(v, str) and v.isdigit()):
            return int(v) * 86400
        return duration_to_seconds(v)

    @model_validator(mode="before")
    @classmethod
    def _propagate_base_folder(cls, values):
        if isinstance(values, dict) and "base_folder" in values:
            values.setdefault("database", {})
            values["database"]["base_folder"] = values["base_folder"]
        return values

    @model_validator(mode="before")
    @classmethod
    def _detect_deprecated_fields(cls, values):
        if isinstance(values, dict):
            if "strict_queue_tags" in values:
                logging.getLogger(__name__).warning("strict_queue_tags is deprecated. Use strict_compute_tags instead")
                values["strict_compute_tags"] = values.pop("strict_queue_tags")
        return values

    @model_validator(mode="after")
    def _check_paths(self):
        self.homepage_directory = _make_abs_path(self.homepage_directory, self.base_folder, None)
        self.upload_directory = _make_abs_path(self.upload_directory, self.base_folder, None)
        self.logfile = _make_abs_path(self.logfile, self.base_folder, None)
        self.geoip2_dir = _make_abs_path(self.geoip2_dir, self.base_folder, "geoip2")

        if self.temporary_dir is None:
            self.temporary_dir = tempfile.gettempdir()
            self.temporary_dir = _make_abs_path("qcf_tmp", self.base_folder, tempfile.gettempdir())
        else:
            self.temporary_dir = _make_abs_path(self.temporary_dir, self.base_folder, None)

        os.makedirs(self.temporary_dir, exist_ok=True)
        return self

    model_config = SettingsConfigDict(
        extra="forbid",
        case_sensitive=False,
        env_prefix="QCF_",
        env_nested_delimiter="__",
        use_attribute_docstrings=True,
    )

    # Since we manually read the yaml files, the values passed into the init
    # come from files and should be lower priority that env settings
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return env_settings, dotenv_settings, init_settings, file_secret_settings


# Environment variable prefixes that were removed when the configuration moved from one
# BaseSettings per section (each with its own flat prefix) to a single BaseSettings at
# the top level with env_nested_delimiter="__". Maps the old prefix to its replacement.
_DEPRECATED_ENV_PREFIXES: list[tuple[str, str]] = [
    ("QCF_DB_", "QCF_DATABASE__"),
    ("QCF_APILIMIT_", "QCF_API_LIMITS__"),
    ("QCF_AUTORESET_", "QCF_AUTO_RESET__"),
    ("QCF_S3_", "QCF_S3__"),
    ("QCF_API_", "QCF_API__"),
]


def check_deprecated_env_vars() -> None:
    """
    Raise if the environment contains one of the removed per-section variable prefixes

    These no longer have any effect, so without this check a stale ``QCF_DB_HOST`` would
    leave the setting at its default with no indication that anything was wrong.

    Raises
    ------
    RuntimeError
        If a deprecated environment variable is set. The message names the replacement.
    """

    # Checked first: a valid variable can begin with a deprecated prefix. QCF_API_LIMITS__
    # starts with QCF_API_, and used to be rejected as if it were the old QCF_API_ prefix,
    # which made api_limits impossible to set from the environment at all.
    current_prefixes = tuple(
        f"QCF_{name.upper()}__"
        for name, field in FractalConfig.model_fields.items()
        if isinstance(field.annotation, type) and issubclass(field.annotation, BaseModel)
    )

    for key in os.environ:
        upper_key = key.upper()

        if upper_key.startswith(current_prefixes):
            continue

        for old_prefix, new_prefix in _DEPRECATED_ENV_PREFIXES:
            if not upper_key.startswith(old_prefix):
                continue
            # A further underscore means a nested-style name that simply does not exist
            # (eg QCF_DB__HOST); leave it to pydantic rather than suggesting a mangled
            # replacement.
            if upper_key.startswith(old_prefix + "_"):
                continue
            new_key = new_prefix + upper_key[len(old_prefix) :]
            raise RuntimeError(f"Environment variable {key} is deprecated. Use {new_key} instead.")


def read_configuration(file_paths: list[str], extra_config: dict[str, Any] | None = None) -> FractalConfig:
    """
    Reads QCFractal configuration from YAML files
    """
    logger = logging.getLogger(__name__)
    config_data: dict[str, Any] = {}

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

    check_deprecated_env_vars()

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
        yaml.dump(default_config.model_dump(include=include), f, sort_keys=False)
