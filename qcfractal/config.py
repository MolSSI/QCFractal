"""
The global qcfractal config file specification.
"""

import argparse
import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import Field, validator

from .interface.models import AutodocBaseSettings


def _str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


class SettingsCommonConfig:
    env_prefix = "QCF_"
    case_insensitive = True
    extra = "forbid"


class ConfigSettings(AutodocBaseSettings):

    _type_map = {"string": str, "integer": int, "float": float, "boolean": _str2bool}

    @classmethod
    def field_names(cls):
        return list(cls.schema()["properties"].keys())

    @classmethod
    def help_info(cls, field):
        info = cls.schema()["properties"][field]

        ret = {"type": cls._type_map[info["type"]]}
        # if "default" in info:
        #     ret["default"] = info["default"]
        if "description" in info:
            ret["help"] = info["description"]
        return ret


class DatabaseSettings(ConfigSettings):
    """
    Postgres Database settings
    """

    port: int = Field(5432, description="The postgresql default port")
    host: str = Field(
        "localhost",
        description="Default location for the postgres server. If not localhost, qcfractal command lines cannot manage "
        "the instance.",
    )
    username: str = Field(None, description="The postgres username to default to.")
    password: str = Field(None, description="The postgres password for the give user.")
    directory: str = Field(
        None, description="The physical location of the QCFractal instance data, defaults to the root folder."
    )
    database_name: str = Field("qcfractal_default", description="The database name to connect to.")
    logfile: str = Field("qcfractal_postgres.log", description="The logfile to write postgres logs.")
    own: bool = Field(
        True,
        description="If own is True, QCFractal will control the database instance. If False "
        "Postgres will expect a booted server at the database specification.",
    )

    class Config(SettingsCommonConfig):
        pass


class ViewSettings(ConfigSettings):
    """
    HDF5 view settings
    """

    enable: bool = Field(True, description="Enable frozen-views.")
    directory: str = Field(None, description="Location of frozen-view data. If None, defaults to base_folder/views.")


class FractalServerSettings(ConfigSettings):
    """
    Fractal Server settings
    """

    name: str = Field("QCFractal Server", description="The QCFractal server default name.")
    port: int = Field(7777, description="The QCFractal default port.")

    compress_response: bool = Field(
        True, description="Compress REST responses or not, should be True unless behind a proxy."
    )
    allow_read: bool = Field(True, description="Always allows read access to record tables.")
    security: str = Field(
        None,
        description="Optional user authentication. Specify 'local' to enable "
        "authentication through locally stored usernames. "
        "User permissions may be manipulated through the ``qcfractal-server "
        "user`` CLI.",
    )

    query_limit: int = Field(1000, description="The maximum number of records to return per query.")
    logfile: Optional[str] = Field("qcfractal_server.log", description="The logfile to write server logs.")
    loglevel: str = Field("info", description="Level of logging to enable (debug, info, warning, error, critical)")
    cprofile: Optional[str] = Field(
        None, description="Enable profiling via cProfile, and output cprofile data to this path"
    )
    service_frequency: int = Field(60, description="The frequency to update the QCFractal services.")
    max_active_services: int = Field(20, description="The maximum number of concurrent active services.")
    heartbeat_frequency: int = Field(1800, description="The frequency (in seconds) to check the heartbeat of workers.")
    log_apis: bool = Field(
        False,
        description="True or False. Store API access in the Database. This is an advanced "
        "option for servers accessed by external users through QCPortal.",
    )
    geo_file_path: Optional[str] = Field(
        None,
        description="Geoip2 cites file path (.mmdb) for resolving IP addresses. Defaults to [base_folder]/GeoLite2-City.mmdb",
    )

    _default_geo_filename: str = "GeoLite2-City.mmdb"

    @validator("logfile")
    def check_basis(cls, v):
        if v == "None":
            v = None
        return v

    class Config(SettingsCommonConfig):
        pass


class FractalConfig(ConfigSettings):
    """
    Top level configuration headers and options for a QCFractal Configuration File
    """

    # class variable, not in the pydantic model
    _defaults_file_path: str = os.path.expanduser("~/.qca/qcfractal_defaults.yaml")

    base_folder: str = Field(
        os.path.expanduser("~/.qca/qcfractal"),
        description="The QCFractal base instance to attach to. " "Default will be your home directory",
    )
    database: DatabaseSettings = DatabaseSettings()
    view: ViewSettings = ViewSettings()
    fractal: FractalServerSettings = FractalServerSettings()

    class Config(SettingsCommonConfig):
        pass

    def __init__(self, **kwargs):

        # If no base_folder provided, read it from ~/.qca/qcfractal_defaults.yaml (if it exists)
        # else, use the default base_folder
        if "base_folder" in kwargs:
            kwargs["base_folder"] = os.path.expanduser(kwargs["base_folder"])
        else:
            if Path(FractalConfig._defaults_file_path).exists():
                with open(FractalConfig._defaults_file_path, "r") as handle:
                    kwargs["base_folder"] = yaml.load(handle.read(), Loader=yaml.FullLoader)["default_base_folder"]

        super().__init__(**kwargs)

    @classmethod
    def from_base_folder(cls, base_folder):
        path = Path(base_folder).absolute() / "qcfractal_config.yaml"
        with open(str(path), "r") as handle:
            return cls(**yaml.load(handle.read(), Loader=yaml.FullLoader))

    @property
    def base_path(self):
        return Path(self.base_folder)

    @property
    def config_file_path(self):
        return self.base_path / "qcfractal_config.yaml"

    @property
    def database_path(self):
        if self.database.directory is None:
            return self.base_path / "postgres"
        else:
            return Path(os.path.expanduser(self.database.directory))

    def database_uri(self, safe: bool = True, database: str = None) -> str:

        uri = "postgresql://"
        if self.database.username is not None:
            uri += f"{self.database.username}:"

            if self.database.password is not None:
                if safe:
                    pw = "*******"
                else:
                    pw = self.database.password
                uri += pw

            uri += "@"

        uri += f"{self.database.host}:{self.database.port}/"

        if database is None:
            uri += self.database.database_name
        else:
            uri += database

        return uri

    @property
    def view_path(self):
        if self.view.directory is None:
            default_view_path = self.base_path / "views"
            default_view_path.mkdir(parents=False, exist_ok=True)
            return default_view_path
        else:
            return Path(os.path.expanduser(self.view.directory))

    def geo_file_path(self):

        if self.fractal.geo_file_path:
            return self.fractal.geo_file_path
        else:
            return os.path.join(self.base_folder, self.fractal._default_geo_filename)
