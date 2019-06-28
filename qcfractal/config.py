"""
The global qcfractal config file specification.
"""

import argparse
import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, BaseSettings, Schema, validator


def _str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

class SettingsCommonConfig:
    env_prefix = "QCF_"
    case_insensitive = True
    extra = "forbid"


class ConfigSettings(BaseSettings):

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

    port: int = Schema(5432, description="The postgresql default port")
    host: str = Schema(
        "localhost",
        description=
        "Default location for the postgres server. If not localhost, qcfractal command lines cannot manage the instance."
    )
    username: str = Schema(None, description="The postgres username to default to.")
    password: str = Schema(None, description="The postgres password for the give user.")
    directory: str = Schema(
        None, description="The physical location of the QCFractal instance data, defaults to the root folder.")
    default_database: str = Schema("qcfractal_default", description="The default database to connect to.")
    logfile: str = Schema("qcfractal_postgres.log", description="The logfile to write postgres logs.")
    own: bool = Schema(True, description="If own is True, QCFractal will control the database instance. If False Postgres will expect a booted server at the database specification.")

    class Config(SettingsCommonConfig):
        pass


class FractalServerSettings(ConfigSettings):
    """
    Postgres Database settings
    """

    name: str = Schema("QCFractal Server", description="The QCFractal server default name.")
    port: int = Schema(7777, description="The QCFractal default port.")

    compress_response: bool = Schema(True, description="Compress REST responses or not, should be True unless behind a proxy.")
    allow_read: bool = Schema(True, description="Always allows read access to record tables.")
    security: str = Schema(None, description="Optional security features.")

    query_limit: int = Schema(1000, description="The maximum number of records to return per query.")
    logfile: Optional[str] = Schema("qcfractal_server.log", description="The logfile to write server logs.")
    max_active_services: int = Schema(20, description="The maximum number of concurrent active services.")
    heartbeat_frequency: int = Schema(1800,
                                      description="The frequency (in seconds) to check the heartbeat of workers.")

    @validator('logfile')
    def check_basis(cls, v):
        if v == "None":
            v = None
        return v

    class Config(SettingsCommonConfig):
        pass


class FractalConfig(ConfigSettings):

    base_folder: str = Schema(os.path.expanduser("~/.qca/qcfractal"),
                              description="The QCFractal base instance to attach to.")
    database: DatabaseSettings = DatabaseSettings()
    fractal: FractalServerSettings = FractalServerSettings()

    class Config(SettingsCommonConfig):
        pass

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

    def database_uri(self, safe=True, database=None):

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
            uri += self.database.default_database
        else:
            uri += database

        return uri
