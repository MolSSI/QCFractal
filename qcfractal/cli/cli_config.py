"""
The global qcfractal config file specification.
"""

import os
from enum import Enum
from functools import partial
from math import ceil
from pathlib import Path

from typing import List, Optional

import tornado.log

import qcengine as qcng
import qcfractal
from pydantic import BaseModel, BaseSettings, validator, Schema

from . import cli_utils

__all__ = ["main"]

QCA_FOLDER = Path(os.path.expanduser("~/.qca/"))
QCF_CONFIG_FILE = QCA_FOLDER / "qcfractal_config.yaml"
QCF_DATABASE_FOLDER = QCA_FOLDER / "qcfractal_data"

class SettingsCommonConfig:
    env_prefix = "QCF_"
    case_insensitive = True
    extra = "forbid"

class ConfigSettings(BaseSettings):

    _type_map = {"string": str, "integer": int, "float": float, "bool": bool}

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

    port: int = Schema(7778, description="The postgresql default port")
    address: str = Schema(
        "localhost",
        description=
        "Default location for the postgres server. If not localhost, qcfractal command lines cannot manage the instance."
    )
    username: str = Schema("qcfractal_user", description="The postgres username to default to.")
    password: str = Schema(None, description="The postgres password for the give user.")
    directory: str = Schema("~/.qca/qcfractal_data/",
                               description="The physical location of the QCFractal instance data.")
    default_database: str = Schema("qcfractal_default", description="The default database to connect to.")
    logfile: str = Schema("qcfractal_postgres.log", description="The logfile to write postgres logs.")

    class Config(SettingsCommonConfig):
        pass

class FractalServerSettings(ConfigSettings):
    """
    Postgres Database settings
    """

    name: str = Schema("QCFractal Server", description="The QCFractal server default name.")
    port: int = Schema(7777, description="The QCFractal default port.")

    query_limit: int = Schema(1000, description="The maximum number of records to return per query.")
    logfile: str = Schema("qcfractal_server.log", description="The logfile to write server logs.")
    max_active_services: int = Schema(20,
                               description="The maximum number of concurrent active services.")
    heartbeat_frequency: int = Schema(1800, description="The frequency (in seconds) to check the heartbeat of workers.")

    class Config(SettingsCommonConfig):
        pass

class FractalConfig(ConfigSettings):

    database: DatabaseSettings = DatabaseSettings()
    fractal:  FractalServerSettings = FractalServerSettings()

    class Config(SettingsCommonConfig):
        pass