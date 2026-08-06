import copy
import os

import pytest
import yaml

from qcfractal.config import FractalConfig, check_deprecated_env_vars, read_configuration

_base_config = {
    "api": {
        "secret_key": "abc1234def456",
        "jwt_secret_key": "abc123def456",
    },
    "database": {"username": "qcfractal", "password": "abc123def456"},
}


def test_config_durations_plain(tmp_path):
    base_folder = str(tmp_path)

    base_config = copy.deepcopy(_base_config)
    base_config["service_frequency"] = 3600
    base_config["heartbeat_frequency"] = 30
    base_config["access_log_keep"] = 31
    base_config["internal_job_keep"] = 7
    base_config["api"]["jwt_access_token_expires"] = 7450
    base_config["api"]["jwt_refresh_token_expires"] = 637277
    cfg = FractalConfig(base_folder=base_folder, **base_config)

    assert cfg.service_frequency == 3600
    assert cfg.heartbeat_frequency == 30
    assert cfg.access_log_keep == 2678400  # interpreted as days
    assert cfg.internal_job_keep == 604800
    assert cfg.api.jwt_access_token_expires == 7450
    assert cfg.api.jwt_refresh_token_expires == 637277


def test_config_durations_str(tmp_path):
    base_folder = str(tmp_path)

    base_config = copy.deepcopy(_base_config)
    base_config["service_frequency"] = "1h"
    base_config["heartbeat_frequency"] = "30s"
    base_config["access_log_keep"] = "1d4h2s"
    base_config["internal_job_keep"] = "1d4h7s"
    base_config["api"]["jwt_access_token_expires"] = "2h4m10s"
    base_config["api"]["jwt_refresh_token_expires"] = "7d9h77s"
    cfg = FractalConfig(base_folder=base_folder, **base_config)

    assert cfg.service_frequency == 3600
    assert cfg.heartbeat_frequency == 30
    assert cfg.access_log_keep == 100802
    assert cfg.internal_job_keep == 100807
    assert cfg.api.jwt_access_token_expires == 7450
    assert cfg.api.jwt_refresh_token_expires == 637277


def test_config_durations_dhms(tmp_path):
    base_folder = str(tmp_path)

    base_config = copy.deepcopy(_base_config)
    base_config["service_frequency"] = "1:00:00"
    base_config["heartbeat_frequency"] = "30"
    base_config["access_log_keep"] = "1:04:00:02"
    base_config["internal_job_keep"] = "1:04:00:07"
    base_config["api"]["jwt_access_token_expires"] = "2:04:10"
    base_config["api"]["jwt_refresh_token_expires"] = "7:09:00:77"
    cfg = FractalConfig(base_folder=base_folder, **base_config)

    assert cfg.service_frequency == 3600
    assert cfg.heartbeat_frequency == 30
    assert cfg.access_log_keep == 100802
    assert cfg.internal_job_keep == 100807
    assert cfg.api.jwt_access_token_expires == 7450
    assert cfg.api.jwt_refresh_token_expires == 637277


def test_config_tmpdir_create(tmp_path):
    base_config = copy.deepcopy(_base_config)
    base_config["base_folder"] = str(tmp_path)
    base_config["temporary_dir"] = str(tmp_path / "qcatmpdir")
    cfg = FractalConfig(**base_config)

    assert cfg.temporary_dir == str(tmp_path / "qcatmpdir")
    assert os.path.exists(cfg.temporary_dir)


@pytest.fixture
def clean_qcf_env(monkeypatch):
    """Removes any QCF_* variables inherited from the caller's environment"""
    for key in list(os.environ):
        if key.upper().startswith("QCF_"):
            monkeypatch.delenv(key, raising=False)
    return monkeypatch


@pytest.mark.parametrize(
    "old_var,expected_new",
    [
        ("QCF_DB_HOST", "QCF_DATABASE__HOST"),
        ("QCF_API_PORT", "QCF_API__PORT"),
        ("QCF_APILIMIT_GET_RECORDS", "QCF_API_LIMITS__GET_RECORDS"),
        ("QCF_AUTORESET_ENABLED", "QCF_AUTO_RESET__ENABLED"),
        ("QCF_S3_ENABLED", "QCF_S3__ENABLED"),
        # Matching is case insensitive, since pydantic-settings reads env vars that way
        ("qcf_db_host", "QCF_DATABASE__HOST"),
    ],
)
def test_config_deprecated_env_vars(clean_qcf_env, old_var, expected_new):
    clean_qcf_env.setenv(old_var, "some_value")

    with pytest.raises(RuntimeError, match=f"Use {expected_new} instead"):
        check_deprecated_env_vars()


@pytest.mark.parametrize(
    "env_var",
    [
        "QCF_LOGLEVEL",
        "QCF_DATABASE__HOST",
        "QCF_API__PORT",
        # Begins with the deprecated QCF_API_ prefix, and used to be rejected because of
        # it, which made api_limits impossible to set from the environment
        "QCF_API_LIMITS__GET_RECORDS",
        "QCF_AUTO_RESET__ENABLED",
        "QCF_CORS__ENABLED",
        "QCF_S3__ENABLED",
    ],
)
def test_config_current_env_vars_not_deprecated(clean_qcf_env, env_var):
    clean_qcf_env.setenv(env_var, "1")

    check_deprecated_env_vars()  # must not raise


def test_config_env_var_api_limits(clean_qcf_env, tmp_path):
    """api_limits must be settable from the environment"""

    config_path = tmp_path / "qcfractal_config.yaml"
    with open(config_path, "w") as f:
        yaml.safe_dump(_base_config, f)

    clean_qcf_env.setenv("QCF_API_LIMITS__GET_RECORDS", "4321")
    cfg = read_configuration([str(config_path)])

    assert cfg.api_limits.get_records == 4321
