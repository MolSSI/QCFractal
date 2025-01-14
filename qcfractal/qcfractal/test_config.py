import copy
import os

from qcfractal.config import FractalConfig

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
    base_folder = str(tmp_path)
    base_config = copy.deepcopy(_base_config)
    base_config["temporary_dir"] = str(tmp_path / "qcatmpdir")
    cfg = FractalConfig(base_folder=base_folder, **base_config)

    assert cfg.temporary_dir == str(tmp_path / "qcatmpdir")
    assert os.path.exists(cfg.temporary_dir)
