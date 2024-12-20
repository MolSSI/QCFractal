"""
Contains testing infrastructure for QCFractal.
"""

from __future__ import annotations

import json
import lzma
import os
import signal
import sys
import time
from contextlib import contextmanager

from qcelemental.models import Molecule
from qcelemental.models.results import WavefunctionProperties

from qcfractal.components.serverinfo.socket import geoip2_found
from qcportal.serialization import _json_decode

# Valid client encodings
valid_encodings = ["application/json", "application/msgpack"]

# Path to this file (directory only)
_my_path = os.path.dirname(os.path.abspath(__file__))

geoip_path = os.path.join(_my_path, "MaxMind-DB", "test-data")
geoip_filename = "GeoLite2-City-Test.mmdb"
ip_testdata_path = os.path.join(_my_path, "MaxMind-DB", "source-data", "GeoIP2-City-Test.json")

ip_tests_enabled = os.path.exists(geoip_path) and os.path.exists(ip_testdata_path) and geoip2_found

testconfig_path = os.path.join(_my_path, "config_files")
migrationdata_path = os.path.join(_my_path, "migration_data")

test_groups = ["group1", "group2", "group3"]
test_users = {
    "admin_user": {
        "pw": "something123",
        "info": {
            "role": "admin",
            "groups": ["group1", "group2"],
            "fullname": "Mrs. Admin User",
            "organization": "QCF Testing",
            "email": "admin@example.com",
        },
    },
    "read_user": {
        "pw": "something123",
        "info": {
            "role": "read",
            "groups": ["group2"],
            "fullname": "Mr. Read User",
            "organization": "QCF Testing",
            "email": "read@example.com",
        },
    },
    "monitor_user": {
        "pw": "something123",
        "info": {
            "role": "monitor",
            "groups": ["group1"],
            "fullname": "Mr. Monitor User",
            "organization": "QCF Testing",
            "email": "monitor@example.com",
        },
    },
    "compute_user": {
        "pw": "something123",
        "info": {
            "role": "compute",
            "groups": [],
            "fullname": "Mr. Compute User",
            "organization": "QCF Testing",
            "email": "compute@example.com",
        },
    },
    "submit_user": {
        "pw": "something123",
        "info": {
            "role": "submit",
            "groups": ["group1"],
            "fullname": "Mrs. Submit User",
            "organization": "QCF Testing",
            "email": "submit@example.com",
        },
    },
}


def read_record_data(name: str):
    """
    Loads pre-computed/dummy procedure data from the test directory

    Parameters
    ----------
    name
        The name of the file to load (without the json extension)

    Returns
    -------
    :
        A dictionary with all the data in the file

    """

    data_path = os.path.join(_my_path, "procedure_data")
    file_path = os.path.join(data_path, name + ".json.xz")
    is_xz = True

    if not os.path.exists(file_path):
        file_path = os.path.join(data_path, name + ".json")
        is_xz = False

    if not os.path.exists(file_path):
        raise RuntimeError(f"Procedure data file {file_path} not found!")

    if is_xz:
        with lzma.open(file_path, "rt") as f:
            data = json.load(f, object_hook=_json_decode)
    else:
        with open(file_path, "r") as f:
            data = json.load(f, object_hook=_json_decode)

    return data


def load_molecule_data(name: str) -> Molecule:
    """
    Loads a molecule object for use in testing
    """

    data_path = os.path.join(_my_path, "molecule_data")
    file_path = os.path.join(data_path, name + ".json")
    return Molecule.from_file(file_path)


def load_wavefunction_data(name: str) -> WavefunctionProperties:
    """
    Loads a wavefunction object for use in testing
    """

    data_path = os.path.join(_my_path, "wavefunction_data")
    file_path = os.path.join(data_path, name + ".json")

    with open(file_path, "r") as f:
        data = json.load(f)
    return WavefunctionProperties(**data)


def load_ip_test_data():
    """
    Loads data for testing IP logging
    """

    with open(ip_testdata_path, "r") as f:
        d = json.load(f)

    # Stored as a list containing a dictionary with one key. Convert to a regular dict
    ret = {}
    for x in d:
        ret.update(x)

    return ret


def load_hash_test_data(file_base: str):
    """
    Loads data for testing dictionary hashing
    """

    file_path = os.path.join(_my_path, "hash_data", f"{file_base}.json.xz")
    with lzma.open(file_path, "rt") as f:
        return json.load(f)


@contextmanager
def caplog_handler_at_level(caplog_fixture, level, logger=None):
    """
    Helper function to set the caplog fixture's handler to a certain level as well, otherwise it wont be captured

    e.g. if caplog.set_level(logging.INFO) but caplog.handler is at logging.CRITICAL, anything below CRITICAL wont be
    captured.
    """
    starting_handler_level = caplog_fixture.handler.level
    caplog_fixture.handler.setLevel(level)
    with caplog_fixture.at_level(level, logger=logger):
        yield
    caplog_fixture.handler.setLevel(starting_handler_level)


def terminate_process(proc):
    if proc.poll() is None:
        # Interrupt (SIGINT)
        if sys.platform.startswith("win"):
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)

        try:
            start = time.time()
            while (proc.poll() is None) and (time.time() < (start + 15)):
                time.sleep(0.02)

        # Kill (SIGKILL)
        finally:
            proc.kill()
