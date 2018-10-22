"""
Utilities for CLI programs
"""

import importlib
import json
import yaml


def import_module(module):
    """Protected import of a module
    """
    try:
        ret = importlib.import_module(module)
    except ImportError:
        raise ImportError("Requested module '{}' not found.".format(module))

    return ret


def read_config_file(fname):
    """Reads a JSON or YAML file.
    """
    if fname.endswith(".yaml") or fname.endswith(".yml"):
        rfunc = yaml.load
    elif fname.endswith(".json"):
        rfunc = json.load
    else:
        raise TypeError("Did not understand file type {}.".format(fname))

    with open(fname, "r") as handle:
        ret = rfunc(handle)

    return ret
