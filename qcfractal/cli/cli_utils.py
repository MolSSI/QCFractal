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


def build_adapter_from_options(adapter_type, uri=None, config_file=None, **kwargs):

    options = sum(x is not None for x in [uri, config_file])
    if len(options) != 1:
        raise KeyError("Can only provide a single URI or config_file.")

    if adapter_type == "fireworks":
        fireworks = import_module("fireworks")

        if uri is not None:
            adapter = fireworks.LaunchPad(uri)
        elif config_file is not None:
            adapter = fireworks.LaunchPad.from_file(config_file)
        else:
            raise KeyError("A URI or config_file must be specified.")

    elif adapter_type == "dask":
        ddd = import_module("distributed")

        if uri is not None:
            adapter = fireworks.Client(uri)
        elif config_file is not None:
            data = read_config_file(config_file)
            adapter = fireworks.Client(**data)
    else:
        raise KeyError("Unknown adapter type '{}', available options: {'fireworks', 'dask'}.".format(adapter_type))

    return adapter
