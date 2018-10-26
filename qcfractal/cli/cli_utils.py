"""
Utilities for CLI programs
"""

import copy
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


def argparse_config_merge(parser, parsed_options, config_options, parser_default=None):
    """Merges options between a configuration file and a parser

    Parameters
    ----------
    parser : ArgumentParser
    config_options : dict
    parser_default : None, optional
    """
    config_options = copy.deepcopy(config_options)

    default_options = vars(parser.parse_args(args=parser_default))
    diff = config_options.keys() - default_options.keys()
    if diff:
        raise argparse.ArgumentError(None,
                                     "Unknown arguments found in configuration file: {}.".format(", ".join(diff)))

    # Add in parsed options
    for k, v in parsed_options.items():

        # User has overridden default options, update config
        if v != parser.get_default(k):
            config_options[k] = v

        # Add in missing defaults
        if v not in config_options:
            config_options[k] = v

    return config_options