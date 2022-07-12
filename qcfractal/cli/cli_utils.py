"""
Utilities for CLI programs
"""

import argparse
import copy
import importlib
import json
import signal
from functools import partial

import yaml


def import_module(module, package=None):
    """Protected import of a module"""
    try:
        ret = importlib.import_module(module, package=package)
    except ModuleNotFoundError:
        if package is not None:
            raise ModuleNotFoundError("Requested module/package '{}/{}' not found.".format(module, package))
        raise ModuleNotFoundError("Requested module '{}' not found.".format(module))
    return ret


def read_config_file(fname):
    """Reads a JSON or YAML file."""
    if fname.endswith(".yaml") or fname.endswith(".yml"):
        try:
            rfunc = partial(yaml.load, Loader=yaml.FullLoader)
        except AttributeError:
            rfunc = yaml.load
    elif fname.endswith(".json"):
        rfunc = json.load
    else:
        raise TypeError("Did not understand file type {}.".format(fname))

    try:
        with open(fname, "r") as handle:
            ret = rfunc(handle)
    except FileNotFoundError:
        raise FileNotFoundError("No config file found at {}.".format(fname))

    return ret


def argparse_config_merge(parser, parsed_options, config_options, parser_default=None, check=True):
    """Merges options between a configuration file and a parser

    Parameters
    ----------
    parser : ArgumentParser
    config_options : dict
    parser_default : List, Optional
    """
    config_options = copy.deepcopy(config_options)

    if check:
        default_options = vars(parser.parse_args(args=parser_default))
        diff = config_options.keys() - default_options.keys()
        if diff:
            raise argparse.ArgumentError(
                None, "Unknown arguments found in configuration file: {}.".format(", ".join(diff))
            )

    # Add in parsed options
    for k, v in parsed_options.items():

        # User has overridden default options, update config
        if v != parser.get_default(k):
            config_options[k] = v

        # Add in missing defaults
        if k not in config_options:
            config_options[k] = v

    return config_options


def install_signal_handlers(loop, cleanup):
    """
    Install cleanup handlers to shutdown on:
     - Keyboard Interupt (SIGINT)
     - Shutdown kill/pkill (SIGTERM)
    """

    old_handlers = {}

    def handle_signal(sig, frame):
        async def cleanup_and_stop():
            try:
                cleanup()
            finally:
                loop.stop()

        loop.add_callback_from_signal(cleanup_and_stop)

        # Add old handlers back in so we do not cleanup twice
        signal.signal(sig, old_handlers[sig])

    for sig in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[sig] = signal.signal(sig, handle_signal)
