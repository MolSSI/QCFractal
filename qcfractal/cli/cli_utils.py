"""
Utilities for CLI programs
"""

import argparse
import copy
import importlib
import json
import signal
from enum import Enum
from functools import partial
from textwrap import dedent, indent

import yaml

from pydantic import BaseModel, BaseSettings


def import_module(module, package=None):
    """Protected import of a module
    """
    try:
        ret = importlib.import_module(module, package=package)
    except ModuleNotFoundError:
        if package is not None:
            raise ModuleNotFoundError("Requested module/package '{}/{}' not found.".format(module, package))
        raise ModuleNotFoundError("Requested module '{}' not found.".format(module))
    return ret


def read_config_file(fname):
    """Reads a JSON or YAML file.
    """
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
            raise argparse.ArgumentError(None,
                                         "Unknown arguments found in configuration file: {}.".format(", ".join(diff)))

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


def doc_formatter(target_object):
    """
    Set the docstring for a Pydantic object automatically based on the parameters

    This could use improvement.
    """
    doc = target_object.__doc__

    # Handle non-pydantic objects
    if doc is None:
        new_doc = ''
    elif 'Parameters\n' in doc or not (issubclass(target_object, BaseSettings) or issubclass(target_object, BaseModel)):
        new_doc = doc
    else:
        type_formatter = {'boolan': 'bool',
                          'string': 'str',
                          'integer': 'int'
                          }
        # Add the white space
        if not doc.endswith('\n\n'):
            doc += "\n\n"
        new_doc = dedent(doc) + "Parameters\n----------\n"
        target_schema = target_object.schema()
        # Go through each property
        for prop_name, prop in target_schema['properties'].items():
            # Catch lookups for other Pydantic objects
            if '$ref' in prop:
                lookup = prop['$ref'].split('/')[-1]
                prop = target_schema['definitions'][lookup]
            # Get common properties
            prop_type = prop["type"]
            new_doc += prop_name + " : "
            prop_desc = prop['description']

            # Check for enumeration
            if 'enum' in prop:
                new_doc += '{' + ', '.join(prop['enum']) + '}'

            # Set the name/type of object
            else:
                if prop_type == 'object':
                    prop_field = prop['title']
                else:
                    prop_field = prop_type
                new_doc += f'{type_formatter[prop_field] if prop_field in type_formatter else prop_field}'

            # Handle Classes so as not to re-copy pydantic descriptions
            if prop_type == 'object':
                if not ('required' in target_schema and prop_name in target_schema['required']):
                    new_doc += ", Optional"
                prop_desc = f":class:`{prop['title']}`"

            # Handle non-classes
            else:
                if 'default' in prop:
                    default = prop['default']
                    try:
                        # Get the explicit default value for enum classes
                        if issubclass(default, Enum):
                            default = default.value
                    except TypeError:
                        pass
                    new_doc += f", Default: {default}"
                elif not ('required' in target_schema and prop_name in target_schema['required']):
                    new_doc += ", Optional"

            # Finally, write the detailed doc string
            new_doc += "\n" + indent(prop_desc, "    ") + "\n"

    # Assign the new doc string
    target_object.__doc__ = new_doc
