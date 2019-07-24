"""
Utility functions for QCPortal/QCFractal Interface.
"""
import re
from enum import Enum, EnumMeta
from textwrap import dedent, indent
from typing import Any

from pydantic import BaseModel, BaseSettings

__all__ = ["doc_formatter", "replace_dict_keys"]


def type_to_string(input_type):
    if type(input_type) is type:
        return input_type.__name__
    else:
        return repr(input_type).replace("typing.", "")


def is_pydantic(test_object):
    try:
        instance = isinstance(test_object, BaseSettings) or isinstance(test_object, BaseModel)
    except TypeError:
        instance = False
    try:
        subclass = issubclass(test_object, BaseSettings) or issubclass(test_object, BaseModel)
    except TypeError:
        subclass = False
    return instance or subclass


def parse_type_str(prop) -> str:
    from pydantic.fields import Shape  # Import here to minimize issues
    typing_map = {
        Shape.TUPLE: "Tuple",
        Shape.SET: "Set",
        Shape.LIST: "List",
        Shape.SINGLETON: "Union"
    }
    if type(prop) is type or prop.__module__ == "typing":
        # True native Python type
        prop_type_str = type_to_string(prop)
    elif issubclass(prop.type_.__class__, Enum) or issubclass(prop.type_.__class__, EnumMeta):
        # Enumerate, have to do the __class__ or issubclass(prop.type_) throws issues later.
        prop_type_str = '{' + ','.join([str(x.value) for x in prop.type_]) + '}'
    elif type(prop.type_) is type and prop.shape == Shape.SINGLETON:
        # Native Python type buried in a Field
        prop_type_str = type_to_string(prop.type_)
    elif is_pydantic(prop.type_):
        # Pydantic types
        prop_type_str = f":class:`{prop.type_.__name__}`"
    elif prop.type_.__module__ == "typing":
        # Typing Types
        prop_type_str = ""
        key_field = prop.key_field
        sub_fields = prop.sub_fields
        # Special case of Optional[]
        # if sub_fields is not None and any([sf.sub_fields is type(None) for sf in sub_fields]):
        if sub_fields is not None and any([sf.type_ is type(None) for sf in sub_fields]):
            reconstructed_props = [f for f in sub_fields if f.type_ is not type(None)]
            parsed_types = [parse_type_str(f) for f in reconstructed_props]
            if len(parsed_types) == 1:
                prop_type_str = parsed_types[0]
            else:
                prop_type_str = "Union[" + ', '.join(parsed_types) + ']'
        elif prop.shape == Shape.MAPPING:
            prop_type_str = "Dict[" + parse_type_str(key_field) + ', ' + parse_type_str(prop.type_) + ']'
        elif sub_fields is not None:
            # Not "optional", but iterable
            prop_type_str = typing_map[prop.shape] + '[' + ', '.join([parse_type_str(sf) for sf in sub_fields]) + ']'
        elif prop.type_ is Any:
            prop_type_str = "Any"
    elif prop.shape in typing_map.keys():
        if prop.sub_fields is None:
            # Single item
            if prop.type_.__module__ == "pydantic.types":
                # A bit of a catch-all
                prop_type_str = prop.type_.__name__
            else:
                prop_type_str = typing_map[prop.shape] + '[' + parse_type_str(prop.type_) + ']'
        else:
            prop_type_str = typing_map[prop.shape] + '[' + ', '.join([parse_type_str(sf) for sf in prop.sub_fields]) + ']'
    else:
        # Finally, with nothing else to do...
        prop_type_str = str(prop)

    return prop_type_str


def doc_formatter(target_object, allow_failure=True):
    """
    Set the docstring for a Pydantic object automatically based on the parameters

    This could use improvement.

    Might be ported to Elemental at some point
    """
    doc = target_object.__doc__

    # Convert the None to regex-parsable string
    if doc is None:
        doc_edit = ''
    else:
        doc_edit = doc

    # Is pydantic and not already formatted
    if is_pydantic(target_object) and not re.search(r'^\s*Parameters\n', doc_edit, re.MULTILINE):
        try:
            # Add the white space
            if not doc_edit.endswith('\n\n'):
                doc_edit += "\n\n"
            # Add Parameters separate
            new_doc = dedent(doc_edit) + "Parameters\n----------\n"
            # Get Pydantic fields
            target_fields = target_object.__fields__
            # Go through each property
            for prop_name, prop in target_fields.items():
                # Handle Type
                prop_type_str = parse_type_str(prop)

                # Handle (optional) description
                prop_desc = prop.schema.description

                # Combine in the following format:
                # name : type(, Optional, Default)
                #   description
                first_line = prop_name + ' : ' + prop_type_str
                if not prop.required and (prop.default is None or is_pydantic(prop.default)):
                    first_line += ", Optional"
                elif prop.default is not None:
                    first_line += f", Default: {prop.default}"
                # Write the prop description
                second_line = "\n" + indent(prop_desc, "    ") if prop_desc is not None else ""
                # Finally, write the detailed doc string
                new_doc += first_line + second_line + "\n"
        except Exception:
            if allow_failure:
                new_doc = doc
            else:
                raise
        except (SystemExit, KeyboardInterrupt):
            # Make lgtm happy. Since this is user/higher order failures than this function, always raise.
            raise

    else:
        new_doc = doc
    # except:
    #     # Something in the formatting went wrong, just ignore it since this is just docstring formatting
    #     new_doc = doc

    # Assign the new doc string
    target_object.__doc__ = new_doc


def replace_dict_keys(data, replacement):
    """
    Recursively replaces the keys in data from a dictionary `replacement`.
    """

    if isinstance(data, dict):
        ret = {}
        for k, v in data.items():

            # Replace key
            if k in replacement:
                k = replacement[k]

            # Recurse value if needed.
            new_v = v
            if isinstance(v, dict):
                new_v = replace_dict_keys(v, replacement)
            elif isinstance(v, (list, tuple)):
                new_v = [replace_dict_keys(x, replacement) for x in v]
                if isinstance(v, tuple):
                    new_v = tuple(new_v)

            ret[k] = new_v
        return ret

    elif isinstance(data, (tuple, list)):
        new_data = [replace_dict_keys(x, replacement) for x in data]
        if isinstance(data, tuple):
            new_data = tuple(new_data)

        return new_data

    elif isinstance(data, BaseModel):
        # Handle base model structures
        ret = data.copy()  # Create a copy
        search_keys = data.fields.keys()  # Enumerate keys
        for key in search_keys:
            existing_data = getattr(data, key)
            # Try to replace data recursively
            new_data = replace_dict_keys(existing_data, replacement)
            if new_data == existing_data:
                continue  # Do nothing if new data is the same (safer)
            setattr(ret, key, new_data)  # Replace new data in the copy to avoid in-place changes
        return ret  # Return

    else:
        return data
