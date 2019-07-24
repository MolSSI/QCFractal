"""
Utility functions for QCFractal.
"""

import socket
from enum import Enum
from textwrap import dedent, indent
from typing import Tuple, Dict

from pydantic import BaseModel, BaseSettings, validator, ValidationError


def find_port() -> int:
    sock = socket.socket()
    sock.bind(('', 0))
    host, port = sock.getsockname()
    return port


def is_port_open(ip: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect((ip, int(port)))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except ConnectionRefusedError:
        return False
    finally:
        s.close()


class _JsonRefModel(BaseModel):
    """
    Reference model for Json replacement fillers

    Matches style of:

    ``'allOf': [{'$ref': '#/definitions/something'}]}``

    and will always be a length 1 list
    """
    allOf: Tuple[Dict[str, str]]

    @validator("allOf", whole=True)
    def all_of_entries(cls, v):
        value = v[0]
        if len(value) != 1:
            raise ValueError("Dict must be of length 1")
        elif '$ref' not in value:
            raise ValueError("Dict needs to have key $ref")
        elif not isinstance(value["$ref"], str) or not value["$ref"].startswith('#/'):
            raise ValueError("$ref should be formatted as #/definitions/...")
        return v


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
        try:
            new_doc = doc
            type_formatter = {'boolan': 'bool',
                              'string': 'str',
                              'integer': 'int'
                              }
            # Add the white space
            if not new_doc.endswith('\n\n'):
                new_doc += "\n\n"
            new_doc = dedent(new_doc) + "Parameters\n----------\n"
            target_schema = target_object.schema()
            # Go through each property
            for prop_name, prop in target_schema['properties'].items():
                # Catch lookups for other Pydantic objects
                if '$ref' in prop:
                    # Pre 0.28 lookup
                    lookup = prop['$ref'].split('/')[-1]
                    prop = target_schema['definitions'][lookup]
                elif 'allOf' in prop:
                    # Post 0.28 lookup
                    try:
                        # Validation, we don't need output, just the object
                        _JsonRefModel(**prop)
                        lookup = prop['allOf'][0]['$ref'].split('/')[-1]
                        prop = target_schema['definitions'][lookup]
                    except ValidationError:
                        # Doesn't conform, pass on
                        pass
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
        except:
            # For any reason, this fails, fall back
            new_doc = doc

    # Assign the new doc string
    target_object.__doc__ = new_doc
