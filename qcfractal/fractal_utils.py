"""
A set of common utility functions to be used in multiple modules
"""

import importlib
import operator
from typing import Callable


def get_function(function: str) -> Callable:
    """Obtains a Python function from a given string.

    Parameters
    ----------
    function : str
        A full path to a function

    Returns
    -------
    callable
        The desired Python function

    Examples
    --------

    >>> get_function("numpy.einsum")
    <function einsum at 0x110406a60>
    """

    module_name, func_name = function.split(".", 1)
    module = importlib.import_module(module_name)
    return operator.attrgetter(func_name)(module)
