"""
Helpers to hash complex objects
"""

import numpy as np


def float_prep(array, around):
    """
    Rounds floats to a common value and build positive zero's to prevent hash conflicts.
    """

    if isinstance(array, (list, np.ndarray)):
        # Round array
        array = np.around(array, around)
        # Flip zeros
        array[np.abs(array) < 5 ** (-(around + 1))] = 0

    elif isinstance(array, (float, int)):
        array = round(array, around)
        if array == -0.0:
            array = 0.0
    else:
        raise TypeError("Type '{}' not recognized".format(type(array).__name__))

    return array
