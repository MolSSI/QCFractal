"""
A set of utility functions to help the Collections
"""

import math


def nCr(n, r):
    """
    Compute the binomial coefficient n! / (k! * (n-k)!)

    Parameters
    ----------
    n : int
        Number of samples
    r : int
        Denominator

    Returns
    -------
    ret : int
        Value
    """
    return math.factorial(n) / math.factorial(r) / math.factorial(n - r)