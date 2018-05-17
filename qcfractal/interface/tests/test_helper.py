"""
A file containing utility functions for testing dqm_client
"""

def compare_lists(bench, val):
    """
    Checks to see if two list like objects are the same.
    """

    if not len(bench) == len(val):
        return False
    if not sorted(bench) == sorted(val):
        return False

    return True
