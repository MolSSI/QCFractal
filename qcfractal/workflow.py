"""
Builds graph workflows to compute.
"""

import copy.deepcopy


class IterWorfklow:
    """
    A workflow object that can be used for iterative workflows such
    as geometry optimizations.
    """
    
    def __init__(self, func, args, method, program, options):
        self._state = state
