"""
A module for generating TS search adapters.
"""

import typing

from rmgpy.reaction import Reaction

from .ts_adapter import TSAdapter


_registered_ts_adapters = {}


def register_ts_adapter(ts_method: str,
                        ts_method_class: typing.Type[TSAdapter],
                        ) -> None:
    """
    A register for TS search methods adapters.

    Parameters
    ----------
    ts_method: TSMethodsEnum
        A string representation for a TS search adapter.
    ts_method_class: child(TSAdapter)
        The TS search method adapter class (a child of TSAdapter).
    """
    if not issubclass(ts_method_class, TSAdapter):
        raise TypeError(f'{ts_method_class} is not a TSAdapter.')
    _registered_ts_adapters[ts_method] = ts_method_class


def ts_method_factory(ts_adapter: str,
                      user_guesses: list = None,
                      rmg_reaction: Reaction = None,
                      dihedral_increment = None,
                      ) -> TSAdapter:
    """
    A factory generating the TS search method adapter corresponding to ``ts_adapter``.

    Parameters
    ----------
    ts_adapter: TSMethodsEnum
        A string representation for a TS search adapter.
    user_guesses: list, optional
        Entries are string representations of Cartesian coordinate.
    rmg_reaction: Reaction, optional
        The RMG reaction object with the family attribute populated.
    dihedral_increment: float, optional
        The scan dihedral increment to use when generating guesses.

    Returns
    -------
    TSAdapter
        The requested TSAdapter object, initialized with the respective reaction information,
    """
    ts_method = _registered_ts_adapters[ts_adapter](user_guesses=user_guesses,
                                                    rmg_reaction=rmg_reaction,
                                                    dihedral_increment=dihedral_increment
                                                    )
    return ts_method
