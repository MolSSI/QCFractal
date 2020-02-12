"""
A TS adapter for user guesses.
"""

from rmgpy.reaction import Reaction

from qcelemental.molparse.from_string import from_string

from .factory import register_ts_adapter
from .ts_adapter import TSAdapter


class UserAdapter(TSAdapter):
    """
    A class for representing user guesses for a transition state.
    """

    def __init__(self, user_guesses: list = None,
                 rmg_reaction: Reaction = None,
                 dihedral_increment: float = 20,
                 ) -> None:
        """
        Initializes a UserAdapter instance.

        Parameters
        ----------
        user_guesses : list
            TS user guesses.
        rmg_reaction: Reaction, optional
            The RMG Reaction object, not used in the UserAdapter class.
        dihedral_increment: float, optional
            The scan dihedral increment to use when generating guesses, not used in the UserAdapter class.
        """
        if user_guesses is not None and not isinstance(user_guesses, list):
            raise TypeError(f'user_guessed must be a list, got\n'
                            f'{user_guesses}\n'
                            f'which is a {type(user_guesses)}.')
        self.user_guesses = user_guesses

    def __repr__(self) -> str:
        """A short representation of the current UserAdapter.

        Returns
        -------
        str
            The desired representation.
        """
        return f"UserAdapter(user_guesses={self.user_guesses})"

    def generate_guesses(self) -> list:
        """
        Generate TS guesses using the user guesses.

        Returns
        -------
        list
            Entries are TS guess dictionaries.
        """
        if self.user_guesses is None:
            return list()

        results = list()
        for user_guess in self.user_guesses:
            if isinstance(user_guess, str):
                mol = from_string(user_guess)
                geometry, symbols = mol['geom'], mol['elem']
            elif isinstance(user_guess, dict) and 'geom' in user_guess and 'elem' in user_guess:
                geometry, symbols = user_guess['geom'], user_guess['elem']
            else:
                raise TypeError(f'Entries of user_guesses must be wither a string representation of the coordinates, '
                                f'or a dictionary with "geometry" and "symbols" entries.\n'
                                f'got: {user_guess}\n'
                                f'which is a {type(user_guess)}.')
            results.append({'geom': geometry,
                            'elem': symbols,
                            })
        return results


register_ts_adapter('user', UserAdapter)
