"""
A module for the abstract TSAdapter class
"""

from abc import ABC, abstractmethod

class TSAdapter(ABC):

    @abstractmethod
    def generate_guesses(self) -> list:
        """
        Generate TS guesses.

        Returns
        -------
        list
            Entries are TS guess dictionaries.
        """
        pass
