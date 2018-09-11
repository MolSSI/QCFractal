"""Mongo QCDB Fragment object and helpers
"""

from .. import molecule
from .collection import Collection


class BioFragment(Collection):
    """
    This is a QCA Fragment class.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    """

    __required_fields = {"initial_molecule"}

    def __init__(self, name, initial_molecule=None, client=None, **kwargs):
        """
        Initializer for the Database object. If no Portal is supplied or the database name
        is not present on the server that the Portal is connected to a blank database will be
        created.

        Parameters
        ----------
        name : str
            The name of the Database
        client : client.FractalClient, optional
            A Portal client to connect to a server

        """
        super().__init__(name, client=client, initial_molecule=initial_molecule)


    def _init_collection_data(self, additional_args):
        mol = additional_args.get("initial_molecule", None)
        if (mol is None):
            raise KeyError("No record of Fragment {} found and no initial molecule passed in.".format(name))

        return {"initial_molecule": mol.to_json()}

    def _pre_save_prep(self, client):
        pass


