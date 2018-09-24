Portal Client
=============

The ``FractalClient`` is the primary entry point to a ``FractalServer`` instance.

We can initialize a ``FractalClient`` by pointing it to a server instance. If
you would like to start your own server see the setting up a server (NYI)
section.

.. code-block:: python

    >>> import qcfractal.interface as portal
    >>> client = portal.FractalClient("localhost:8888")
    >>> client
    FractalClient(server='http://localhost:8888/', username='None')

The ``FractalClient`` handles all communication to the server from the Python
API layer and can be used to build more specific handlers. See
(Database/TorsionDriveORM/etc).

The ``FractalClient`` can also be initialized from a file which is useful so
that addresses and username do not have to be retyped for every line and
reduces the chance that a username and password could accidentally be added to
a version control system. Creation from file uses the classmethod
``FractalClient.from_file()``, by default the client searches for a
``qcportal_config.yaml`` file in either the current working directory or from
the canonical |qcarc| folder.


Molecule Handling
-----------------

As an example, we can use a molecule that comes with QCPortal and adds it to
the database as shown. Please note that the Molecule ID (a :term:`DB ID`)
shown below will not be the same as your result and is unique to every
database.

.. code-block:: python

    >>> hooh = portal.data.get_molecule("hooh.json")
    >>> hooh
        Geometry (in Angstrom), charge = 0.0, multiplicity = 1:

           Center              X                  Y                   Z
        ------------   -----------------  -----------------  -----------------
               H          0.977494197627     0.778135098208     0.428565624355
               O          0.694599115267    -0.068915578683    -0.027163830307
               O         -0.694920304666     0.069482110511    -0.026567833892
               H         -0.972396644160    -0.787126321701     0.424194864034


    >>> data = client.add_molecules({"hooh": hooh})
    >>> data
    {'hooh': '5b882c957b87878925ffaf22'}

.. note::

    Adding the same molecule to the database as an idempotent operation and returns the same
    molecule ID.

    .. code-block:: python

        >>> data = client.add_molecules({"hooh": hooh})
        >>> data
        {'hooh': '5b882c957b87878925ffaf22'}

Molecules can either be queried from their Molecule ID or Molecule
hash:

.. code-block:: python

    >>> client.get_molecules([hooh.get_hash()], index="hash")[0]["id"]
    '5b882c957b87878925ffaf22'

    >>> client.get_molecules([data["hooh"]], index="id")[0]["id"]
    '5b882c957b87878925ffaf22'
