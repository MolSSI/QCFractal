Add/Query Objects
=================

Unlike the ``Compute`` object, the ``Molecule``, ``KeywordSet``, ``Collection``, and ``KVStore``
objects are always added/queried directly to the server instances as these structures
are not acted upon by the server itself.

Adding Objects to the Server
----------------------------

A list of objects can be added to the server via ``client.add_*`` commands
which return the :term:`ObjectId` of the object instance.

.. code-block:: python

    >>> helium = ptl.Molecule.from_data("He 0 0 0")
    >>> data = client.add_molecules([helium])
    ['5b882c957b87878925ffaf22']

Any attempt to add the same molecule again will not proceed and the same :term:`ObjectId`
will always be returned:

.. code-block:: python

    >>> helium = ptl.Molecule.from_data("He 0 0 0")
    >>> data = client.add_molecules([helium, helium])
    ['5b882c957b87878925ffaf22', '5b882c957b87878925ffaf22']

Note that the order of :term:`ObjectId`'s are identical to the order of molecules
in the input list.

.. note::

    The :term:`ObjectId` can change between object instances but 
    it is unique within a particular database.

Querying Objects from the Server
--------------------------------

Each server object has a set of fields that can be queried to obtain the object instances in
addition to their :term:`ObjectId`. All queries will return a list of objects.

Interacting with Molecules on the Server
----------------------------------------

As an example, we can use a molecule that comes with QCPortal and add it to
the database. Please note that the Molecule ID (a :term:`ObjectId`)
shown below will not be the same as your result and will be unique for each
database.

.. code-block:: python

    >>> hooh = ptl.Molecule.from_data("""
    >>>        H          1.8486716127,  1.472346669,  0.644643566
    >>>        O          1.3127881568, -0.130419379, -0.211892270
    >>>        O         -1.3127927010,  0.133418733, -0.211896415
    >>>        H         -1.8386801669, -1.482348324,  0.644636970
    >>>        """)
    >>> hooh
        Geometry (in Angstrom), charge = 0.0, multiplicity = 1:

           Center              X                  Y                   Z
        ------------   -----------------  -----------------  -----------------
               H          0.977494197627     0.778135098208     0.428565624355
               O          0.694599115267    -0.068915578683    -0.027163830307
               O         -0.694920304666     0.069482110511    -0.026567833892
               H         -0.972396644160    -0.787126321701     0.424194864034


    >>> data = client.add_molecules([hooh])
    >>> data
    ['5c82c51895d5923b946989c1']


Molecules can either be queried from their Molecule ID or Molecule
hash:

.. code-block:: python

    >>> client.query_molecules(molecule_hash=[hooh.get_hash()])[0].id
    '5c82c51895d5923b946989c1'

    >>> client.query_molecules(id=data)[0].id
    '5c82c51895d5923b946989c1'



