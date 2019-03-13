Add/Query Objects
=================

``Molecule``, ``KeywordSet``, ``Collection``, and ``KVStore`` objects are
always added/queried directly to the server unlike compute objects as this
particular set of structures are not acted upon by the server itself.

Adding Objects
--------------

Adding objects to the server uses the ``client.add_*`` commands and takes in a
list of objects to add and returns the :term:`ObjectId` of the object.

.. code-block:: python

    >>> helium = ptl.Molecule.from_data("He 0 0 0")
    >>> data = client.add_molecules([helium])
    ['5b882c957b87878925ffaf22']

Adding the same molecule again will not add a new molecule and will always return the same :term:`ObjectId`:

.. code-block:: python

    >>> helium = ptl.Molecule.from_data("He 0 0 0")
    >>> data = client.add_molecules([helium, helium])
    ['5b882c957b87878925ffaf22', '5b882c957b87878925ffaf22']

The order of :term:`ObjectId` returned is identical to the order of molecules added.

.. note::

    The :term:`ObjectId` changes and is unique to a particular database.

Querying Objects
----------------

Each objects has a set of fields that can be queried to obtain the objects in
addition to their :term:`ObjectId`. All queries will return a list of objects.

Molecules
---------

As an example, we can use a molecule that comes with QCPortal and adds it to
the database as shown. Please note that the Molecule ID (a :term:`ObjectId`)
shown below will not be the same as your result and is unique to every
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



