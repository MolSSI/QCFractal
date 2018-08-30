Portal Molecule
===============

The Portal Molecule class can represent many different quantum chemical molecules and also provides the primary manipulation tool for molecules.

Creating Molecules
------------------

Molecules can be created via NumPy, Psi4-like molecule strings, and QCSchema JSON representations.

.. code-block:: python

    >>> mol = portal.Molecule({"symbols": ["He", "He"], "geometry": [0, 0, -3, 0, 0, 3]})
        Geometry (in Angstrom), charge = 0.0, multiplicity = 1:

           Center              X                  Y                   Z
        ------------   -----------------  -----------------  -----------------
              He          0.000000000000     0.000000000000    -1.587531625770
              He          0.000000000000     0.000000000000     1.587531625770

.. note::

    Molecules can automatically be orientated to a common frame of reference using the ``orient=True`` kwarg.
    This is useful for finding identical molecules.

Molecule Hash
-------------

A molecule hash is automatically created to allow each molecule to be uniquely identified. The following keys are used to generate the hash:

 - symbols
 - masses (1.e-6 tolerance)
 - charge (1.e-4 tolerance)
 - multiplicity
 - real
 - geometry (1.e-8 tolerance)
 - fragments
 - fragment_charges (1.e-4 tolerance)
 - fragment_multiplicities
 - connectivity

Hashes can be acquired from any molecule object and a ``FractalServer`` automatically generates canonical hashes when a molecule is added to the database.

.. code-block:: python

    >>> mol.get_hash()
    '84872f975d19aafa62b188b40fbadaf26a3b1f84'

.. note::

    A molecule hash is different from a Molecule ID as the Molecule ID is a
    :term:`DB Index` and reference where the molecule is in the database and
    is different between databases. A molecule hash is a unique molecule
    identifier that can be created and accessed anywhere.


Fragments
---------

The molecule class natively supports fragments. If a molecule is built with fragments, obtained an individual piece is straightforward.

.. code-block:: python

    dimer = portal.Molecule({"symbols": ["He", "He"], "geometry": [0, 0, -3, 0, 0, 3], "fragments": [[0], [1]]})

    >>> dimer.get_fragment(0)
        Geometry (in Angstrom), charge = 0.0, multiplicity = 0:

           Center              X                  Y                   Z
        ------------   -----------------  -----------------  -----------------
              He          0.000000000000     0.000000000000     0.000000000000

Fragments with ghost atoms can be created by adding one more argument to the ``get_fragment`` function. For obtaining
many fragments at once a list can be passed in instead of a number.

.. code-block:: python

    >>> dimer.get_fragment([0], [1])
        Geometry (in Angstrom), charge = 0.0, multiplicity = 0:

           Center              X                  Y                   Z
        ------------   -----------------  -----------------  -----------------
              He          1.587531625770     0.000000000000     0.000000000000
              He(Gh)     -1.587531625770     0.000000000000     0.000000000000