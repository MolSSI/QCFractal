Molecules
=====================================

The molecule objects used in QCArchive come from the
`QCElemental package <https://docs.qcarchive.molssi.org/projects/QCElemental>`_. This class is also accessible
from ``qcportal.molecules.Molecule``, but is otherwise the same as the
`QCElemental Molecule <https://docs.qcarchive.molssi.org/projects/QCElemental/en/stable/model_molecule.html>`_.

.. _creating_molecules:
Creating Molecules
------------------

A common way to programmatically create a molecule is with the Molecule constructor. With the constructor,
you typically specify the symbols and geometry (coordinates) in bohr:

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from qcportal.molecules import Molecule
      >>> water = Molecule(symbols=['H', 'H', 'O'], geometry=[[0.0, 2.0, 0.0], [2.0, 0.0, 0.0], [0.0, 0.0, 0.0]])
      >>> print(water)
      Molecule(name='H2O', formula='H2O', hash='3a04ba3')

The geometry can be specified as a nested list as above, or simply as a flattened list. The below is equivalent to the
above:

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from qcportal.molecules import Molecule
      >>> water = Molecule(symbols=['H', 'H', 'O'], geometry=[0.0, 2.0, 0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0])
      >>> print(water)
      Molecule(name='H2O', formula='H2O', hash='3a04ba3')

Another option is to read molecules from strings that are in some common format (like XYZ, although it will work
with more):

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> mol_xyz = """
      ...   3
      ...
      ...   H   0.000000 1.000000 0.000000
      ...   H   1.000000 0.000000 0.000000
      ...   O   0.000000 0.000000 0.000000
      ... """
      >>> water = Molecule.from_data(mol_xyz)
      >>> print(water2)
      Molecule(name='H2O', formula='H2O', hash='246998b')

Note that in the case of XYZ files, the units are angstroms.

Similarly, you can construct the molecule from a file. If the above XYZ data was in a file,

.. code-block:: py3

  >>> water = Molecule.from_file('water.xyz')
  >>> print(water2)
  Molecule(name='H2O', formula='H2O', hash='246998b')



Adding molecules to the server
------------------------------

.. hint::

  For the most part, this is not needed. You can pass a Molecule object to functions like
  :meth:`~qcportal.client.PortalClient.add_singlepoints` and they will be automatically added
  to the database as needed. However, in some cases, this may be useful.

Molecules can be added to the server database with `~qcportal.client.PortalClient.add_molecules`. This returns
some metadata about the insertion, and the molecule IDs.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> water = Molecule(symbols=['H', 'H', 'O'], geometry=[0.0, 2.0, 0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0])
      >>> water2 = Molecule(symbols=['H', 'H', 'O'], geometry=[0.0, 2.5, 0.0, 2.5, 0.0, 0.0, 0.0, 0.0, 0.0])
      >>> meta, ids = client.add_molecules([water, water2])
      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[0, 1], existing_idx=[])

      >>> print(ids)
      [585, 586]

These IDs can be passed into functions like :meth:`~qcportal.client.PortalClient.add_singlepoints` instead of
full molecule objects.

Retrieving and Querying Molecules
---------------------------------

The client has two methods for retrieving molecules: :meth:`~qcportal.client.PortalClient.get_molecules` and
:meth:`~qcportal.client.PortalClient.query_molecules`. The :meth:`~qcportal.client.PortalClient.get_molecules`
method is used to get molecules by ID, and returns molecules in the same order as the given ids.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> mols = client.get_molecules([5, 15, 10])
      >>> print(mols[0].id, mols[1].id, mols[2].id)
      5 15 10

You can also specify a single ID and get a single molecule back

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> mol = client.get_molecules(5)
      >>> print(mol.id)
      5


You can also query the molecules in the database with :meth:`~qcportal.client.PortalClient.query_molecules`.
This function returns an :doc:`iterator <query_iterators>`, which you can then use to
iterate over the results. The iterator automatically handles returning batches or pages of query results from the
server.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> query_it = client.query_molecules(molecular_formula='N2')
      >>> for mol in query_it:
      ...    print(mol.id, mol.identifiers.molecular_formula)
      371 N2
      372 N2


.. caution::

   Unlike the ``get_molecules`` function, the molecules from ``query_molecules`` are not in any defined order,
   and the order may be different even with repeated calls with the same arguments



Managing Molecules
------------------

Molecules can be deleted from the server with :meth:`~qcportal.client.PortalClient.delete_molecules`

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> meta = client.delete_molecules([585])
      >>> print(meta)
      DeleteMetadata(error_description=None, errors=[], deleted_idx=[0], n_children_deleted=0)


The server also allows for some limited modification of molecules. This is limited to the name, comment, and
identifiers of the molecule. By default, new identifiers will be merged with the existing identifiers
unless ``overwrite_identifiers=True``, in which case all identifiers will be replaced (that is, identifiers
that are not specified in the call to ``modify_molecules`` will be removed).

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> n2 = Molecule(symbols=['N', 'N'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 2.0],
      ...               name='nitrogen', comment='initial geometry of nitrogen', identifiers={'smiles': 'N#N'})
      >>> _, ids = client.add_molecules([n2])
      >>> print(ids)
      [601]

      >>> meta = client.modify_molecule(601, name='dinitrogen', comment='dinitrogen molecule',
      ...                               identifiers={'pubchem_cid': '947'})
      >>> print(meta)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=0)

      >>> mol = client.get_molecules(601)
      >>> print(mol.name)
      dinitrogen

      >>> print(mol.comment)
      dinitrogen molecule

      >>> print(mol.identifiers.smiles)
      N#N

      >>> print(mol.identifiers.pubchem_cid)
      947
