Query Iterators
===============

Query functions typically return objects that behave like `iterators <https://wiki.python.org/moin/Iterator>`_.
These objects do not allow for direct access to individual items (like ``iter[0]``), but do allow for looping through
them.

The reason for returning an iterator rather than a list is that an iterator will handle batching to the server. Since
a query can return many items, they must be retrieved in batches. The iterator does this automatically, but does so
incrementally as iteration continues.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> query_it = client.query_molecules(molecular_formula='N2')
      >>> for mol in query_it:
      ...    print(mol.id, mol.identifiers.molecular_formula)
      371 N2
      372 N2

If you need all items as a list, then you can use the ``list`` constructor, which will use the iterator to fill in the
list. In this case, all the records will be fetched from the server as the list is being created.

.. tab-set::

  .. tab-item:: PYTHON
    
    .. code-block:: py3

      >>> query_it = client.query_molecules(molecular_formula='N2')
      >>> mols = list(query_it)
      >>> print(len(mols))
      621


Iterators API
-------------

.. autoclass:: qcportal.molecules.models.MoleculeQueryIterator

.. autoclass:: qcportal.record_models.RecordQueryIterator
