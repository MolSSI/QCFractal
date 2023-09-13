Interpreting metadata
=====================

Many client functions return metadata. This class includes information about what objects records were created or
already existing on the server, or which objects were deleted, modified, or had errors.

All metadata objects have some common features. Notably, if the operation was completely successful operation,
then ``meta.success`` will be ``True``. If any errors happened, these are also stored in ``meta.errors`` and
``meta.success`` will be ``False``.

Metadata classes also have fields with an ``_idx`` suffix. These are lists that reference the
`zero-based index` of the input list. If we take molecules as an example,

.. tab-set::

  .. tab-item:: PYTHON
    
    .. code-block:: py3

      >>> water = Molecule(symbols=['H', 'H', 'O'], geometry=[0.0, 2.0, 0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0])
      >>> nitrogen = Molecule(symbols=['N', 'N'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 2.0])
      >>> meta, ids = client.add_molecules([water, nitrogen])

      >>> print(meta.success)
      True

      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[0], existing_idx=[1])

In this case, the addition of the two molecules to the server was a success. The first molecule ``water``
did not exist and was added to the server - this is represented by ``0`` in the ``inserted_idx`` attribute
of the metadata. Likewise, the nitrogen molecule already existed (because ``1`` is in the ``existing_idx`` list).

If the ``water`` molecule already existed as well, ``inserted_idx`` would be ``[]`` and ``existing_idx`` would be
``[0, 1]``.

The total number of inserted or existing objects can be retrieved with ``n_inserted`` and ``n_existing``.


Metadata API
------------

.. autoclass:: qcportal.metadata_models.InsertMetadata

.. autoclass:: qcportal.metadata_models.DeleteMetadata

.. autoclass:: qcportal.metadata_models.UpdateMetadata

.. autoclass:: qcportal.metadata_models.TaskReturnMetadata
