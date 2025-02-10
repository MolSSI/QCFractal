Creating datasets
=================

Adding Datasets
---------------

Datasets can be created on a server with the :meth:`~qcportal.client.PortalClient.add_dataset`
function of the :class:`~qcportal.client.PortalClient`. This function returns the dataset:

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = client.add_dataset("optimization", "Optimization of important molecules")
      >>> print(ds.id)
      27

The :meth:`~qcportal.client.PortalClient.add_dataset` takes several optional arguments, including
some for descriptions of the dataset as well as default priority and :ref:`tags <routing_tags>`.



Manipulating Entries and Specifications
---------------------------------------

Entries and specifications can be added with ``add_entries``, ``add_entry``, and ``add_specification``.
The details of these functions depend on the type of dataset - see :doc:`../records/index`. The following examples
are for an :ref:`optimization dataset <optimization_dataset>`.

Both entries and specifications are given descriptive names, which can be use later in other functions
(like :meth:`~qcportal.dataset_models.BaseDataset.get_record`).

When entries or specifications are added, the changes are reflected immediately on the server.

First, we add some entries to this dataset. For an optimization dataset, an entry corresponds to
an unoptimized 'initial' molecule. Adding entries returns :doc:`metadata <../metadata>`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from qcportal.molecules import Molecule
      >>> from qcportal.optimization import OptimizationDatasetEntry

      >>> mol = Molecule(symbols=['C', 'O'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 2.0])
      >>> meta = ds.add_entry("carbon monoxide", mol)
      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[0], existing_idx=[])

      >>> # Can also create lots of entries and add them at once
      >>> mol2 = Molecule(symbols=['F', 'F'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 2.0])
      >>> mol3 = Molecule(symbols=['Br', 'Br'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 2.0])
      >>> entry2 = OptimizationDatasetEntry(name='difluorine', initial_molecule=mol2)
      >>> entry3 = OptimizationDatasetEntry(name='dibromine', initial_molecule=mol3)
      >>> meta = ds.add_entries([entry2, entry3])
      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[0, 1], existing_idx=[])

Now our dataset has three entries

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> print(ds.entry_names)
      ['carbon monoxide', 'difluorine', 'dibromine']

Next, we will add some specifications. For an optimization dataset, this is an
:class:`~qcportal.optimization.OptimizationSpecification`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from qcportal.singlepoint import SinglepointSpecification
      >>> from qcportal.optimization import OptimizationSpecification

      >>> # Use geometric, compute gradients with psi4. Optimize with b3lyp/def2-tzvp
      >>> spec = OptimizationSpecification(
      ...   program='geometric',
      ...   qc_specification=QCSpecification(
      ...     program='psi4',
      ...     driver='deferred',
      ...     method='b3lyp',
      ...     basis='def2-tzvp',
      ...   )
      ... )

      >>> meta = ds.add_specification(name='psi4/b3lyp/def2-tzvp', specification=spec)
      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[0], existing_idx=[])


.. _dataset_submission:

Submitting Computations
-----------------------

Adding entries and specifications does not immediately create the underlying records. To do that,
we use :meth:`~qcportal.dataset_models.BaseDataset.submit`.

With no arguments, this will create missing records for all entries and specifications, using the
default tag and priority of the dataset. However, you may also submit only certain entries and specifications,
or change the tag and priority.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds.submit() # Create everything

      >>> # Submit missing difluorine computations with a special tag
      >>> ds.submit(['difluorine'], tag='special_tag')

      >>> # Submit dibromine hf/sto-3g computation at a high priority
      >>> ds.submit(['dibromine'], ['hf/sto-3g'], priority='high')


Renaming and Deleting Entries and Specifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Entries and specifications can be renamed and deleted. Deletion can also optionally delete the underlying record.

:meth:`~qcportal.dataset_models.BaseDataset.rename_entries` and
:meth:`~qcportal.dataset_models.BaseDataset.rename_specification`
take a dictionary of old name to new name

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds.rename_entries({'difluorine': 'F2 molecule'})
      >>> ent = ds.get_entry('F2 molecule')
      >>> print(ent.initial_molecule)
      initial_molecule=Molecule(name='F2', formula='F2', hash='7ffa835')

Entries and specifications are deleted with
:meth:`~qcportal.dataset_models.BaseDataset.delete_entries` and
:meth:`~qcportal.dataset_models.BaseDataset.delete_specification`.
Note that deleting entries and specifications by default do not delete the records

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> # Keeps any records, but removes from dataset
      >>> ds.delete_entries(['carbon monoxide'])

      >>> # Deletes the records too
      >>> ds.delete_specification('hf/sto-3g', delete_records=True)


Record Management
-----------------

Records that belong to the dataset can be managed via the usual client methods (see :doc:`../record_management`).
However, datasets have convenient methods for management, which use entry and specification names rather than record.

* :meth:`~qcportal.dataset_models.BaseDataset.modify_records`
* :meth:`~qcportal.dataset_models.BaseDataset.reset_records`
* :meth:`~qcportal.dataset_models.BaseDataset.cancel_records` and :meth:`~qcportal.dataset_models.BaseDataset.uncancel_records`
* :meth:`~qcportal.dataset_models.BaseDataset.invalidate_records` and :meth:`~qcportal.dataset_models.BaseDataset.uninvalidate_records`

These functions are similar to the client counterparts, but instead use entry and specification names.

In addition, individual records can be removed from a dataset (and optionally deleted) with
:meth:`~qcportal.dataset_models.BaseDataset.remove_records`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> # Reset carbon monoxide records
      >>> ds.reset_records(entry_names=['carbon monoxide'])

      >>> # Cancel pbe0/def2-qzvp computations
      >>> ds.cancel_records(specification_names=['pbe0/def2-qzvp'])
