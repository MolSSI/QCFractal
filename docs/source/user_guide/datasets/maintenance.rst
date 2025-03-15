Maintenance
===========

.. _dataset_modify_meta:

Modifying metadata and defaults
-------------------------------

Various parameters that are set when :ref:`creating a dataset <creating_datasets>` can be later
modified. These include the name, description, tags, tagline, and the default routing tag & priority.
See the following functions:

* :meth:`~qcportal.dataset_models.BaseDataset.set_name`
* :meth:`~qcportal.dataset_models.BaseDataset.set_description`
* :meth:`~qcportal.dataset_models.BaseDataset.set_tags`
* :meth:`~qcportal.dataset_models.BaseDataset.set_tagline`
* :meth:`~qcportal.dataset_models.BaseDataset.set_provenance`
* :meth:`~qcportal.dataset_models.BaseDataset.set_metadata`
* :meth:`~qcportal.dataset_models.BaseDataset.set_default_compute_tag`
* :meth:`~qcportal.dataset_models.BaseDataset.set_default_compute_priority`


.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

          >>> ds = get_dataset_by_id(123)
          >>> ds.set_default_compute_priority("low")
          >>> ds.set_name("New Dataset Name")
          >>> print(ds.name)
          New Dataset Name

          >>> print(ds.default_priority)
          PriorityEnum.low

          >>> ds = get_dataset_by_id(123)
          >>> ds.set_default_compute_priority("low")
          >>> ds.set_name("New Dataset Name")
          >>> print(ds.name)
          New Dataset Name

          >>> print(ds.default_priority)
          PriorityEnum.low

      >>> ds = get_dataset_by_id(123)
      >>> ds.set_default_priority("low")
      >>> ds.set_name("New Dataset Name")
      >>> print(ds.name)
      New Dataset Name

      >>> print(ds.default_priority)
      PriorityEnum.low


Renaming and Deleting Entries and Specifications
------------------------------------------------

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


.. _dataset_internal_jobs:

Internal Jobs
-------------

Internal jobs associated with a dataset can be listed using :meth:`~qcportal.dataset_models.BaseDataset.list_internal_jobs`.
These are :class:`~qcportal.internal_jobs.models.InternalJob` objects.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds.list_internal_jobs()
      [InternalJob(id=1614530, name='create_attach_view_ds_414', status=<InternalJobStatusEnum.complete: 'complete'>, ...
