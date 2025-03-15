Creating datasets
=================

.. _creating_datasets:

Adding Datasets
---------------

Datasets can be created on a server with the :meth:`~qcportal.client.PortalClient.add_dataset`
function of the :class:`~qcportal.client.PortalClient`. This function returns the dataset object
(such as a :class:`~qcportal.optimization.dataset_models.OptimizationDataset`)

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = client.add_dataset("optimization", "Optimization of important molecules")
      >>> print(ds.id)
      27

The :meth:`~qcportal.client.PortalClient.add_dataset` takes several optional arguments, including
some for descriptions of the dataset as well as default compute priority and :ref:`tags <compute_tags>`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = client.add_dataset("optimization", "Optimization of large molecules",
      ...                          default_compute_tag="large_mem", default_compute_priority="low")
      >>> print(ds.id)
      28


Adding Entries and Specifications
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

The process of submitting is generally as follows.

#. Loop over the given entries and specifications
#. If a record is attached to the dataset for that entry/specification pair, nothing is done
#. If the ``find_existing`` parameter is false, then a new record is created and attached to the dataset
#. If the ``find_existing`` parameter is true (default), then the database is searched for an existing record matching that entry and specification.

   a. If a record is found, then that record is attached to the dataset.
   b. If a record is not found, then a new record is created and attached to the dataset

With no arguments, this will find/create and attach missing records for all entries and specifications, using the
default compute tag and priority of the dataset (set when creating the dataset, or :ref:`modified after <dataset_modify_meta>`).

You may also submit only certain entries and specifications, or change the compute tag and priority of any newly-created records.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds.submit() # Create everything

      >>> # Submit missing difluorine computations with a special tag
      >>> ds.submit(['difluorine'], compute_tag='special_tag')

      >>> # Submit dibromine hf/sto-3g computation at a high priority
      >>> ds.submit(['dibromine'], ['hf/sto-3g'], compute_priority='high')


If there are a lot of records to be created, you may instead submit them using
:meth:`~qcportal.dataset_models.BaseDataset.background_submit`. This will create a background
:ref:`glossary_internal_job` and return an :class:`~qcportal.internal_jobs.models.InternalJob` object that
you can use to monitor the progress (if desired - see :meth:`~qcportal.internal_jobs.models.InternalJob.watch`).

:meth:`~qcportal.dataset_models.BaseDataset.background_submit`. Takes the same arguments as
:meth:`~qcportal.dataset_models.BaseDataset.submit`.


.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ij = ds.background_submit()
      >>> print(ij.progress)
      50

Cloning a dataset
----------------------------

An entire dataset can be cloned using :meth:`PortalClient.clone_dataset() <qcportal.client.PortalClient.clone_dataset>`.
This will clone all entries, specifications, and records. Records themselves are not duplicated, but will be linked to
by both datasets.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> cloned_ds = client.clone_dataset(432, "New dataset name")
      >>> print(cloned_ds.name)
      New dataset name

      >>> print(cloned_ds.id)
      542

      >>> cloned_ds.print_status()
      specification    complete    error    invalid
      -------------  ----------  -------  ---------
             spec_2       13569      401
             spec_6       13926                  44


Copying from another dataset
----------------------------

Entries, specifications, and records can be copied from another dataset of the same type using the following
functions:

* :meth:`~qcportal.dataset_models.BaseDataset.copy_entries_from`
* :meth:`~qcportal.dataset_models.BaseDataset.copy_specifications_from`
* :meth:`~qcportal.dataset_models.BaseDataset.copy_records_from`

These functions take a dataset id, and optionally lists of entry and specification names.

:meth:`~qcportal.dataset_models.BaseDataset.copy_records_from` will copy entries and specifications before copying
the records. The records themselves are not duplicated - instead, the records will be referenced by both datasets.

.. note:

  Entries or specifications will not be overwritten - instead, an exception is raised if entries or specifications
  with the same name as those attempting to be copied already exist.


The below copies some entries and specifications from another dataset

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = client.add_dataset("singlepoint", "New combined dataset")

      >>> # If no entry_names are given, all entries will be copied
      >>> ds.copy_entries_from(432, entry_names=['010 ALA-0', '010 ALA-1', '010 ALA-2'])

      >>> print(ds.entry_names)
      ['010 ALA-0', '010 ALA-1', '010 ALA-2']

      >>> # If no specification_names are given, all specifications will be copied
      >>> ds.copy_specifications_from(432, specification_names=['wb97m-d3bj/def2-tzvppd'])

      >>> print(ds.specification_names)
      ['wb97m-d3bj/def2-tzvppd']

The below copies entries, specifications, and actual records from the other dataset

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = client.add_dataset("singlepoint", "New combined dataset")

      >>> # If no entry_names or specification_names are given, all will be copied from the source
      >>> ds.copy_records_from(432, entry_names=['010 ALA-0', '010 ALA-1', '010 ALA-2'], specification_names=['wb97m-d3bj/def2-tzvppd'])

      >>> print(ds.entry_names)
      ['010 ALA-0', '010 ALA-1', '010 ALA-2']

      >>> # If no specification_names are given, all specifications will be copied
      >>> ds.copy_specifications_from(432, specification_names=['wb97m-d3bj/def2-tzvppd'])

      >>> print(ds.specification_names)
      ['wb97m-d3bj/def2-tzvppd']

      >>> ds.print_status()
               specification    complete
      ----------------------  ----------
      wb97m-d3bj/def2-tzvppd           3


.. note::

  :ref:`Singlepoint datasets <singlepoint_dataset>` have the ability to add entries from other types of
  datasets - see the :ref:`singlepoint documentation <singlepoint_dataset_add_entries_from>`