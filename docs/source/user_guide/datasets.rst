Using datasets
==============

A *dataset* is a relatively homogeneous collection of records that allows for
submission and management of a large number of computations.

A dataset is made up of entries, specifications, and records.
It can be thought of as a table, where the *entries* are rows of the
table, and *specifications* are the columns. A cell within the table
(intersection between a row /entry and column/specification) is a :ref:`record <glossary_record>`.

Below is an example of this analogy, where the records are identified by their ID.
For example, record 18263 is an HF/sto-3g computation on water, and
record 23210 is an MP2/cc-pvdz computation on ethanol.

.. table::

  ==============  ==============  =================  =============
    Entry           HF/sto-3g      B3LYP/def2-tzvp    MP2/cc-pvdz
  ==============  ==============  =================  =============
   **water**          18263        18277              18295
   **methane**        19722        19642              19867
   **ethanol**        20212        20931              23210
  ==============  ==============  =================  =============

Using a dataset allows for control of entire rows and columns of the table, and even
the entire table itself. As an example, you can add a new specification, and then easily
submit computations for that specification that apply to all the existing entries.

Dataset entries, specifications, and records are dependent on the type of dataset; different dataset types
have different types for these items. That is, entries in a :ref:`singlepoint_dataset` are different than entries
in an :ref:`optimization_dataset`, and the same is true for specifications.


Dataset Limitations
-------------------

A dataset can only contain one type of calculation. For example, you can have a :ref:`singlepoint_dataset`
or a :ref:`optimization_dataset`, but not a dataset that contains both single point and optimization
calculations.

A specification should work for all entries in the dataset. There is some limited ability to override
keywords on a per-entry basis, but there is no way to assign a different basis for a particular entry.


Retrieving Datasets
-------------------

.. note::

  When retrieving a dataset, only a limited about of data is downloaded first. After that, operations such as
  retrieving entries or records will require contacting the server. This is generally done transparently.

Datasets have unique ID, and a unique name. The unique name only applies to datasets of the same type,
so two datasets can have the same name as long as they are of different types. The names are not case sensitive.

You can retrieve a dataset with via its ID with :meth:`~qcportal.client.PortalClient.get_dataset_by_id`
and its name with :meth:`~qcportal.client.PortalClient.get_dataset`

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = client.get_dataset_by_id(123)
      >>> print(ds.id, ds.dataset_type, ds.name)
      123 singlepoint Organic molecule energies

      >>> ds = client.get_dataset("optimization", "Diatomic geometries")
      >>> print(ds.id, ds.dataset_type, ds.name)
      52 optimization Diatomic geometries


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

Dataset Metadata
--------------------------

Datasets have some useful metadata and properties

* **name**, **description**, **tagline**, **group**, and **tags** are user-defined metadata that categorize this dataset
  among the other datasets
* **default_tag** and **default_priority** are the defaults used when submitting new computations (can be overridden
  in :meth:`~qcportal.dataset_models.BaseDataset.submit`, see :ref:`dataset_submission`).
* **provenance** is a user-defined dictionary with any provenance or source information
* **metadata** is a user-defined dictionary with any other metadata the user wants to attach to the dataset


This metadata is created when the dataset is constructed on the server, but can be changed
with :meth:`~qcportal.dataset_models.BaseDataset.set_name`,
:meth:`~qcportal.dataset_models.BaseDataset.set_description`,
:meth:`~qcportal.dataset_models.BaseDataset.set_metadata`, and so on.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> print(ds.description)
      Optimization of diatomic molecules at different levels of theory

      >>> ds.set_description("A new description")

      >>> # It has been changed on the server
      >>> ds = client.get_dataset_by_id(1)
      >>> print(ds.description)
      A new description


Status
~~~~~~

The :meth:`~qcportal.dataset_models.BaseDataset.status` returns a dictionary describing the status of the computations.
This is indexed by specification

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds.status()
      {'pbe0/sto-3g': {<RecordStatusEnum.complete: 'complete'>: 4,
      <RecordStatusEnum.error: 'error'>: 1},
       'b3lyp/def2-tzvp': {<RecordStatusEnum.error: 'error'>: 1,
      <RecordStatusEnum.complete: 'complete'>: 4},
       'pbe/6-31g': {<RecordStatusEnum.complete: 'complete'>: 3,
      <RecordStatusEnum.error: 'error'>: 2}}

If you are in an interactive session or notebook, or just want a prettier version, you can use
:meth:`~qcportal.dataset_models.BaseDataset.status_table` returns a table as a string, and
:meth:`~qcportal.dataset_models.BaseDataset.print_status` prints a table of the statuses.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds.print_status_()
          specification    complete    error    invalid
      -----------------  ----------  -------  ---------
          pbe/def2-tzvp           3        2
            pbe/sto-3g           4        1
            pbe0/6-31g           4        1
          pbe0/6-31g**           4        1
      pbe0/aug-cc-pvtz           3        1          1
        pbe0/def2-tzvp           4        1
            pbe0/sto-3g           4        1


.. note::

  The status is computed on the server, and does require download all the records. This does mean the the
  status may reflect changes to records that have been retrieved, and so may be out of sync with any
  local caching.


Specifications and Entries
--------------------------

The specifications of the dataset are available with the ``.specification_names`` and ``.specifications`` properties.
``.specifications`` returns a dictionary, with the key being the name of the specification.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> print(ds.specification_names)
      ['hf/sto-3g', 'hf/def2-tzvp']

      >>> print(ds.specifications['hf/sto-3g'])
      name='hf/sto-3g' specification=OptimizationSpecification(program='geometric',
      qc_specification=QCSpecification(program='psi4', driver=<SinglepointDriver.deferred: 'deferred'>, method='hf',
      basis='sto-3g', keywords={'maxiter': 100}, protocols=AtomicResultProtocols(wavefunction=<WavefunctionProtocolEnum.none: 'none'>,
      stdout=True, error_correction=ErrorCorrectionProtocol(default_policy=True, policies=None),
      native_files=<NativeFilesProtocolEnum.none: 'none'>)), keywords={},
      protocols=OptimizationProtocols(trajectory=<TrajectoryProtocolEnum.all: 'all'>)) description=None


Entries are slightly different. Since it is expected that a dataset may have many entries, only the names
are accessible all at once

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> print(ds.entry_names)
      ['H2', 'N2', 'O2', 'F2', 'Hg2']

You can obtain a full entry from its name with :meth:`~qcportal.dataset_models.BaseDataset.get_entry`:

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> print(ds.get_entry)
      OptimizationDatasetEntry(name='H2', initial_molecule=Molecule(name='H2', formula='H2', hash='7746e69'),
      additional_keywords={}, attributes={}, comment=None)

If you need to get all entries, you may iterate over the entries with
:meth:`~qcportal.dataset_models.BaseDataset.iterate_entries`:

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

        >>> for x in ds.iterate_entries():
        ...    print(x.initial_molecule)
        Molecule(name='H2', formula='H2', hash='7746e69')
        Molecule(name='N2', formula='N2', hash='609abf3')
        Molecule(name='O2', formula='O2', hash='018caee')
        Molecule(name='F2', formula='F2', hash='7ffa835')
        Molecule(name='Hg2', formula='Hg2', hash='a67cb93')

:meth:`~qcportal.dataset_models.BaseDataset.iterate_entries` can also be restricted to only iterate over certain
entry names.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

        >>> for x in ds.iterate_entries(entry_names=['H2', 'O2']):
        ...    print(x.initial_molecule)
        Molecule(name='H2', formula='H2', hash='7746e69')
        Molecule(name='O2', formula='O2', hash='018caee')


Retrieving Records
------------------

A single records can be retrieved by entry name and specification name
with :meth:`~qcportal.dataset_models.BaseDataset.get_record`

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

        >>> rec = ds.get_record('H2', 'hf/sto-3g')
        >>> print(rec)
        <OptimizationRecord id=3 status=complete>

        >>> print(rec.final_molecule)
        Molecule(name='H2', formula='H2', hash='6c7a0a9')

Multiple records (or all records) can be obtained by using the iterator returned from
:meth:`~qcportal.dataset_models.BaseDataset.iterate_records`. The iterator return a tuple of three
values - the entry name, specification name, and then the full record.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for e_name, s_name, record in ds.iterate_records():
      ...   print(e_name, s_name, record.id, record.status)
      H2 hf/sto-3g 3 RecordStatusEnum.complete
      N2 hf/sto-3g 1 RecordStatusEnum.complete
      O2 hf/sto-3g 4 RecordStatusEnum.complete
      F2 hf/sto-3g 5 RecordStatusEnum.complete
      Hg2 hf/sto-3g 2 RecordStatusEnum.error
      H2 hf/def2-tzvp 8 RecordStatusEnum.complete
      N2 hf/def2-tzvp 9 RecordStatusEnum.complete
      O2 hf/def2-tzvp 6 RecordStatusEnum.complete


:meth:`~qcportal.dataset_models.BaseDataset.iterate_records` also has filtering options, including
by entry name, specification name, and status

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for e_name, s_name, record in ds.iterate_records(status='error'):
      ...   print(e_name, s_name, record.id, record.status)
      Hg2 hf/sto-3g 2 RecordStatusEnum.error
      Hg2 hf/def2-tzvp 10 RecordStatusEnum.error
      Hg2 hf/6-31g 15 RecordStatusEnum.error
      Hg2 hf/6-31g** 17 RecordStatusEnum.error

If the record was previously retrieved, it won't be retrieved again unless it has been updated on the server. This can
be overridden with ``force_refetch=True`` which will always download a fresh record.


Manipulating Entries and Specifications
---------------------------------------

Entries and specifications can be added with ``add_entries``, ``add_entry``, and ``add_specification``.
The details of these functions depend on the type of dataset - see :doc:`records/index`. The following examples
are for an :ref:`optimization dataset <optimization_dataset>`.

Both entries and specifications are given descriptive names, which can be use later in other functions
(like :meth:`~qcportal.dataset_models.BaseDataset.get_record`).

When entries or specifications are added, the changes are reflected immediately on the server.

First, we add some entries to this dataset. For an optimization dataset, an entry corresponds to
an unoptimized 'initial' molecule. Adding entries returns :doc:`metadata <metadata>`.

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

Records that belong to the dataset can be managed via the usual client methods (see :doc:`record_management`).
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