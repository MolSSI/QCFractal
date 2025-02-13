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

One important thing to keep in mind is that records exist outside of a dataset, and a dataset
references these records. Therefore, `records can be part of multiple datasets`, or not be part of any dataset.
This has implications, for example, when :ref:`submitting calculations <dataset_submission>`.


Dataset Limitations
-------------------

A dataset can only contain one type of calculation. For example, you can have a :ref:`singlepoint_dataset`
or a :ref:`optimization_dataset`, but not a dataset that contains both single point and optimization
calculations.

A specification should work for all entries in the dataset. There is some limited ability to override
keywords on a per-entry basis, but there is no way to assign a different basis for a particular entry.


Listing Datasets
-------------------

Datasets that are currently available on the server can be listed using :meth:`~qcportal.client.PortalClient.list_datasets`.
This returns the dataset information as a list of dictionaries

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.list_datasets()
      [{'id': 54,
        'dataset_type': 'optimization',
        'dataset_name': 'JGI Metabolite Set 1',
        'record_count': 808},
       {'id': 150,
        'dataset_type': 'singlepoint',
        'dataset_name': 'QM7',
        'record_count': 343920},
       {'id': 154,
        'dataset_type': 'singlepoint',
        'dataset_name': 'GDB13-T',
        'record_count': 24000}]


In an interactive environment, the :meth:`~qcportal.client.PortalClient.print_datasets_table` function prints out
a more user-friendly version

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.print_datasets_table()
        id  type            record_count  name
      ----  ------------  --------------  ----------------------------------------------------------
        54  optimization             808  JGI Metabolite Set 1
       150  singlepoint           343920  QM7
       154  singlepoint            24000  GDB13-T


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


Record Status
-------------

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

.. note::

  The contents of the specifications and entries are different for each type of dataset. See
  :doc:`individual record documentation <../records/index>` for the different types.


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