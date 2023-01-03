Submitting computations
=======================

Calculations can be submitted to the server with the ``add_`` functions of a client.
These functions generally take input :doc:`molecules <molecule>` and a specification.
It is generally possible to submit many calculations at once by supplying a list of input molecules
rather than a single molecule. This will create one computation per molecule, but all with the same specification.

.. note::

  Some computations (like :doc:`torsiondrives <records/torsiondrive>`) take multiple input molecules
  for a single computation, and therefore adding multiple computations requires a nested list.

.. hint::

  If working with lots of calculations, it is almost always better to use a :doc:`dataset <datasets>`.
  Datasets allow for coordinating large numbers of similar calculations.

The ``add_`` functions return two objects. The first is :class:`qcportal.metadata_models.InsertMetadata`
about the insertion, which describes which records were inserted/created, and which already
existed on the server (see :ref:`record_submit_dedup`).

The second object is a list of record IDs, in the order of the input molecule(s). It is always a list, even
if only one record is added.


.. tabs::
  .. tab:: QCPortal
    .. code-block:: py3

      # Add a single computation
      >>> mol = Molecule(symbols=['H', 'H'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
      >>> meta, ids = client.add_singlepoints(mol, 'psi4', 'energy', 'hf', 'sto-3g')
      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[0], existing_idx=[])

      >>> print(ids)
      [940]

      # Multiple computations
      >>> mol2 = Molecule(symbols=['H', 'H'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 2.0])
      >>> meta, ids = client.add_singlepoints([mol, mol2], 'psi4', 'energy', 'hf', 'sto-3g')
      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[1], existing_idx=[0])

      >>> print(ids)
      [940, 941]

The specifics of the ``add_`` functions depends on the type of computation. :doc:`See the documentation
for different records <records/index>`.

Interpreting metadata
---------------------

The metadata includes information about which records were created or already existing, and which inputs had errors.
The ``_idx`` fields reference the `zero-based index` of the input molecules list.

In the above example, the computation containing `mol` (hydrogen molecule with a bond distance of 1.0 bohr)
already existed. So the `existing_idx` of that is ``0`` (since indexing is zero-based). Similarly for
``inserted_idx``, with stores the index of computations that did not exist on the server, and were created.
In our example, the computation containing the second molecule (hydrogen molecule with a bond distance of 2.0 bohr)
was created, and ``inserted_idx`` for that is ``1``.

The total number of records inserted or existing can also be retrieved with ``n_inserted`` and ``n_existing``.

.. tabs::
  .. tab:: QCPortal
    .. code-block:: py3

      >>> print(meta.n_inserted)
      1
      >>> print(meta.inserted_idx)
      [1]

      >>> print(meta.n_existing)
      1
      >>> print(meta.existing_idx)
      [0]


Lastly, if this is a completely successful operation, then ``meta.success`` will be ``True``. If any errors
happened, these are also stored in the metadata and ``meta.success`` will be ``False``.


.. _record_submit_tags_owners:

Tag & Priority
--------------

The :ref:`routing tag <routing_tags>` and/or priority of a record/task can be specified with the ``add_`` functions as
well. Tasks with a higher priority will be claimed before lower priorities. Priorities can be
specified either as a string or integer (0 = 'low', 1 = 'normal', 2 = 'high'),
or as an enum member (like ``PriorityEnum.high``).

See :class:`~qcportal.record_models.PriorityEnum`.

.. tabs::
  .. tab:: QCPortal
    .. code-block:: py3

      # Add a single computation
      >>> mol = Molecule(symbols=['H', 'H'], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
      >>> meta, ids = client.add_singlepoints(mol, 'psi4', 'energy', 'b3lyp', 'def2-tzvp', tag='small_mem', priority='high')
      >>> print(meta)
      InsertMetadata(error_description=None, errors=[], inserted_idx=[0], existing_idx=[])

      >>> record = client.get_records(ids[0])
      >>> print(record.task.tag)
      'small_mem'



.. _record_submit_dedup:

Record Deduplication
--------------------

New computations that match existing records will not be added; instead, the IDs of the existing
records are returned.

What counts as a duplicate calculation varies depending on the type of calculation, but in general
is quite strict. This means that small differences will likely result in new calculations being added.
Some features that are usually considered are:

* Molecules are identical within a tolerance
* The program to be used in the computation matches
* Basis set and methods exactly match
* Keywords and protocols exactly match

Some things that are **not** considered when finding duplicate calculations:

* status
* tags
* priority

.. note::

  Automatic de-duplication will likely be relaxed in the future, and users will have more
  fine-grained control of when to de-duplicate.
