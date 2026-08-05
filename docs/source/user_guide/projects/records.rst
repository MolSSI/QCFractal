Records in a Project
====================

A project holds :ref:`records <glossary_record>` under project-local names, so that a record can
be referred to as ``"water_scan"`` rather than by its ID. Every method here that takes a record
accepts either the name or the ID.

There are three ways a record ends up in a project:

- :ref:`Adding <project_add_records>` - a new computation is created and submitted to be run
- :ref:`Linking <project_link_records>` - a record that already exists on the server is
  associated with the project
- :ref:`Importing <project_import_records>` - a computation that was run elsewhere is ingested
  into the server


.. _project_add_records:

Adding records
--------------

:meth:`~qcportal.project_models.Project.add_record` creates a new record in the project and
submits it for computation.

This differs from the client's ``add_*`` methods (see :doc:`../record_submission`) in two ways.
It creates exactly one record rather than one per molecule, and it takes the specification and
input molecule together as a single *record input* object rather than as separate arguments.
The input types are :class:`~qcportal.singlepoint.record_models.SinglepointInput`,
:class:`~qcportal.optimization.record_models.OptimizationInput`, and the equivalent for each
of the other :doc:`computation types <../records/index>`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from qcportal.singlepoint import SinglepointInput

      >>> inp = SinglepointInput(
      ...     molecule=Molecule(symbols=['h', 'h'], geometry=[0, 0, 0, 0, 0, 1.5]),
      ...     specification={
      ...         "program": "psi4",
      ...         "driver": "energy",
      ...         "method": "b3lyp",
      ...         "basis": "def2-svp",
      ...     },
      ... )

      >>> r = proj.add_record("hydrogen", inp)
      >>> print(r.id, r.status)
      118326390 RecordStatusEnum.waiting

Unlike the client's ``add_*`` methods, this returns the record itself rather than metadata and a
list of IDs.

The name must be unique within the project - reusing a name raises a
:class:`~qcportal.client_base.PortalRequestError`. There is no ``existing_ok`` equivalent here,
so check :attr:`~qcportal.project_models.Project.record_metadata` first if a script may be run
more than once.

The optional arguments are keyword-only:

- ``description`` - A longer description of this record within the project
- ``tags`` - A list of strings for categorizing this record within the project
- ``compute_tag`` - The :ref:`compute tag <glossary_tag>` to run with. Defaults to the project's
  ``default_compute_tag``
- ``compute_priority`` - The priority to run at. Defaults to the project's
  ``default_compute_priority``
- ``find_existing`` - If False, always create a new record rather than reusing a matching
  existing one. See :ref:`record_submit_dedup`

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = proj.add_record("hydrogen_fresh", inp,
      ...                     description="Same computation, deliberately not deduplicated",
      ...                     tags=["scratch"],
      ...                     compute_tag="small_mem",
      ...                     compute_priority="high",
      ...                     find_existing=False)

.. note::

  Deduplication applies as usual. With the default ``find_existing=True``, adding a record whose
  computation already exists on the server links the project to that existing record instead of
  running it again - so the returned record may already be ``complete``.


.. _project_link_records:

Linking existing records
------------------------

:meth:`~qcportal.project_models.Project.link_record` associates a record that already exists on
the server with the project. Nothing is copied and the record itself is unchanged; the project
gains a reference to it plus a project-local name, description, and tags. All four arguments are
required.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> meta, ids = client.add_singlepoints(mol, 'psi4', 'energy', 'hf', 'sto-3g')
      >>> r = proj.link_record(ids[0], "reference_hf", "HF reference for comparison", ["reference"])
      >>> print(r.id)
      118326412

The same record may be linked into more than one project. Linking a record that is already in
*this* project is an error.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.link_record(ids[0], "reference_hf_again", "", [])
      ---------------------------------------------------------------------------
      PortalRequestError                        Traceback (most recent call last)

      ...

      PortalRequestError: Request failed: Record 118326412 already linked to project 7 (HTTP status 400)


.. _project_import_records:

Importing records
-----------------

:meth:`~qcportal.project_models.Project.import_record` ingests a computation that was run
somewhere else - on another QCArchive server, or by hand - and stores its results as a record on
this server. The computation is not run again; the results are stored as they are given.

This is the only way to get externally-computed records onto a server, and it works only into a
project.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> # A record retrieved from some other server
      >>> other_client = PortalClient("https://ml.qcarchive.molssi.org")
      >>> external = other_client.get_singlepoints(123, include=['**'])

      >>> r = proj.import_record("imported_hf", external,
      ...                        description="Imported from the ML server",
      ...                        tags=["imported"])

      >>> print(r.status)
      RecordStatusEnum.complete

      >>> print(r.creator_user)
      ben

The imported record is a normal record on this server afterwards, with a new ID. The importing
user becomes its ``creator_user``.

.. note::

  Fetch the source record with ``include=['**']`` before importing it. Only the data actually
  present on the object is transferred, so a partially-fetched record imports partial results.

Records of every computation type can be imported, as can the raw QCSchema results that a compute
manager would return (``AtomicResult``, ``OptimizationResult``, and ``FailedOperation`` from
``qcportal.qcschema_v1``).

.. important::

  Importing is new and still limited. Only individual records can be imported - there is no
  facility for importing a whole dataset - and only into a project. See :pr:`972`.


.. _project_get_records:

Getting records
---------------

:meth:`~qcportal.project_models.Project.get_record` fetches a record from the project, by
project-local name or by ID.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = proj.get_record("hydrogen")
      >>> print(r.id, r.status)
      118326390 RecordStatusEnum.complete

      >>> # By ID works too
      >>> r = proj.get_record(118326390)

      >>> # Fetch all of the record's data up front
      >>> r = proj.get_record("hydrogen", include=['**'])

The record that comes back is an ordinary record of the appropriate type - a
:class:`~qcportal.singlepoint.record_models.SinglepointRecord` here - and behaves exactly as one
retrieved through :meth:`~qcportal.client.PortalClient.get_records`. See :doc:`../records/index`.

.. note::

  Looking a record up by name requires the project's record metadata to have been fetched, which
  happens automatically on first use. Records added during this session are also findable by name.


.. _project_record_metadata:

Listing records
---------------

The :attr:`~qcportal.project_models.Project.record_metadata` property lists what the project
contains without fetching the records themselves. Each entry is a
:class:`~qcportal.project_models.ProjectRecordMetadata` object holding the project-local name,
description, and tags, plus the record's ID, type, and status.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for rm in proj.record_metadata:
      ...     print(rm.record_id, rm.record_type, rm.status, rm.name, rm.tags)
      118326390 singlepoint RecordStatusEnum.complete hydrogen []
      118326412 singlepoint RecordStatusEnum.complete reference_hf ['reference']

This is fetched from the server the first time it is accessed and then cached. The status values
in particular go stale, since they are a snapshot from when the metadata was fetched. Call
:meth:`~qcportal.project_models.Project.fetch_record_metadata` to refresh it.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.fetch_record_metadata()

For an up-to-date count by status across the whole project, use
:meth:`~qcportal.project_models.Project.status` instead - see :ref:`project_status`.


.. _project_unlink_records:

Removing records
----------------

:meth:`~qcportal.project_models.Project.unlink_records` removes records from the project. By
default the records stay on the server and only the association with the project is dropped -
this is the reverse of linking, and it applies equally to records that were added or imported.

It accepts a single name or ID, or a list, and the two may be mixed.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.unlink_records("hydrogen")

      >>> proj.unlink_records(["reference_hf", 118326390])

Pass ``delete_records=True`` to delete the records themselves as well.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.unlink_records("hydrogen_fresh", delete_records=True)

.. note::

  ``delete_records=True`` attempts to remove the record from the server, not just from the project,
  and the deletion is permanent rather than the reversible soft delete.

  It is best effort, though. A record that is also in another project or in a dataset is protected
  by the database and will survive - unlinked from this project, but still on the server - and
  nothing in the return value tells you so. See :ref:`project_deletion_shared`.


Records QCPortal API
--------------------

* :class:`~qcportal.project_models.Project`

  * :meth:`~qcportal.project_models.Project.add_record`
  * :meth:`~qcportal.project_models.Project.import_record`
  * :meth:`~qcportal.project_models.Project.link_record`
  * :meth:`~qcportal.project_models.Project.get_record`
  * :meth:`~qcportal.project_models.Project.unlink_records`
  * :attr:`~qcportal.project_models.Project.record_metadata`
  * :meth:`~qcportal.project_models.Project.fetch_record_metadata`

* :class:`~qcportal.project_models.ProjectRecordMetadata`
