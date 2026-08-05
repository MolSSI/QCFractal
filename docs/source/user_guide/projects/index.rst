Projects
========

A :ref:`project <glossary_project>` is a named container that holds the
:ref:`records <glossary_record>` and :ref:`datasets <glossary_dataset>` belonging to a single
piece of work, together with any files you want to keep alongside them.

Projects are organizational, not computational. Putting a record in a project does not change
how it is computed, and a record or dataset in a project is an ordinary record or dataset -
it can still be retrieved, queried, and managed through the usual client methods. What a project
adds is a place to collect them and a local name for each one, so you can write
``proj.get_record("water_scan")`` instead of remembering that it is record 118326390.

There are two ways something gets into a project. It can be **created** in the project
(:meth:`~qcportal.project_models.Project.add_record`,
:meth:`~qcportal.project_models.Project.add_dataset`), or an existing record or dataset already on
the server can be **linked** into it (:meth:`~qcportal.project_models.Project.link_record`,
:meth:`~qcportal.project_models.Project.link_dataset`). Linking does not copy anything, and the
same record may be linked into more than one project.

.. note::

  Projects were introduced in 0.61 (:pr:`944`) as a beta feature, and the interface is still
  evolving. Record importing (:ref:`project_import_records`) arrived later, in 0.63 (:pr:`972`).

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   records
   datasets


.. _project_creating:

Creating a project
------------------

Projects are created with :meth:`~qcportal.client.PortalClient.add_project`. Only a name is
required. Project names must be unique across the whole server, and are compared
case-insensitively.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj = client.add_project("Peroxide barrier heights")
      >>> print(proj.id)
      7

      >>> print(proj.name)
      Peroxide barrier heights

The remaining arguments are all optional metadata:

- ``description`` - A longer description of the project
- ``tagline`` - A short, one-line description
- ``tags`` - A list of strings for categorizing the project
- ``default_compute_tag`` - The default :ref:`compute tag <glossary_tag>` for computations
  created within this project (defaults to ``*``)
- ``default_compute_priority`` - The default priority for computations created within this
  project (defaults to ``normal``)
- ``extras`` - A dictionary of arbitrary additional information

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from qcportal.record_models import PriorityEnum

      >>> proj = client.add_project(
      ...     "Peroxide barrier heights",
      ...     description="Barrier heights for a set of peroxides, at several levels of theory",
      ...     tagline="Peroxide barriers",
      ...     tags=["peroxide", "barriers"],
      ...     default_compute_tag="big_mem",
      ...     default_compute_priority=PriorityEnum.low,
      ...     extras={"grant": "OAC-1547580"},
      ... )

The ``default_compute_tag`` and ``default_compute_priority`` are inherited by records and datasets
created in the project, which saves passing them on every call. They can still be overridden per
record or per dataset. Note that compute tags are lowercased by the server.

Adding a project whose name already exists raises an error. Pass ``existing_ok=True`` to get the
existing project back instead - useful in a script that may be run more than once.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj = client.add_project("Peroxide barrier heights")
      ---------------------------------------------------------------------------
      PortalRequestError                        Traceback (most recent call last)

      ...

      PortalRequestError: Request failed: Project with name='Peroxide barrier heights' already exists (HTTP status 400)

      >>> proj = client.add_project("Peroxide barrier heights", existing_ok=True)
      >>> print(proj.id)
      7


.. _project_retrieval:

Getting an existing project
---------------------------

A project can be retrieved by name with :meth:`~qcportal.client.PortalClient.get_project`, or by
ID with :meth:`~qcportal.client.PortalClient.get_project_by_id`. Names are matched
case-insensitively.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj = client.get_project("peroxide BARRIER heights")
      >>> print(proj.id)
      7

      >>> proj = client.get_project_by_id(7)
      >>> print(proj.name)
      Peroxide barrier heights

The project's metadata is available as attributes.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> print(proj.tagline)
      Peroxide barriers

      >>> print(proj.tags)
      ['peroxide', 'barriers']

      >>> print(proj.default_compute_tag, proj.default_compute_priority)
      big_mem PriorityEnum.low

      >>> print(proj.owner_user)
      ben

.. note::

  Projects use ``owner_user`` for the user that created them. This is unlike records and
  datasets, where the equivalent field was renamed to ``creator_user`` in 0.61 (:pr:`931`).


.. _project_listing:

Listing projects
----------------

:meth:`~qcportal.client.PortalClient.list_projects` returns a summary of every project on the
server, without fetching the projects themselves. Each entry is a dictionary containing the
project ID and metadata, plus a count of the records and datasets it holds.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for p in client.list_projects():
      ...     print(p["id"], p["record_count"], p["dataset_count"], p["project_name"])
      6 0 3 Diatomic geometries
      7 12 2 Peroxide barrier heights

The keys of each entry are ``id``, ``project_name``, ``tagline``, ``tags``, ``description``,
``record_count``, ``dataset_count``, ``owner_user``, and ``creator_user`` (which currently
duplicates ``owner_user``).

.. note::

  The key holding the name is ``project_name``, not ``name``.


.. _project_status:

Project status
--------------

:meth:`~qcportal.project_models.Project.status` summarizes the state of everything in the project.
It returns a dictionary with two keys - ``records`` and ``datasets`` - each mapping
:class:`record statuses <qcportal.record_models.RecordStatusEnum>` to a count.

The ``records`` entry counts only the records added directly to the project. The ``datasets``
entry counts the records of every dataset in the project, summed together.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.status()
      {'records': {<RecordStatusEnum.complete: 'complete'>: 10,
                   <RecordStatusEnum.error: 'error'>: 2},
       'datasets': {<RecordStatusEnum.complete: 'complete'>: 340,
                    <RecordStatusEnum.waiting: 'waiting'>: 60}}

For a per-dataset breakdown, use the :meth:`~qcportal.dataset_models.BaseDataset.status` method
of the individual dataset instead. See :doc:`datasets`.


.. _project_attachments:

Attachments
-----------

Arbitrary files can be uploaded to a project - notes, plots, input archives, analysis scripts, and
so on. Upload a file with :meth:`~qcportal.project_models.Project.upload_attachment`, which returns
the ID of the new attachment.

The ``attachment_type`` and ``tags`` arguments are both required. Currently the only available
attachment type is ``other``; tags may be an empty list.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> file_id = proj.upload_attachment("./barriers.csv", "other", ["analysis"])
      >>> print(file_id)
      14

      >>> # With a description, and stored under a different name
      >>> proj.upload_attachment("./notes.md", "other", [],
      ...                        description="Working notes",
      ...                        new_file_name="lab_notes.md")
      15

The :attr:`~qcportal.project_models.Project.attachments` property lists the files that have been
uploaded. These are :class:`~qcportal.project_models.ProjectAttachment` objects, which contain the
file metadata but not the contents.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for att in proj.attachments:
      ...     print(att.id, att.file_size, att.file_name, att.tags)
      14 20418 barriers.csv ['analysis']
      15 1044 lab_notes.md []

Download the contents with :meth:`~qcportal.external_files.models.ExternalFile.download`, and
remove an attachment with :meth:`~qcportal.project_models.Project.delete_attachment`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.attachments[0].download("/path/to/barriers.csv")

      >>> proj.delete_attachment(15)

.. note::

  The file size and checksum are verified against the metadata stored on the server when
  downloading. Downloading will not overwrite an existing file unless ``overwrite=True``
  is passed.

.. important::

  Attachments require the server to have file storage (S3) configured. On a server without it,
  uploading will fail.


.. _project_finding:

Finding the project something belongs to
----------------------------------------

Given a record or dataset ID, the
:class:`~qcportal.client.PortalClient` contains the methods :meth:`~qcportal.client.PortalClient.query_project_records` and
:meth:`~qcportal.client.PortalClient.query_project_datasets`. These report which project it is in, and
under what name.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.query_project_records(118326390)
      [{'record_id': 118326390, 'project_id': 7, 'project_name': 'Peroxide barrier heights',
        'record_name': 'water_scan'}]

      >>> client.query_project_datasets([377, 381])
      [{'record_id': 377, 'project_id': 7, 'project_name': 'Peroxide barrier heights',
        'dataset_name': 'b3lyp barriers'},
       {'record_id': 381, 'project_id': 6, 'project_name': 'Diatomic geometries',
        'dataset_name': 'geometries'}]

Both methods accept a single ID or a list. Records and datasets that are not in any project are
simply absent from the result, so the returned list may be shorter than the list of IDs given.

.. note::

  In the result of :meth:`~qcportal.client.PortalClient.query_project_datasets`, the dataset ID
  is under the key ``record_id``, not ``dataset_id``.

Going the other way, the ``query_*`` methods of the client take a ``project_id`` argument to
restrict a record query to a single project. See :doc:`../record_retrieval`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for r in client.query_singlepoints(project_id=7, status='error'):
      ...     print(r.id, r.status)
      118326391 RecordStatusEnum.error
      118326404 RecordStatusEnum.error


.. _project_deleting:

Deleting a project
------------------

:meth:`~qcportal.client.PortalClient.delete_project` deletes the project. By default this deletes
*only* the project - the records and datasets it contained are left on the server, and are
afterwards reachable in the usual way by ID.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.delete_project(7)

The contents can be deleted along with the project using three separate flags:

- ``delete_records`` - also delete the records added directly to the project
- ``delete_datasets`` - also delete the datasets in the project
- ``delete_dataset_records`` - also delete the records held by those datasets

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> # Delete the project and everything in it
      >>> client.delete_project(7,
      ...                       delete_records=True,
      ...                       delete_datasets=True,
      ...                       delete_dataset_records=True)

The project itself is always deleted permanently and cannot be recovered. What happens to its
contents when those flags are set is more subtle, and is described next.


.. _project_deletion_shared:

Shared records and datasets when deleting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``delete_records``, ``delete_datasets``, and ``delete_dataset_records`` request a **hard delete**
(see :ref:`record_status`), not the reversible soft delete. Hard deletes are constrained by the
database: a record cannot be removed while anything still refers to it - another project, a
dataset, or a parent record. This is the same protection described in :doc:`../record_management`.

The practical consequence is that these flags are **best effort**, and they fail quietly:

- A record that is also in another project or in a dataset is **not** deleted. It is unlinked from
  this project and stays on the server.
- You are not told when this happens. The server does determine which records it could not remove,
  but that information is discarded on this code path - the endpoint returns nothing either way.

So a project holding a record that is also used elsewhere cannot take that record away from
everyone else. But equally, do not treat ``delete_records=True`` as a guarantee that the records
are gone.

To confirm what actually happened, look afterwards:

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.delete_project(7, delete_records=True)

      >>> # Was record 118326390 really removed?
      >>> client.get_records(118326390, missing_ok=True)
      None

If you need a definite answer up front, delete the records yourself with
:meth:`~qcportal.client.PortalClient.delete_records` before deleting the project. That method
returns :class:`~qcportal.metadata_models.DeleteMetadata`, which reports the records it could not
remove.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj = client.get_project_by_id(7)
      >>> ids = [rm.record_id for rm in proj.record_metadata]

      >>> meta = client.delete_records(ids, soft_delete=False)
      >>> print(meta.deleted_idx)
      [0, 1, 2]

      >>> print(meta.errors)
      [(3, 'Integrity Error - may still be referenced')]

      >>> client.delete_project(7)

.. warning::

  Deleting a dataset that is linked into **more than one project** behaves differently, and worse.
  The dataset row itself is protected by the same kind of constraint, but that failure is not
  handled - it surfaces as an internal server error rather than being skipped. Check with
  :meth:`~qcportal.client.PortalClient.query_project_datasets` first if a dataset may be shared.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> client.query_project_datasets(381)
        [{'record_id': 381, 'project_id': 6, ...}, {'record_id': 381, 'project_id': 7, ...}]

        >>> # Dataset 381 is in two projects - do not use delete_datasets=True here


.. _project_permissions:

Permissions
-----------

Reading projects requires the ``read`` role; creating, modifying, and deleting them requires
``submit`` (or higher). As with records and datasets, these permissions are not scoped by
ownership - a user with the ``submit`` role may modify or delete any project on the server, not
only their own. See :ref:`server_admin_roles`.


.. _project_qcportal_api:

Projects QCPortal API
---------------------

* :mod:`Project models <qcportal.project_models>`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.add_project`
  * :meth:`~qcportal.client.PortalClient.get_project`
  * :meth:`~qcportal.client.PortalClient.get_project_by_id`
  * :meth:`~qcportal.client.PortalClient.list_projects`
  * :meth:`~qcportal.client.PortalClient.delete_project`
  * :meth:`~qcportal.client.PortalClient.query_project_records`
  * :meth:`~qcportal.client.PortalClient.query_project_datasets`
