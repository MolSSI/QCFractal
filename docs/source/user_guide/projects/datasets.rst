Datasets in a Project
=====================

A project holds :ref:`datasets <glossary_dataset>` under project-local names, in the same way it
holds records. Every method here that takes a dataset accepts either the name or the ID.

A dataset in a project is an ordinary dataset. Everything in :doc:`../datasets/index` applies to it
unchanged - entries, specifications, submission, caching, and views all work the same way. What
the project adds is the grouping and the name.


.. _project_add_datasets:

Creating datasets
-----------------

:meth:`~qcportal.project_models.Project.add_dataset` creates a new dataset in the project. It takes
the same arguments as :meth:`~qcportal.client.PortalClient.add_dataset` - see
:ref:`creating_datasets` - with the dataset type and name required and the rest optional.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = proj.add_dataset("singlepoint", "b3lyp barriers")
      >>> print(ds.id, ds.name)
      377 b3lyp barriers

      >>> ds.add_specification("psi4/b3lyp/def2-svp", spec)
      >>> ds.add_entry("hooh", hooh_mol)
      >>> ds.submit()

The dataset that comes back is a normal dataset of the appropriate type - a
:class:`~qcportal.singlepoint.dataset_models.SinglepointDataset` here.

Two defaults are inherited from the project unless given explicitly:
``default_compute_tag`` and ``default_compute_priority``. This is the main practical benefit of
creating a dataset inside a project rather than alongside it - the routing settings are set once,
on the project.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj = client.add_project("Peroxide barrier heights",
      ...                           default_compute_tag="big_mem",
      ...                           default_compute_priority="low")

      >>> ds = proj.add_dataset("optimization", "geometries")
      >>> print(ds.default_compute_tag, ds.default_compute_priority)
      big_mem PriorityEnum.low

      >>> # Or override for this one dataset
      >>> ds2 = proj.add_dataset("optimization", "quick geometries",
      ...                        default_compute_tag="small_mem",
      ...                        default_compute_priority="high")

The name must be unique within the project - reusing a name raises a
:class:`~qcportal.client_base.PortalRequestError`.

.. note::

  Dataset names must be unique both within the project and, for a given dataset type, across the
  server. The two checks are separate and ``existing_ok`` only affects the second one.

  ``existing_ok=True`` means "if a dataset of this type and name already exists on the server, add
  *that* dataset to the project instead of creating a new one". It does **not** suppress the error
  from a name that is already used within this project - that check happens first and is
  unconditional. To make a script re-runnable, check
  :attr:`~qcportal.project_models.Project.dataset_metadata` before adding.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> existing = {dm.name for dm in proj.dataset_metadata}
        >>> if "b3lyp barriers" not in existing:
        ...     ds = proj.add_dataset("singlepoint", "b3lyp barriers")
        ... else:
        ...     ds = proj.get_dataset("b3lyp barriers")


.. _project_link_datasets:

Linking existing datasets
-------------------------

:meth:`~qcportal.project_models.Project.link_dataset` associates a dataset that already exists on
the server with the project. Nothing is copied - the project gains a reference to it.

Only the ID is required. By default the dataset keeps its existing name, description, tagline, and
tags.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = proj.link_dataset(381)
      >>> print(ds.name)
      Diatomic geometries

Any of those four can be given to store a project-local value instead.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = proj.link_dataset(381,
      ...                        name="reference geometries",
      ...                        description="Diatomic geometries, used as a reference set",
      ...                        tagline="Reference geometries",
      ...                        tags=["reference"])

      >>> print(ds.name)
      reference geometries

.. important::

  These overrides belong to the project, not to the dataset. The dataset's own name, description,
  tagline, and tags are unchanged, and the project's values are substituted in only when the
  dataset is fetched *through* the project.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> # Through the project - the project's name
        >>> print(proj.get_dataset(381).name)
        reference geometries

        >>> # Directly from the server - the dataset's own name
        >>> print(client.get_dataset_by_id(381).name)
        Diatomic geometries

  A consequence is that the two can drift: renaming the dataset itself with
  :meth:`~qcportal.dataset_models.BaseDataset.set_name` does not update the project's name for it,
  and vice versa. The same applies to records, whose project-local ``name``, ``description``, and
  ``tags`` exist only within the project.

Linking a dataset that is already in this project is an error.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.link_dataset(381)
      ---------------------------------------------------------------------------
      PortalRequestError                        Traceback (most recent call last)

      ...

      PortalRequestError: Request failed: Dataset 381 already linked to project 7 (HTTP status 400)


.. _project_get_datasets:

Getting datasets
----------------

:meth:`~qcportal.project_models.Project.get_dataset` fetches a dataset from the project, by
project-local name or by ID.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = proj.get_dataset("b3lyp barriers")
      >>> print(ds.id)
      377

      >>> # By ID works too
      >>> ds = proj.get_dataset(377)

      >>> print(ds.status())
      {'psi4/b3lyp/def2-svp': {<RecordStatusEnum.complete: 'complete'>: 20}}

From here on it is an ordinary dataset - see :doc:`../datasets/basics`.

.. note::

  Datasets obtained through a project are cached in the same way as those obtained through the
  client. If the client was created with a ``cache_dir``, the dataset's cache file lives there.
  See :doc:`../datasets/caching`.


.. _project_dataset_metadata:

Listing datasets
----------------

The :attr:`~qcportal.project_models.Project.dataset_metadata` property lists the datasets in the
project without fetching them. Each entry is a
:class:`~qcportal.project_models.ProjectDatasetMetadata` object.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for dm in proj.dataset_metadata:
      ...     print(dm.dataset_id, dm.dataset_type, dm.name, dm.tags)
      377 singlepoint b3lyp barriers []
      381 optimization reference geometries ['reference']

This is fetched from the server the first time it is accessed and then cached. Call
:meth:`~qcportal.project_models.Project.fetch_dataset_metadata` to refresh it.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.fetch_dataset_metadata()

Note that, unlike :class:`~qcportal.project_models.ProjectRecordMetadata`, there is no status
field here - a dataset does not have a single status. Use
:meth:`~qcportal.project_models.Project.status` for a project-wide summary
(:ref:`project_status`), or the dataset's own
:meth:`~qcportal.dataset_models.BaseDataset.status` for a per-specification breakdown.


.. _project_unlink_datasets:

Removing datasets
-----------------

:meth:`~qcportal.project_models.Project.unlink_datasets` removes datasets from the project. By
default the datasets stay on the server and only the association with the project is dropped.

It accepts a single name or ID, or a list, and the two may be mixed.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> proj.unlink_datasets("b3lyp barriers")

      >>> proj.unlink_datasets(["reference geometries", 377])

Two flags control how much else goes with it:

- ``delete_datasets`` - also delete the datasets themselves from the server
- ``delete_dataset_records`` - also delete the records held by those datasets

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> # Remove from the project and delete the dataset, but keep its records
      >>> proj.unlink_datasets("b3lyp barriers", delete_datasets=True)

      >>> # Remove from the project and delete the dataset and its records
      >>> proj.unlink_datasets("b3lyp barriers",
      ...                      delete_datasets=True,
      ...                      delete_dataset_records=True)

.. warning::

  These deletions are permanent and affect the server, not just the project.

  If the dataset is linked into another project as well, ``delete_datasets=True`` does not quietly
  skip it - it fails with an internal server error, because the dataset is still referenced.
  Check with :meth:`~qcportal.client.PortalClient.query_project_datasets` first when a dataset may
  be shared.

  ``delete_dataset_records=True`` is best effort in the same way as for records: entries whose
  records are also used by another dataset or project are protected by the database and survive,
  silently. See :ref:`project_deletion_shared`.


Datasets QCPortal API
---------------------

* :class:`~qcportal.project_models.Project`

  * :meth:`~qcportal.project_models.Project.add_dataset`
  * :meth:`~qcportal.project_models.Project.link_dataset`
  * :meth:`~qcportal.project_models.Project.get_dataset`
  * :meth:`~qcportal.project_models.Project.unlink_datasets`
  * :attr:`~qcportal.project_models.Project.dataset_metadata`
  * :meth:`~qcportal.project_models.Project.fetch_dataset_metadata`

* :class:`~qcportal.project_models.ProjectDatasetMetadata`
