Managing Records
=======================

Computations are generally meant to be fire-and-forget. That is, they get submitted, after which they get
run somewhere, and the results stored on the server where they can be queried. When dealing
with lots of computations, though, there will always be a need for a user to manually manage them
in some way. This includes restarting, deleting, cancelling, or otherwise modifying records in some way.

.. hint::

  If working with lots of calculations, it is almost always better to use a :doc:`dataset <datasets>`.
  Datasets allow for coordinating large numbers of similar calculations.

.. _record_status:

Record Statuses
---------------

One important property of records is its :class:`status <qcportal.record_models.RecordStatusEnum>`, which
that represents its state in the server. This is accessed by the :attr:`~qcportal.record_models.BaseRecord.status`
property on any record.

waiting
~~~~~~~~~~~~

The record exists but has not be run, and is waiting to be picked up and run by a
:ref:`compute manager <glossary_manager>` (or, for a :ref:`glossary_service`,
waiting for the first iteration to be run by the :ref:`internal job <glossary_internal_job>` runner).

This is the initial status of a newly-created record.

running
~~~~~~~~~~~~

The record has been claimed by a :ref:`compute manager <glossary_manager>`
and is either being run or has been queued to be run.


complete
~~~~~~~~~~~~

The record has been successfully computed.

After completion, the :doc:`task or service <../overview/tasks_services>` associated with the
record is no longer needed and has been removed.

error
~~~~~~~~~~~~

The record encountered an error while it was being computed.

Errors can have many different causes. These can include unknown methods or basis sets,
computations not converging, or random compute node failures on compute clusters.

The :attr:`~qcportal.record_models.BaseRecord.error` and possibly the
:attr:`~qcportal.record_models.BaseRecord.stdout`/:attr:`~qcportal.record_models.BaseRecord.stderr`
properties may have more details about the error.

For services, this can be an error with iterating the service or an error with one
of its dependency records.

In general, a compute manager disappearing (due to shutting them down or networking errors)
does not lead to an error state. In that case, the record will (eventually) be recycled to
``waiting`` again.

To attempt to run the same record again, use :meth:`~qcportal.client.PortalClient.reset_records`.
The server may also automatically reset errored records - see :ref:`server_configuration_autoreset_error`.

cancelled
~~~~~~~~~~~~

The record was waiting or running, but has been cancelled by the user for some reason.
A cancelled record can be uncancelled again. In the case of a service, the service
will continue from where it left off.

Client functions: :meth:`~qcportal.client.PortalClient.cancel_records` and
:meth:`~qcportal.client.PortalClient.uncancel_records`

deleted
~~~~~~~~~~~~

The record has been soft-deleted.

While a hard-deleted record is completely removed from the database, a user may
opt to "soft" delete the record. This sets the status to ``deleted`` to mark that
the record may be removed in the future.

Client functions: :meth:`~qcportal.client.PortalClient.delete_records` and
:meth:`~qcportal.client.PortalClient.undelete_records`

invalid
~~~~~~~~~~~~

A record that was successfully completed, but after manual review, was discovered to have
some problem with it (for example, converging to an incorrect state). This state must be manually
set by a user.

Typically, such a record should be deleted instead. However, in some cases, it may make sense to
keep the record around (e.g., for historical or reproducibility purposes), but with this state to signal that the
computation can not be trusted.

Client functions: :meth:`~qcportal.client.PortalClient.invalidate_records` and
:meth:`~qcportal.client.PortalClient.uninvalidate_records`


Modifying Records
-----------------

Resetting errored records
~~~~~~~~~~~~~~~~~~~~~~~~~

If a record has a status of ``error``, you can manually reset it back to ``waiting`` so that it will run again.
This can be useful if you think the error may be spurious or random and that running it again will be successful.
You can check what the error is with
the :attr:`~qcportal.record_models.BaseRecord.error`, :attr:`~qcportal.record_models.BaseRecord.stdout`, and
:attr:`~qcportal.record_models.BaseRecord.stderr` properties.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(411)
      >>> print(r.status)
      RecordStatusEnum.error

      >>> meta = client.reset_records(411) # can also take a list
      >>> print(meta)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=0)

The :doc:`metadata <metadata>` is similarly to the metadata returned by ``add_`` functions.
In this case, the only record (index 0) had its status updated back to ``waiting`` and will be picked up/run by a
manager again.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(411)
      >>> print(r.status)
      RecordStatusEnum.waiting


Deleting records
~~~~~~~~~~~~~~~~

Records can be deleted from the server with :meth:`~qcportal.client.PortalClient.delete_records`.

There are a couple important arguments to this function. The first is the option to "soft delete".
Soft deletion means that the record is marked for deletion, but otherwise remains on the server.
This operation can be done with :meth:`~qcportal.client.PortalClient.undelete_records`.
If ``soft_delete=False`` ("hard delete"), then the record is deleted permanently from the server.

.. important::

  A record cannot be hard-deleted if it is being referenced somewhere (another record or a dataset).

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(149)
      >>> print(r.status)
      RecordStatusEnum.complete

      >>> meta = client.delete_records(149, soft_delete=True) # can also take a list
      >>> print(meta)
      DeleteMetadata(error_description=None, errors=[], deleted_idx=[0], n_children_deleted=5)

      >>> r = client.get_records(149)
      >>> print(r.status)
      RecordStatusEnum.deleted

Note that in this example, the child records were also (soft) deleted. This can be controlled with the
``delete_children`` argument to  :meth:`~qcportal.client.PortalClient.delete_records`.
In this case, the record was an optimization, meaning that the trajectory records were (soft) deleted.

We can undo a soft deletion with :meth:`~qcportal.client.PortalClient.undelete_records`

.. tab-set::

  .. tab-item:: PYTHON
    
    .. code-block:: py3

      >>> meta = client.undelete_records(149) # can also take a list
      >>> print(meta)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=5)

Hard deletions are permanent and result in the removal of the record from the server

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> meta = client.delete_records([942, 943], soft_delete=False) # can also take a list
      >>> print(meta)
      DeleteMetadata(error_description=None, errors=[], deleted_idx=[0, 1], n_children_deleted=0)

      >>> r = client.get_records(942, missing_ok=True)
      >>> print(r)
      None

.. warning::

  A record that is deleted with ``soft_delete=False`` is permanently removed and can not be recovered.



Cancelling records
~~~~~~~~~~~~~~~~~~

Records that are ``waiting`` or ``running`` can be cancelled with
:meth:`~qcportal.client.PortalClient.cancel_records`. If they were ``waiting``, then they will no longer
be picked up by a compute manager.

Cancelling can be undone with :meth:`~qcportal.client.PortalClient.uncancel_records`. If the record was ``running``
before it was cancelled, with will go back to a ``waiting state``.

Invalidation can be undone with :meth:`~qcportal.client.PortalClient.uncancel_records`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(411)
      >>> print(r.status)
      RecordStatusEnum.waiting

      >>> meta = client.cancel_records(411) # can also take a list
      >>> print(meta)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=0)

      >>> r = client.get_records(411)
      >>> print(r.status)
      RecordStatusEnum.cancelled

.. note::

  Cancelling a record will also cancel any parent records (for example, if this record was part of a service).

Invalidating records
~~~~~~~~~~~~~~~~~~~~

A completed record can be marked as invalid with :meth:`~qcportal.client.PortalClient.invalidate_records`. This signals
that the record, although "successfully" completed, contains other problems and can not be trusted. Normally,
the record should be deleted, but in some cases it may be useful to keep the record and mark it as invalid instead.

Invalidation can be undone with :meth:`~qcportal.client.PortalClient.uninvalidate_records`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(149)
      >>> print(r.status)
      RecordStatusEnum.complete

      >>> meta = client.invalidate_records(149) # can also take a list
      >>> print(meta)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=0)

      >>> r = client.get_records(149)
      >>> print(r.status)
      RecordStatusEnum.invalid

.. note::

  Invalidating a record will also cancel any parent records (for example, if this record was part of a service).


Changing tag and priority
~~~~~~~~~~~~~~~~~~~~~~~~~

A record's tag and priority can be changed if it has not yet been successfully completed (ie, the task or service
still exists - see :doc:`../overview/tasks_services`).

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(941)
      >>> print(r.task.tag, r.task.priority)
      * PriorityEnum.normal

      >>> meta = client.modify_records(941, new_tag='a_new_tag', new_priority='low')
      >>> print(meta)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=0)

      >>> r = client.get_records(941)
      >>> print(r.task.tag, r.task.priority)
      a_new_tag PriorityEnum.low


Adding comments
~~~~~~~~~~~~~~~

Comments can be added to records with :meth:`~qcportal.client.PortalClient.add_comment`. These are then available
with the :attr:`~qcportal.record_models.BaseRecord.comments` property.

The server will automatically add the time the comment was added and the name of the user adding the comment.

.. tab-set::

  .. tab-item:: PYTHON
    
    .. code-block:: py3

      >>> meta = client.add_comment(149, 'Invalid due to convergence to wrong minimum')
      >>> print(meta)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=0)

      >>> r = client.get_records(149)
      >>> print(r.comments)
      [RecordComment(id=1, record_id=149, username='ben', timestamp=datetime.datetime(2023, 1, 4, 17, 21, 1, 990674),
      comment='Invalid due to convergence to wrong minimum')]