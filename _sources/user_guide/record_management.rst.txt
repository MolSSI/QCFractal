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

A record that was successfully completed, but after manual (human) review, was discovered to have
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

todo

Deleting records
~~~~~~~~~~~~~~~~

todo

.. warning::

  A record that is deleted with ``soft_delete=False`` is gone permanently, and can not be recovered.



Invalidating, and Cancelling records
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

todo

Adding comments
~~~~~~~~~~~~~~~

todo
