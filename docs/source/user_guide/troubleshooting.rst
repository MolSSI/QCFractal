Troubleshooting
===============

This page collects the questions that come up most often when computations do not behave as
expected, and the client methods that answer them.

Most problems fall into one of three categories: the computation is not being picked up, the
computation ran and failed, or the client cannot talk to the server.


.. _troubleshooting_waiting:

A record is stuck in "waiting"
------------------------------

A record with a status of ``waiting`` has been created but it is not being claimed. See
:ref:`record_status`. This is normal for a short while, but a record that stays ``waiting``
indefinitely means no :ref:`compute manager <glossary_manager>` is able to take its
:ref:`task <glossary_task>`.

The server can answer this directly. :meth:`~qcportal.client.PortalClient.get_waiting_reason`
takes a record ID and reports why the record has not been claimed, checking each active manager in
turn. :meth:`~qcportal.record_models.BaseRecord.get_waiting_reason` does the same from a record
object.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.get_waiting_reason(118326390)
      {'reason': 'No manager matches programs & tags',
       'details': {'test_cluster-a_host1-1234-5678': "Manager missing programs: {'psi4'}",
                   'test_cluster-a_host2-8765-4321': 'Manager does not handle tag "big_mem"'}}

      >>> # Or from the record
      >>> r = client.get_records(118326390)
      >>> r.get_waiting_reason()
      {'reason': 'Waiting for a free manager',
       'details': {'test_cluster-a_host1-1234-5678': 'Manager is busy'}}

The ``reason`` key is always present and holds the overall answer. When the record really is
waiting and there is at least one active manager, a ``details`` key is also present, mapping each
active manager's name to why that particular manager cannot take the task.

The possible values of ``reason`` are:

.. table::

  ==========================================  =================================================================
   Reason                                      What it means
  ==========================================  =================================================================
   ``Record does not exist``                   No record with that ID on the server
   ``Record is not waiting``                   The record's status is something other than ``waiting``
   ``Record is a service``                     Services are iterated by the server, not claimed by a
                                               manager. See :ref:`troubleshooting_service`
   ``No active managers``                      No manager is currently connected to the server
   ``No manager matches programs & tags``      Managers are active, but none has both the required programs
                                               and a matching compute tag. Check ``details``
   ``Waiting for a free manager``              A manager could take this task but is at capacity. This is
                                               normal - the task should run once the manager has room
  ==========================================  =================================================================

The two entries to act on are the ``details`` messages:

**Manager missing programs** - the task requires a program the manager does not have installed.
The required program comes from the record's specification, so a singlepoint with
``program="psi4"`` needs a manager with psi4 available. Managers report what they have, which you
can check with :meth:`~qcportal.client.PortalClient.query_managers`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for m in client.query_managers(status='active'):
      ...     print(m.name, m.compute_tags, sorted(m.programs.keys()))
      test_cluster-a_host1-1234-5678 ['big_mem'] ['geometric', 'qcengine']
      test_cluster-a_host2-8765-4321 ['*'] ['psi4', 'qcengine', 'rdkit']

**Manager does not handle tag** - the record's :ref:`compute tag <glossary_tag>` is not one the
manager is configured to claim. Note that a task tagged ``*`` is claimed *only* by a manager that
also has ``*``; the wildcard is not a "match anything" on the task side. See :ref:`compute_tags`.

A record's tag can be changed after the fact with
:meth:`~qcportal.client.PortalClient.modify_records`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(118326390)
      >>> print(r.task.compute_tag, r.task.required_programs)
      big_mem ['psi4', 'qcengine']

      >>> client.modify_records(118326390, new_compute_tag='*')

:meth:`~qcportal.client.PortalClient.query_active_managers` answers the same question ahead of
time - given a tag and a set of programs, which managers could take such a task.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.query_active_managers(compute_tag=['big_mem'], programs={'psi4': []})
      []

      >>> client.query_active_managers(compute_tag=['*'], programs={'psi4': []})
      ['test_cluster-a_host2-8765-4321']

.. note::

  ``get_waiting_reason`` only considers managers the server currently regards as *active*. A
  manager that has stopped reporting in is eventually marked inactive, and any tasks it had
  claimed are returned to ``waiting`` to be claimed by someone else.


.. _troubleshooting_errors:

A record has an error
---------------------

A record with a status of ``error`` ran and failed. The details are on the record itself.

- :attr:`~qcportal.record_models.BaseRecord.error` - a dictionary with ``error_type`` and
  ``error_message``. Usually the most useful thing to read
- :attr:`~qcportal.record_models.BaseRecord.stdout` - the program's output, if it was stored
- :attr:`~qcportal.record_models.BaseRecord.stderr` - anything written to stderr

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(118326390)
      >>> print(r.status)
      RecordStatusEnum.error

      >>> print(r.error["error_type"])
      unknown_error

      >>> print(r.error["error_message"])
      QCEngine Unknown Error:
      ...
      psi4.driver.p4util.exceptions.SCFConvergenceError: Could not converge SCF iterations in 200 iterations.

A record may have been attempted more than once. Each attempt is an entry in
:attr:`~qcportal.record_models.BaseRecord.compute_history`, and the top-level ``error``,
``stdout``, and ``stderr`` reflect the most recent one. Earlier entries are useful when a record has
been reset and failed differently each time, and the ``manager_name`` on each entry tells you
*where* it failed - a program failing on one cluster but not another is a strong signal.
See :doc:`records/base`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for h in r.compute_history:
      ...     print(h.modified_on, h.status, h.manager_name)
      2026-04-19 15:14:31.128937+00:00 RecordStatusEnum.error test_cluster-a_host1-1234-5678
      2026-04-21 09:02:11.551204+00:00 RecordStatusEnum.error test_cluster-a_host2-8765-4321

To find the errored records among many, query by status, or use a dataset's status methods.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for r in client.query_records(status='error', limit=10):
      ...     print(r.id, r.record_type, r.error["error_type"])
      118326390 singlepoint unknown_error
      118326404 optimization compute_error

      >>> # Within a dataset
      >>> ds = client.get_dataset_by_id(377)
      >>> ds.print_status()
        specification    complete    error
      ---------------  ----------  -------
       b3lyp/def2-svp          18        2

If an error looks transient - a node failure, a network problem, a queue timeout - reset the record
to run it again with :meth:`~qcportal.client.PortalClient.reset_records`. A server may also be
configured to reset errored records automatically; see
:ref:`server_configuration_autoreset_error`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.reset_records(118326390)
      UpdateMetadata(error_description=None, errors=[], updated_idx=[0], n_children_updated=0)

.. hint::

  If an error is *not* transient - an unknown method, a basis set the program does not have, a
  molecule that will never converge - resetting will just fail again. Fix the specification and
  submit a new record instead.

.. note::

  A compute manager disappearing does not normally produce an ``error``. Those records go back to
  ``waiting`` instead, and there is nothing to reset.


.. _troubleshooting_service:

A service is not progressing
----------------------------

A :ref:`service <glossary_service>` - a torsiondrive, gridoptimization, NEB, manybody, or reaction
record - is iterated by the server rather than claimed by a manager, so
:meth:`~qcportal.client.PortalClient.get_waiting_reason` will only tell you
``Record is a service``. See :doc:`../overview/tasks_services`.

A service alternates between waiting on the records it created and iterating. The usual reason it
appears stalled is that one of those dependencies is itself stuck or errored.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(118326390)
      >>> print(r.is_service)
      True

      >>> # What is it waiting on?
      >>> dep_ids = [d.record_id for d in r.service.dependencies]
      >>> for dep in client.get_records(dep_ids):
      ...     print(dep.id, dep.status)
      118326391 RecordStatusEnum.complete
      118326392 RecordStatusEnum.error

Once a stuck dependency is dealt with, the service will pick up again on its next iteration.
Services also write progress to :attr:`~qcportal.record_models.BaseRecord.stdout` as they run,
which is worth reading - unlike task-based records, this is updated while the record is still
running.

Two server settings limit how quickly services iterate: ``max_active_services`` caps how many run
at once, and ``service_frequency`` sets how often the server looks for services to iterate. On a
busy server a service may simply be queued behind others. See :ref:`server_configuration`.


.. _troubleshooting_connection:

The client cannot reach the server
----------------------------------

Failed requests raise :class:`~qcportal.client_base.PortalRequestError`, which carries the HTTP
status code and the server's message.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from qcportal import PortalRequestError

      >>> try:
      ...     r = client.get_records(1)
      ... except PortalRequestError as e:
      ...     print(e.status_code)
      ...     print(e.msg)
      404
      Request failed: Could not find record with id 1

Common cases:

- **401 / 403** - not logged in, or the role does not permit the operation. Check the username and
  password in use, and see :ref:`overview_roles` for what each role allows
- **404** - the record, dataset, or project does not exist. Many client methods accept
  ``missing_ok=True`` to return ``None`` instead of raising
- **400** - the server rejected the request. The message is usually specific
- **500** - an internal server error. See :ref:`troubleshooting_server_errors`

If the connection fails outright rather than returning a status code, the address or the
credentials are the place to start. :meth:`~qcportal.client.PortalClient.get_server_information`
is a cheap way to confirm you are talking to the server you think you are.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> print(client.address)
      https://ml.qcarchive.molssi.org/

      >>> info = client.get_server_information()
      >>> print(info["version"])
      0.65

See :doc:`client_setup` and :doc:`connecting_qcportal` for how the address and credentials are
resolved, including from configuration files and environment variables.


.. _troubleshooting_slow:

Retrieval is slow
-----------------

Repeatedly fetching the same records is the most common cause of a slow script.

- Enable an on-disk cache by passing ``cache_dir`` to the
  :class:`~qcportal.client.PortalClient` constructor. Records and datasets are then reused
  across runs instead of being downloaded again. See :doc:`datasets/caching`
- Fetch in bulk. :meth:`~qcportal.client.PortalClient.get_records` takes a list of IDs and
  retrieves them in one request; a loop of single-ID calls does one request each
- Use ``include=['**']`` when you know you will need all of a record's data, so it arrives in the
  initial fetch rather than as a separate request per field later
- For a finished dataset, a :ref:`view <dataset_views>` is a single local file with no server
  round-trips at all

.. hint::

  Queries return an :doc:`iterator <query_iterators>`, not a list, and fetch in batches as you
  iterate. Calling ``list()`` on a query with no ``limit`` will try to pull everything.


.. _troubleshooting_server_errors:

Internal server errors
----------------------

Errors inside the server itself are recorded in a separate log from record errors. By default the
server does not show the details to users - the ``hide_internal_errors`` setting - and instead
returns an error ID to quote to an administrator. See :ref:`server_configuration`.

Users with sufficient permissions can read that log with
:meth:`~qcportal.client.PortalClient.query_error_log`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> for e in client.query_error_log(limit=5):
      ...     print(e.id, e.error_date, e.user, e.request_path)
      12 2026-08-01 14:22:03.118293+00:00 ben /api/v1/records/bulkGet?

      >>> # Or look up the ID a user was given
      >>> entry = next(iter(client.query_error_log(error_id=12)))
      >>> print(entry.error_text)
      Traceback (most recent call last):
      ...

Entries are :class:`~qcportal.serverinfo.models.ErrorLogEntry` objects, and the query can be
filtered by ``error_id``, ``user``, ``before``, and ``after``. Old entries are removed with
:meth:`~qcportal.client.PortalClient.delete_error_log`, which takes a cutoff date and returns the
number deleted.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> from datetime import datetime
      >>> client.delete_error_log(datetime(2026, 1, 1))
      41


Troubleshooting API
-------------------

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.get_waiting_reason`
  * :meth:`~qcportal.client.PortalClient.query_managers`
  * :meth:`~qcportal.client.PortalClient.query_active_managers`
  * :meth:`~qcportal.client.PortalClient.reset_records`
  * :meth:`~qcportal.client.PortalClient.modify_records`
  * :meth:`~qcportal.client.PortalClient.get_server_information`
  * :meth:`~qcportal.client.PortalClient.query_error_log`
  * :meth:`~qcportal.client.PortalClient.delete_error_log`

* :meth:`~qcportal.record_models.BaseRecord.get_waiting_reason`
* :class:`~qcportal.client_base.PortalRequestError`
* :class:`~qcportal.serverinfo.models.ErrorLogEntry`
