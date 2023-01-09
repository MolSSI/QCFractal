Retrieving records
==================

A *record* is a representation of a single computation that is stored on the server. Records are generally
not created by hand by the user, but retrieved from the server.

Records contain an ID that uniquely identifies this record on the server.

Using the :class:`~qcportal.client.PortalClient`, records are retrieved from the server in two ways.
The first is through ``get_`` functions,  which is used to retrieve records by their ID.
These functions (such as :func:`~qcportal.client.PortalClient.get_records` and :func:`~qcportal.client.PortalClient.get_singlepoints`)
take a list or other sequence of IDs, and returns a list of records in the same order as the given IDs

.. tab-set::

  .. tab-item:: QCPortal

    .. code-block:: py3

      >>> records = client.get_records([245, 827])
      >>> print(records[0].id, records[1].id)
      245 827

  .. tab-item:: Raw API

    .. code-block:: http

      POST /api/v1/records/bulkGet HTTP/1.1
      Content-Type: application/json
      Accept: application/json

      {
         "ids": [245, 246]
      }

    .. code-block:: json
      :force:

      [
        {
           "id": 245,
           "record_type": "singlepoint",
           "status": "complete",
           "is_service": false,
           "manager_name": null,
           "modified_on": "2019-03-07T19:45:27.745000",
           "molecule_id": 37,
           "owner_group": null,
           "owner_user": null,
           ...
        },
        {
           "id": 246,
           "record_type": "singlepoint",
           "status": "complete",
           "is_service": false,
           "manager_name": null,
           "modified_on": "2019-03-07T19:45:42.748000",
           "molecule_id": 38,
           "owner_group": null,
           "owner_user": null,
           ...
        }
      ]

If a single ID is specified rather than a list, then just that record is returned (not a list)


.. tab-set::

  .. tab-item:: QCPortal

    .. code-block:: py3

      >>> records = client.get_records(245)
      >>> print(records.id)
      245

  .. tab-item:: Raw API

    .. code-block:: http

      GET /api/v1/records/245 HTTP/1.1
      Accept: application/json

    .. code-block:: json
      :force:

      {
         "id": 245,
         "record_type": "singlepoint",
         "status": "complete",
         "is_service": false,
         "manager_name": null,
         "modified_on": "2019-03-07T19:45:27.745000",
         "molecule_id": 37,
         "owner_group": null,
         "owner_user": null,
         ...
      }



If a record is not found, then an exception is raised. This can be suppressed with ``missing_ok=True``, in which
case missing records are returned as ``None``

.. tab-set::

  .. tab-item:: QCPortal

    .. code-block:: py3

      >>> records = client.get_records([245, 9999999, 827])
      >>> print(records[1])
      None

  .. tab-item:: Raw API (bulk)

    .. code-block:: http

      POST /api/v1/records/bulkGet HTTP/1.1
      Content-Type: application/json
      Accept: application/json

      {
         "ids": [245, 9999999, 827]
      }

    .. code-block:: json
      :force:

      [
        {
           "id": 245,
           "record_type": "singlepoint",
           "status": "complete",
           "is_service": false,
           "manager_name": null,
           "modified_on": "2019-03-07T19:45:27.745000",
           "molecule_id": 37,
           "owner_group": null,
           "owner_user": null,
           ...
        },
        null,
        {
           "id": 827,
           "record_type": "singlepoint",
           "status": "complete",
           "is_service": false,
           "manager_name": null,
           "modified_on": "2019-03-07T19:45:42.748000",
           "molecule_id": 38,
           "owner_group": null,
           "owner_user": null,
           ...
        }
      ]

Querying records
----------------

The second way of retrieving records is by querying the server using ``query_`` functions
(:func:`~qcportal.client.PortalClient.query_records`, :func:`~qcportal.client.PortalClient.query_singlepoints`, etc).
These functions have a lot of parameters, allowing you to query based on dates, molecules, and other
calculation features.

.. caution::

   Unlike the ``get_`` functions, the records returned from query functions are not in any defined order,
   and the order may be different even with repeated calls with the same arguments

Query functions return an :doc:`iterator <query_iterators>` object.
This iterator handles transparent and efficient fetching from the server in
batches, especially when many records may be returned by a query

.. tab-set::

  .. tab-item:: QCPortal

    .. code-block:: py3

      >>> record_it = client.query_records(record_type='singlepoint', created_before='2021-02-01')
      >>> for record in record_it:
      ...    print(record.id)
      114296306
      114296305
      114296304
        ...

Records that are returned must match all query parameters.
Query functions can take lists or iterables for most parameters as well, in which case records that match
any within the list will be returned. For example, the following finds errored or complete records
that were recently modified

.. tab-set::

  .. tab-item:: QCPortal

    .. code-block:: py3

      >>> record_it = client.query_records(status=['complete', 'error'], modified_after='2022-12-01')
      >>> for record in record_it:
      ...    print(record.id)
      81798273
      79692444
        ...

Query functions for different records types (such as :func:`~qcportal.client.PortalClient.query_singlepoints`)
take more parameters that are specific to that computation (such as basis set for singlepoints, initial molecule for
optimizations, etc).



Next steps
----------

- View the :doc:`basic record information <records/base>`
- See :doc:`documentation about the individual kinds of records <records/index>`