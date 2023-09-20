Connecting to a server
====================================

The central object of QCPortal is the :class:`~qcportal.client.PortalClient` (usually referred to as just `client`).
This class handles connecting to the server and exposing all the functionality of the server for use in python.

Connecting to the server is handled by the constructor of the :class:`~qcportal.client.PortalClient` class.

The first parameter is the address or URI of the server you with to connect to (including ``http``/``https``).

.. tab-set::

  .. tab-item:: PYTHON

    >>> from qcportal import PortalClient
    >>> client = PortalClient("https://ml.qcarchive.molssi.org")
    >>> print(client.server_name)
    The MolSSI ML QCFractal Server

However, you can specify the address of another server. Here we connect to the MolSSI-hosted
public demonstration server

.. tab-set::

  .. tab-item:: PYTHON

    >>> from qcportal import PortalClient
    >>> client = PortalClient("https://qcademo.molssi.org")
    >>> print(client.server_name)
    MolSSI QCArchive Demo Server

Servers may require a username and password to connect or to perform certain actions;
these can also be specified in the constructor.

.. tab-set::

  .. tab-item:: PYTHON

    >>> from qcportal import PortalClient
    >>> client = PortalClient("https://my.qcarchive.server", username='grad_student_123', password='abc123XYZ')

For a description of the other parameters, see :class:`~qcportal.client.PortalClient`.

.. _qcportal_connecting_file:

Using user information from a file
----------------------------------

Rather than place your username and password in the script or notebook, you may also
have the client read the credentials from a file (see :ref:`qcportal_setup_configfile`).

To use this file, construct the client using the
:meth:`~qcportal.client.PortalClient.from_file` method.

If no path is passed to this function, then the current working directory
and then the ``~/.qca`` directory are search for ``qcportal_config.yaml``.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

        # A file containing a single server, file stored in working directory or ~/.qca
        >>> from qcportal import PortalClient
        >>> client = PortalClient.from_file()
        >>> print(client.server_name)
        MolSSI QCArchive Demo Server

        # Manually specify a path to the config file
        >>> client = PortalClient.from_file('group_server', '/path/to/config')
        >>> print(client.server_name)
        Professor's Group Server


Viewing server metadata
-----------------------

Some metadata about the server is stored in the client object. The metadata includes the server
name and version, as well as limits on API calls. This also contains any Message-of-the-Day (MOTD) that
the server administrator wishes to include.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

        >>> from qcportal import PortalClient
        >>> client = PortalClient('https://qcademo.molssi.org')
        >>> print(client.server_info)
        {'name': 'MolSSI QCArchive Demo Server',
        'manager_heartbeat_frequency': 1800,
        'version': '0.50b4.post4+ged0d0270',
        'api_limits': {'get_records': 1000,
          'add_records': 500,
          'get_dataset_entries': 2000,
          'get_molecules': 1000,
          'add_molecules': 1000,
          'get_managers': 1000,
          'manager_tasks_claim': 200,
          'manager_tasks_return': 10,
          'get_server_stats': 25,
          'get_access_logs': 1000,
          'get_error_logs': 100,
          'get_internal_jobs': 1000},
        'client_version_lower_limit': '0',
        'client_version_upper_limit': '1',
        'motd': ''}


Next steps
----------

From here, you probably want to look at:

* :doc:`Retrieve records <record_retrieval>`
* :doc:`Add records <record_submission>`
* :doc:`Use datasets <datasets>`
* :class:`View the PortalClient API documentation <qcportal.client.PortalClient>`
