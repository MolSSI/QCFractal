Connecting to a server
======================

The central object of QCPortal is the :class:`~qcportal.client.PortalClient` (usually referred to as just `client`).
This class handles connecting to the server and exposing all the functionality of the server for use in python.


Connecting to a server
----------------------

Connecting to the server is handled by the constructor of the :class:`~qcportal.client.PortalClient` class.

The first parameter is the address or URI of the server you with to connect to (including ``http``/``https``).
If no address is given, then by default the client will connect to the public, MolSSI-hosted server.

    >>> from qcportal import PortalClient
    >>> client = PortalClient()
    >>> print(client.server_name)
    MolSSI Public QCArchive Server

However, you can specify the address of another server. Here we connect to the MolSSI-hosted
public demonstration server

    >>> from qcportal import PortalClient
    >>> client = PortalClient("https://qcademo.molssi.org")
    >>> print(client.server_name)
    MolSSI QCArchive Demo Server

Servers may require a username and password to connect or to perform certain actions;
these can also be specified in the constructor.

    >>> from qcportal import PortalClient
    >>> client = PortalClient("https://my.qcarchive.server", username='grad_student_123', password='abc123XYZ')

Other options are available. See  :class:`~qcportal.client.PortalClient` for details.


Storing username/password in a file
-----------------------------------


Next steps
----------

From here, you probably want to look at:

* :doc:`Add or retrieve records <record_basics>`
* :doc:`Use datasets <dataset_basics>`
* :doc:`Administer the server <serveradmin>`
* :doc:`View the PortalClient API documentation <portalclient_api>`
