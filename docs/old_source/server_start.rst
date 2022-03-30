Fractal Server Start
====================

The sub-command for the ``qcfractal-server`` CLI which starts the Fractal server instance

Command Invocation
------------------

.. code-block:: bash

    qcfractal-server start [<options>]


Command Description
-------------------

This command will attempt to do the following actions for the user in default mode (no args):

* Read the :term:`QCFractal Config directory<Fractal Config Directory>`
* Read the config file in that directory
* Connect to the previously created Fractal database created in the PostgreSQL service (see :doc:`server_init`).
* Start Fractal's periodic services.
* Create and provide SSL certificates.

The options for the database and starting local compute on the same resources as the server can be controlled through
the flags below. Also see all the config file options in :doc:`Config File <server_config>`.

Options
-------

``--base-folder [<folder>]``
    The QCFractal base directory to attach to. Default: ``~/.qca/qcfractal``

``--port [<port>]``
    The Fractal default port. This is the port which Fractal listens to for client connections (and for the URI).
    This is *separate* from the `the port that PostgreSQL database is listening for. In general, these should be
    separate. Default ``7777``.

``--logfile [<log>]``
    The logfile the Fractal Server writes to. Default ``qcfractal_server.log``

``--database-name [<db_name>]``
    The database to connect to, defaults to the default database name. Default ``qcfractal_default``

``--server-name [<server_name>]``
    The Fractal server default name. Controls how the server presents itself to connected clients.
    Default ``QCFractal Server``

``--start-periodics (True|False)``
    **Expert Level Flag Only Warning!** Can disable periodic update (services, heartbeats) if False. Useful when
    running behind a proxy. Default ``True``

``--disable-ssl (False|True)``
    Disables SSL if present, if ``False`` a SSL cert will be created for you. Default ``False``

``--tls-cert [<tls_cert_str>]``
    Certificate file for TLS (in PEM format)

``--tls-key [<tls_key_str>]``
    Private key file for TLS (in PEM format)

``--local-manager [<int>]``
    Creates a local pool QueueManager attached to the server using the number of threads specified by the arg.
    If this flag is set and no number is provided, 1 (one) thread will be spun up and running locally. If you
    expect :term:`Fractal Managers<Manager>` to connect to this server, then it is unlikely you need this. Related, if
    no compute is expected to be done on this server, then it is unlikely this will be needed.
