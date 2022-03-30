Fractal Server Init
===================

The sub-command for the ``qcfractal-server`` CLI which initializes a new server instance, including configuring
the PostgreSQL database if it is not setup yet.

Command Invocation
------------------

.. code-block:: bash

    qcfractal-server init [<options>]


Command Description
-------------------

This command will attempt to do the following actions for the user in default mode (no args):

* Create the :term:`QCFractal Config directory<Fractal Config Directory>`
* Create a blank :doc:`Fractal Config file<server_config>` (assumes defaults)
* Create the folders for housing the PostgreSQL database file, which will be the home of Fractal's data.
* Initialize PostgreSQL's service at the database location from above
* Start the PostgreSQL server
* Populate the database tables and finalize everything for Fractal's operation

In most cases, the user should not have to change any configurations if they are the system owners or admins. However,
if users want to do something different, they can write their own :doc:`Config File <server_config>` and
change the settings though the CLI to start the server.

Options
-------

**This is a set of GLOBAL level options which impact where the ``init`` command looks, and how it interacts with the config file**

``--overwrite``
    Control whether the rest of the settings overwrite an existing config file in the
    :term:`QCFractal Config directory<Fractal Config Directory>`

``--base-folder [<folder>]``
    The QCFractal base directory to attach to. Default: ``~/.qca/qcfractal``

**This set of options pertain to the PostgreSQL database itself** and translate to the ``database`` header in the
:doc:`server_config`.

``--db-port [<port>]``
    The PostgreSQL default port, Default ``5432``

``--db-host [<host>]``
    Default location for the Postgres server. If not ``localhost``, Fractal command lines cannot manage the instance.
    and will have to be configured in the :doc:`Config File <server_config>`. Default: ``localhost``

``--db-username [<user>]``
    The postgres username to default to. **Planned Feature - Currently inactive**.

``--db-password [<password>]``
    The postgres password for the give user. **Planned Feature - Currently inactive**.

``--db-directory [<dir_path>]``
    "The physical location of the QCFractal instance data, defaults to the root
    :term:`Config directory<Fractal Config Directory>`.

``--db-default-database [<db_name>]``
    The default database to connect to. Typically used if you already have a Fractal Database set up or you want to
    use a different name for the database besides the default. Default ``qcfractal_default``.

``--db-logfile [<logfile>]``
    The logfile to write postgres logs. Default ``qcfractal_postgres.log``.

``--db-own (True|False)``
    If own is True, Fractal will control the database instance. If False Postgres will expect a booted server at the
    database specification. Default ``True``


**The settings below here pertain to the Fractal Server** and translate to the ``fractal`` header in the
:doc:`server_config`.

``--name [<name>]``
    The Fractal server default name. Controls how the server presents itself to connected clients.
    Default ``QCFractal Server``

``--port [<port>]``
    The Fractal default port. This is the port which Fractal listens to for client connections (and for the URI).
    This is *separate* from the ``--db-port`` which is the port that PostgreSQL database is listening for. In general,
    these should be separate. Default ``7777``.

``--compres-response (True|False)``
    Compress REST responses or not, should be True unless behind a proxy. Default ``True``.

``--allow-read (True|False)``
    Always allows read access to record tables. Default ``True``

``--security [<security_string>]``
    Optional security features. Not set by default.

``--query-limit [<int_limit>]``
    The maximum number of records to return per query. Default ``1000``

``--logfile [<log>]``
    The logfile the Fractal Server writes to. Default ``qcfractal_server.log``

``--service-frequency [<frequency>]``
    The frequency to update the Fractal services. Default ``60``

``--max-active-services [<max-services>]``
    The maximum number of concurrent active services. Default ``20``

``--heartbeat-frequency [<heartbeat>]``
    The frequency (in seconds) to check the heartbeat of :term:`Managers <Manager>`. Default ``1800``
