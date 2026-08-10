Server administration via the CLI
=================================

This page documents the qcfractal-server command line interface used to
initialize, start, and administer a QCFractal server. The CLI is implemented in
qcfractal/qcfractal/qcfractal_server_cli.py and uses argparse.

Quick start
-----------

- Create an initial configuration with secrets:

  .. code-block:: bash

     qcfractal-server init-config --config /srv/qcfractal/server.yaml

- Initialize the database (creates DB and tables; starts managed Postgres if applicable):

  .. code-block:: bash

     qcfractal-server init-db --config /srv/qcfractal/server.yaml

- Start the server (API + internal job runner):

  .. code-block:: bash

     qcfractal-server start --config /srv/qcfractal/server.yaml -v

Getting help
------------

- Top-level help and version:

  .. code-block:: bash

     qcfractal-server --help
     qcfractal-server --version

- Help for a subcommand (eg, user):

  .. code-block:: bash

     qcfractal-server user --help

Global options
--------------

These options can be provided before any subcommand, or after the subcommand when
shown as part of a subcommand’s help.

- ``--config PATH``
  Path to a QCFractal YAML configuration. See :ref:`server_configuration`.
- ``-v/--verbose``
  Increase logging verbosity for the command itself. Use ``-vv`` for debug output.

Subcommands
-----------

init-config
~~~~~~~~~~~

Create an initial configuration file, generating secure random secrets and writing
an example configuration you can edit.

Synopsis:

.. code-block:: bash

   qcfractal-server init-config --config /path/to/server.yaml [--full] [-v]

Options:

- ``--full``
  Include all available configuration fields with their default values.

Notes:

- The file is created or overwritten at the path given to ``--config``.
- You will likely want to edit the file (database credentials, API host/port, etc.)
  before initializing the database or starting the server.

init-db
~~~~~~~

Initialize the PostgreSQL database described in the configuration. If QCFractal is
configured to manage its own Postgres instance (``database.own: true``), this command
will initialize that instance and create the target database and tables. If using an
external Postgres, the database must exist and be reachable; this command will create
tables within it.

Synopsis:

.. code-block:: bash

   qcfractal-server init-db --config /path/to/server.yaml [-v]

start
~~~~~

Start a QCFractal server, running both the HTTP API and the internal job runner
(service updates and manager cleanup). This command will ensure Postgres is alive and
that the database schema is up-to-date before starting.

Synopsis:

.. code-block:: bash

   qcfractal-server start --config /path/to/server.yaml \
                          [--host HOST] [--port PORT] \
                          [--logfile PATH] [--loglevel LEVEL] \
                          [--enable-security {true,false}] \
                          [--disable-job-runner] [-v]

Useful options (override configuration values):

- ``--host`` / ``--port``
  Bind address and port for the HTTP API (same keys as ``api.host`` and ``api.port``).
- ``--logfile``
  File to write server logs to; if omitted, logs are written to stdout.
- ``--loglevel``
  One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
- ``--enable-security``
  Override the configuration’s security toggle at startup.
- ``--disable-job-runner``
  Expert: start only the API and skip the internal job-runner processes.

Signals and shutdown:

- Press Ctrl+C or send SIGTERM to stop. The CLI will terminate child processes cleanly.

start-job-runner
~~~~~~~~~~~~~~~~

Start only the internal job runner (background processes that advance services and
perform manager cleanup). This is primarily useful in advanced deployments where
API and job runner are separated.

Synopsis:

.. code-block:: bash

   qcfractal-server start-job-runner --config /path/to/server.yaml \
                                     [--logfile PATH] [--loglevel LEVEL] [-v]

start-api
~~~~~~~~~

Start only the HTTP API server. As with ``start``, the command checks connectivity
to Postgres before launching the API.

Synopsis:

.. code-block:: bash

   qcfractal-server start-api --config /path/to/server.yaml \
                              [--logfile PATH] [--loglevel LEVEL] [-v]

upgrade-db
~~~~~~~~~~

Upgrade the QCFractal database schema to the latest version. Use this after updating
QCFractal to a newer release when migration is required.

Synopsis:

.. code-block:: bash

   qcfractal-server upgrade-db --config /path/to/server.yaml [-v]

If the database cannot be upgraded (eg, incompatible or missing revisions), an error
is printed and the command exits non-zero.

upgrade-config
~~~~~~~~~~~~~~

Upgrade an older QCFractal configuration file to the current format. The original file
is backed up to ``<path>.backup`` (or ``.backup.N`` if needed), and a new file is written
with converted settings and fresh secrets.

Synopsis:

.. code-block:: bash

   qcfractal-server upgrade-config --config /path/to/old_server.yaml [-v]

info
~~~~

Display server and environment information, or show Alembic CLI invocation used for DB
migrations.

Synopsis:

.. code-block:: bash

   qcfractal-server info --config /path/to/server.yaml [server|alembic]

- ``server`` (default)
  Prints Python executable, QCFractal version, Postgres server version, and the active
  QCFractal configuration rendered in YAML.
- ``alembic``
  Prints the exact Alembic CLI command QCFractal would use to operate on the database.

.. _server_admin_users:

user
~~~~

Manage users in the QCFractal database. All user operations require database access via
``--config``.

Synopsis:

.. code-block:: bash

   qcfractal-server user [list | info USERNAME | add USERNAME --role ROLE [--password PW]
                                    [--fullname NAME] [--organization ORG] [--email EMAIL]
                         | modify USERNAME [--fullname NAME] [--organization ORG]
                                    [--email EMAIL] [--role ROLE]
                                    [--enable | --disable]
                                    [--password PW | --reset-password]
                         | delete USERNAME [--no-prompt]] --config /path/to/server.yaml [-v]

Subcommands and options:

- ``list``
  Show a table of all users (username, role, auth type, enabled, fullname).

- ``info USERNAME``
  Show details for one user.

- ``add USERNAME --role ROLE [--password PW] [--fullname ...] [--organization ...] [--email ...]``
  Create a new user with the given role. If ``--password`` is omitted, a secure password
  is generated and printed once. The new user is enabled by default.

- ``modify USERNAME`` with flags:
  - ``--fullname``, ``--organization``, ``--email``: Update profile fields.
  - ``--role``: Change the user role.
  - ``--enable`` / ``--disable``: Toggle enabled state (mutually exclusive).
  - Password management:

    - ``--password PW``: Set a specific password.
    - ``--reset-password``: Generate and print a new random password.

- ``delete USERNAME [--no-prompt]``
  Delete the user. Without ``--no-prompt``, a confirmation is required.

Note on roles: the available roles are built into the server and cannot be customized.
Valid values for ``--role`` are ``admin``, ``maintain``, ``monitor``, ``submit``, ``read``,
``compute``, and ``anonymous``; anything else is rejected. See
:ref:`Users, Roles, and Groups <overview_roles>` for what each one permits.

Groups have no CLI subcommand - manage them with a client
(:meth:`~qcportal.client.PortalClient.add_group` and friends).

backup
~~~~~~

Create a PostgreSQL backup (logical dump) of the current database to a file.

Synopsis:

.. code-block:: bash

   qcfractal-server backup --config /path/to/server.yaml /path/to/backup.dump [-v]

The filename is required and may be absolute or relative to the current shell.

restore
~~~~~~~

Restore the database from a backup file created by ``backup``. The target database
must be reachable and empty or suitable for restore per your Postgres setup.

Synopsis:

.. code-block:: bash

   qcfractal-server restore --config /path/to/server.yaml /path/to/backup.dump [-v]

Common tasks and examples
-------------------------

Fresh installation
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # 1) Create a config (edit afterwards as needed)
   qcfractal-server init-config --config /srv/qcfractal/server.yaml --full

   # 2) Edit /srv/qcfractal/server.yaml (db credentials, host/port, etc.)

   # 3) Initialize the database
   qcfractal-server init-db --config /srv/qcfractal/server.yaml

   # 4) Start the server
   qcfractal-server start --config /srv/qcfractal/server.yaml -v

External Postgres example
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   # server.yaml (excerpt)
   database:
     own: false
     host: db.example.org
     port: 5432
     database_name: qcarchive
     username: qcfractal
     password: "<secret>"

.. code-block:: bash

   # Ensure the database exists and credentials work, then:
   qcfractal-server init-db --config server.yaml
   qcfractal-server start --config server.yaml

User management
~~~~~~~~~~~~~~~

.. code-block:: bash

   # Add an admin user with a generated password
   qcfractal-server user add admin --role admin --fullname "QC Admin" --email admin@example.org \
       --config server.yaml

   # List users
   qcfractal-server user list --config server.yaml

   # Show user info
   qcfractal-server user info admin --config server.yaml

   # Change role and reset password
   qcfractal-server user modify admin --role maintain --reset-password --config server.yaml

   # Disable a user
   qcfractal-server user modify alice --disable --config server.yaml

   # Delete a user (with confirmation)
   qcfractal-server user delete temp_user --config server.yaml

Upgrading QCFractal and the database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # After upgrading the QCFractal package, upgrade DB schema
   qcfractal-server upgrade-db --config server.yaml

   # If you have an older config file, upgrade it
   qcfractal-server upgrade-config --config old_server.yaml

Backups and restore
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Create a backup (logical dump)
   qcfractal-server backup --config server.yaml /backups/qcfractal_$(date +%F).dump

   # Restore from a backup
   qcfractal-server restore --config server.yaml /backups/qcfractal_2025-10-01.dump

Running API and job runner separately
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Terminal 1: API only
   qcfractal-server start-api --config server.yaml --loglevel INFO

   # Terminal 2: job runner only
   qcfractal-server start-job-runner --config server.yaml --loglevel INFO

Logging and verbosity
~~~~~~~~~~~~~~~~~~~~~

- ``-v`` increases the CLI’s verbosity; ``-vv`` enables debug-level messages.
- ``--logfile`` and ``--loglevel`` control server process logging during ``start``,
  ``start-api``, and ``start-job-runner``.

Troubleshooting
---------------

- Use ``qcfractal-server info server --config server.yaml`` to print environment and
  the fully merged configuration.
- If a child process dies shortly after ``start``, check the server logs (stdout or
  the file specified by ``--logfile``) for stack traces.
- For database connection issues, verify credentials and reachability, and try
  ``psql`` to the configured host/port/database.

See also
--------

- :ref:`server_configuration` for the full configuration reference.
- User and dataset administration via the SDK and API is covered elsewhere in the
  admin guide.
