Fractal Server Upgrade
======================

The sub-command for the ``qcfractal-server`` CLI which allows in-place upgrade of Fractal Databases to newer versions
through SQLAlchemy Alembic.

Command Invocation
------------------

.. code-block:: bash

    qcfractal-server upgrade [<options>]


Command Description
-------------------

This command will attempt to upgrade an existing Fractal Database (stored in PostgreSQL) to a new version based on the
currently installed Fractal software version. Not every version of Fractal updates the database, so this command will
only need to be run when you know the database has changed (or attempting to start it tells you to).

This command will attempt to do the following actions for the user in default mode (no args):

* Read the database location from your :doc:`Config File <server_config>` in the default location (can be controlled)
* Determine the upgrade paths from your existing version to the version known by Alembic (update information is
  shipped with the Fractal software)
* Stage update
* Commit update if no errors found

You will then need to start the server again through :doc:`server_start` to bring the server back online.

Caveat: This command will **not** initialize the Fractal Database for you from nothing. The database must exist for
this command to run.

Options
-------

``--base-folder [<folder>]``
    The QCFractal base directory to attach to. Default: ``~/.qca/qcfractal``
