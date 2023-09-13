QCFractal Installation and Setup 
=====================================

.. admonition:: Note about PostgreSQL

  The QCFractal server uses `PostgreSQL <https://www.postgresql.org>`_ to store the data in a database.
  Depending on your use case, there are a few ways to setup postgres.

  For new users, the simplest way is to install PostgreSQL via conda, along with the QCFractal server packages.
  Then, you can let QCFractal handle creating and starting the database automatically.

  For advanced use cases, you are free to install and manage a PostgreSQL yourself, either on the same
  machine or on a different machine (where the QCFractal server will access it over the network).
  You may choose to do this for very large databases (100GB+) or if you have other requirements.

  These setup instructions will assume you want the simplest case, and we will let
  QCFractal manage the database.

Installation through conda
--------------------------

The QCFractal server can be installed via `conda/anaconda <https://www.anaconda.com>`_
or `mamba <https://github.com/mamba-org/mamba>`_. The packages exist under the
`QCArchive organization <https://anaconda.org/QCArchive>`_ on Anaconda.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

        mamba create -n qcf_server qcfractal postgresql -c conda-forge
        mamba activate qcf_server


Setting up the server
---------------------

You generally want to keep all files related to the QCFractal server in a single directory.
So we are going to create a directory, and then initialize a configuration file there.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

        mkdir qcf_server
        qcfractal-server --config=qcf_server/qcf_config.yaml init-config

This creates an example configuration file. You are now free to change those settings as
needed - see :ref:`server_configuration`.

Some fields are likely to be changed

  * **name** - A nice name for your server - is occasionally displayed for users
  * **logfile** - A file to log to. If not specified (or 'null'), will just log to the console.
  * **enable_security** - Set to **false** if you want to disable security (ie, users/passwords are not checked). Generally not recommended
  * **allow_unauthenticated_read** - Set to **false** if you want to require user login to view records, etc.
  * **database->database_name** - a descriptive name for the database (no spaces or special characters)
  * **database->database_port** - Port that the database runs on. **5432** is the default postgres port, so it may be in use already. Choose any that is not in use.
  * **api->host** - The server binds to this hostname. **127.0.0.1** will only allow access from the same computer. Change to **0.0.0.0** to allow access from anywhere (dangerous if you are not ready!)
  * **api->port** - Port that the web API runs on. **7777** should be ok for most cases, but you may change it

Now we are ready to initialize the database. This creates the database directory structure and files,
as well as the actual postgres database and tables for QCFractal.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

        qcfractal-server --config=qcf_server/qcf_config.yaml init-db


Before starting the server, it doesn't hurt to check the configuration to make sure it matches
your expectations.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

        qcfractal-server --config=qcf_server/qcf_config.yaml info


Now we may start the server! This will run the server in the foreground, so you can not use your terminal anymore.
You can place it in the background with **screen** or any other utilities if needed.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

        qcfractal-server --config=qcf_server/qcf_config.yaml start

To stop a running server, you can use **Ctrl-C**.


Next steps
---------------------

Next, you will probably want to set up an admin user (see :ref:`server_admin_users`)

