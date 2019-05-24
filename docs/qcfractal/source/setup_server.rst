Setup Server
=============

A  ``qcfractal-server`` instance contains a record of all results, task queue,
and collection information and provides an interface to all FractalClients
and ``qcfractal-manager``\s. All data is stored in a local MongoDB. A server
instance should be run on hardware that is for long periods stable (not
shutdown often),  accessible from both compute resources and users via HTTP,
and have access to permanent storage.  This location is often either research
groups local computers, a supercomputer with  appropriately allocated
resources for this task, or the cloud.


Using the Command Line
----------------------

A ``qcfractal-server`` instance for long-term computation should be created from
the command line like so:

.. code-block:: console

    >>> qcfractal-server mydb

In each case ``qcfractal-server`` requires a database to connect to, in this
case, ``mydb`` is the database name. This command connects to
``localhost::27017`` by default which is MongoDB's default location. Other
MongoDB locations can be passed in with the ``--database-uri`` flag. Multiple
``qcfractal-server`` can be connected to the same MongoDB, but only one
``qcfractal-server`` should be connected to a single database name.

.. note::

    A MongoDB instance must be setup for a ``qcfractal-server``. MongoDB
    can be installed from ``conda`` and comes with ``conda install qcfractal``
    by default. An example setup can be found below:

    .. code-block:: console

        >>> MONGOPATH=/tmp/example
        >>> mkdir -p $MONGOPATH
        >>> mongod --dbpath $MONGOPATH

For small or trial instances of ``qcfractal-server`` an instance can be spun
up with compute with the following lines:

.. code-block:: console

    >>> qcfractal-server mydb --local-manager

This will create a ``qcfractal-manager`` attached to the ``qcfractal-server``
with a ProcessPoolExecutor attached.

Within a Python Script
----------------------

Canonical workflows can be run from a Python script using the ``FractalSnowflake``
instance. With default options a ``FractalSnowflake`` will spin up a database backend
which contains no data and then destroy this database upon shudown.

.. warning::

    All data inside a ``FractalSnowflake`` is temporary and will be deleted when the
    ``FractalSnowflake`` shuts down.

.. code-block:: python

    >>> from qcfractal import FractalSnowflake
    >>> server = FractalSnowflake()

    # Obtain a FractalClient to the server
    >>> client = server.client()

A standard ``FractalServer`` cannot be started in a Python script and then interacted with
as a ``FractalServer`` uses asynchronous programming by default. ``FractalServer.start`` will
stop the script.


Within a Jupyter Notebook
-------------------------

Due to the way Jupyter Notebooks work an interactive server needs to take a different approach
than the canonical Python script. To manipulate a server in a Jupyter Notebook a
``FractalSnowflakeHandler`` can be used much in the same way as a ``FractalSnowflake``.

.. warning::

    All data inside a ``FractalSnowflakeHandler`` is temporary and will be deleted when the
    ``FractalSnowflakeHandler`` shuts down.

.. code-block:: python

    >>> from qcfractal import FractalSnowflakeHandler
    >>> server = FractalSnowflakeHandler()

    # Obtain a FractalClient to the server
    >>> client = server.client()