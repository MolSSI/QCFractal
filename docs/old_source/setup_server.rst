Server Setup
============

A  ``qcfractal-server`` instance contains a record of all results, task queue,
and collection information and provides an interface to all FractalClients
and ``qcfractal-manager``\s. All data is stored in a PostgreSQL database which is often
handled transparently. A server
instance should be run on hardware that is for long periods stable (not
shutdown often),  accessible from both compute resources and users via HTTP,
and have access to permanent storage.  This location is often either research
groups local computers, a supercomputer with  appropriately allocated
resources for this task, or the cloud.


Using the Command Line
----------------------

The command line is used for ``qcfractal-server`` instances that are long-term
data storage and task distribution engines. To begin, a ``qcfractal-server``
is first initialized using the command line:

.. code-block:: console

    >>> qcfractal-server init

This initialization will create ``~/.qca/qcfractal`` folder (which can be
altered) which contains default specifications for the ``qcfractal-server``
and for the underlying PostgreSQL database. The ``qcfractal-server init
--help`` CLI command will describe all parameterizations of this folder. In
addition to the specification information, a new PostgreSQL database will be
initialized and started in the background. The background PostgreSQL database
consumes virtually no resources when not in use and should not interfere with
your system.

Once a ``qcfractal-server`` instance is initialized the server can then be run
with the ``start`` command:

.. code-block:: console

    >>> qcfractal-server start

The QCFractal server is now ready to accept new connections.

Within a Python Script
----------------------

Canonical workflows can be run from a Python script using the ``FractalSnowflake``
instance. With default options a ``FractalSnowflake`` will spin up a fresh database which
will be removed after shutdown.

.. warning::

    All data inside a ``FractalSnowflake`` is temporary and will be deleted when the
    ``FractalSnowflake`` shuts down.

.. code-block:: python

    >>> from qcfractal import FractalSnowflake
    >>> server = FractalSnowflake()

    # Obtain a FractalClient to the server
    >>> client = server.client()

A standard ``FractalServer`` cannot be started in a Python script and then interacted with
as a ``FractalServer`` uses asynchronous programming by default. ``FractalServer.stop`` will
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


Full Server Config Settings
---------------------------

The full CLI and configs for the Fractal Server can be found on the following pages:

* Fractal Server Config file: :doc:`server_config`
* ``qcfractal-server init``: :doc:`server_init`
* ``qcfractal-server start``: :doc:`server_start`
* ``qcfractal-server upgrade``: :doc:`server_upgrade`
