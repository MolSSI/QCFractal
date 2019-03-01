Setup Server
=============

A  ``qcfractal-server`` instance contains a record of all results, task queue,
and collection information and provides an interface to all FractalClients and
``qcfractal-manager``s. All data is stored in a local MongoDB. A server
instance should be run on hardware that is for long periods stable (not
shutdown often),  accessible from both compute resources and users via HTTP,
and have access to permanent storage.  This location is often either research
groups local computers, a supercomputer with  appropriately allocated
resources for this task, or the cloud.

Using the Command Line
----------------------


A ``qcfractal-server`` instance can be created from the command line:


.. code-block:: console

    qcfractal-server mydb

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

        # Make MongoDB Directory and start server
        MONGOPATH=/tmp/example
        mkdir -p $MONGOPATH
        mongod --dbpath $MONGOPATH

Using the Python API
--------------------

TODO

Server with Compute
-------------------

For small or trial instances of ``qcfractal-server`` an instance can be spun
up with compute with the following lines:

.. code-block:: console

    qcfractal-server mydb --local-manager

This will create a ``qcfractal-manager`` attached to the ``qcfractal-server``
with a ProcessPoolExecutor attached.

