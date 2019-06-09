Portal Client
=============

The ``FractalClient`` is the primary entry point to a ``FractalServer`` instance.

We can initialize a ``FractalClient`` by pointing it to a server instance. If
you would like to start your own server see the setting up a server (NYI)
section.

.. code-block:: python

    >>> import qcportal as ptl
    >>> client = ptl.FractalClient("localhost:8888")
    >>> client
    FractalClient(server='http://localhost:8888/', username='None')

The ``FractalClient`` handles all communication to the ``FractalServer`` from
the Python API layer. This includes adding new molecules, computations,
collections, and querying for records. A ``FractalClient`` constructed without
arguments will automatically connect to the MolSSI QCArchive server.

The ``FractalClient`` can also be initialized from a file which is useful so
that addresses and username do not have to be retyped for everytime and
reduces the chance that a username and password could accidentally be added to
a version control system. Creation from file uses the classmethod
``FractalClient.from_file()``, by default the client searches for a
``qcportal_config.yaml`` file in either the current working directory or from
the canonical ``~/.qca`` folder.



