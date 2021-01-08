Portal Client
=============

The ``FractalClient`` is the primary entry point to a ``FractalServer`` instance
which can be initialized by pointing to a server instance:

.. code-block:: python

    >>> import qcportal as ptl
    >>> client = ptl.FractalClient("localhost:8888")
    >>> client
    FractalClient(server='http://localhost:8888/', username='None')

The ``FractalClient`` handles all communications between ``FractalServer``
and the Python API layer. The ``FractalClient`` fascilitates the addition of
new molecules to the datasets, performing computations, interacting with collections,
and querying the records. A ``FractalClient`` object instance created by 
the default constructor (without any arguments) will automatically 
attempt to connect to the MolSSI QCArchive server.

The ``FractalClient`` can also be initialized using a YAML configuration file. This 
approach is useful because server address and username do not have to be retyped
everytime the user initializes a server object. It will also reduce the chance 
for the username and password to be accidentally added to
a version control system and exposed to the public. 
The above strategy adopts the classmethod ``FractalClient.from_file()`` 
which by default, searches for the YAML configuration file named
``qcportal_config.yaml`` under either the current working directory 
or the canonical ``~/.qca`` folder.



