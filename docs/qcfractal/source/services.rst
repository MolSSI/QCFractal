Services
========

Services are unique workflows where there is an iterative component on the
server. A typical service workflow looks like the following:

1. A client submits a new service request to the server.
2. A service is created on the server and placed in the service queue.
3. A service iteration is called that will spawn new tasks.
4. The service waits until all generated tasks are complete.
5. The service repeats 3 and 4 until the service iterations are complete.
6. The service cleans intermediate data, finalizes the data representation, and marks itself complete.

The TorsionDrive service will be used as an example to illuminate the above
steps. The TorsionDrive service optimizes the geometry of a biomolecule at a
number of frozen dihedral angles to provide an energy profile of the rotation
of this dihedral bond.

Consider the service using a concrete example of scanning the
hydrogen peroxide dihedral:

1. A client submits a task to scan the HOOH molecule dihedral every 90 degrees as a service.
2. The service is received by the server, and the first 0-degree dihedral geometry optimization :term:`Task` is spawned.
3. The service waits until the 0-degree :term:`Task` is complete, and then generates 90 and -90-degree :term:`tasks<Task>` based off this 0-degree geometry.
4. The service waits for the two new :term:`tasks<Task>` to complete and spawns 0 and 180-degree tasks based on the 90 and - 90-degree geometries.
5. The service waits for the 90- and -90-degree :term:`tasks<Task>` to complete. Then it builds its final data structure for user querying and marks itself complete.

The service technology allows the ``FractalServer`` to complete very complex
workflows of arbitrary design. To see a pictorial representation of this
process, please see the
:ref:`flowchart showing the pseudo-calls <flowchart_add_procedure>` when a
service is added to the ``FractalServer``


.. toctree::
    service-torsiondrive
