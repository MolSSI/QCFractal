Database Design
==================

.. warning:: Final MongoDB Supported Version: 0.7.0

    **0.7.0 is the last major release which support MongoDB.** Fractal is moving towards a PostgreSQL database to
    make upgrades more stable and because it is more suited to the nature of QCArchive Data. The upgrade path from
    MongoDB to PostgreSQL will be provided by the Fractal developers in the next release. Due to the complex nature
    of the upgrade, the PostgreSQL upgrade will through scripts which will be provided. After the PostgreSQL upgrade,
    there will be built-in utilities to upgrade the Database.

QCArchive stores all its data and computations in a database in the backend
of QCFractal_. The DB is designed with extensibility in mind, allowing
flexibility and easy accommodation of future features. The current backend
of the DB storage is build on top of a non-relational DB, MongoDB, but it can
be easily implemented in a Relational DB like MySQL or Postgres. In addition,
Object Relational Mapping (ORM) is used to add some structure and ensure
validation on the MongoDB which does not have any by definition. The ORM used
is the most popular general MongoDB Python ORM, Mongoengine_.

.. _Mongoengine: http://mongoengine.org


The main idea behind QCArchive DB design is to be able to store and retrieve
wide range of Quantum Chemistry computations using different programs and
variety of configurations. The DB also stores information about jobs submitted
to request computations, and all their related data, along with registered users and
computational managers.


QCArchive DB is organized into a set of tables (or documents), each of which are detailed below.


1) Molecule
+++++++++++++

The molecule table stores molecules used in any computation in the system.
The molecule structure is based on the standard QCSchema_. It stores entries like
geometry, masses, and fragment charges. Please refer to the QCSchema_ for a complete
description of all the possible fields.

.. Uniqueness among molecules is ensured by creating a hash index calculated using
.. TODO: add a simple description


2) Keyword
+++++++++++

Keywords are a store of key-value pairs that are configuration for some
QC program. It is flexible and there is no restriction on what configuration
can be stored here. This table referenced by the ``Result`` table.


3) Result
++++++++++

This table stores the actual computation results along with the attributes
used to calculate it. Each entry is a single unit of computation.
The following are the unique set of keys (or indices) that define a result:

- ``driver`` - The type of calculation being evaluated (i.e. ``energy``, ``gradient``, ``hessian``, ``properties``)
- ``program``: such as ``gamess`` or ``psi4`` (lower case)
- ``molecule``: the ID of the molecule in the ``Molecule`` table
- ``method``: the method used in the computation (b3lyp, mp2, ccsd(t))
- ``keywords``: the ID of the keywords in the ``Keywords`` table
- ``basis``: the name of the basis used in the computation (6-31g, cc-pvdz, def2-svp)

For more information see: :doc:`results`.


4) Procedure
+++++++++++++

Procedures are also computational results but in a more complex fashion.
They perform more aggregate computations like optimizations, torsion drive, and
grid optimization. The DB can support new types of optimizations by
inheriting from the the base procedure table. Each procedure usually reference
several other results from the ``Results`` table, and possibly other procedures
(self-reference).


5) Services
+++++++++++

Services are more flexible workflows that eventually produce results to be
stored in the ``Result`` and/or the ``Procedure`` tables when they are done.
So, from the DB point of view, this is an intermediate table for on going
iterative computations.

More about services in QCArchive can be found here: :doc:`services`.


6) TaskQueue
+++++++++++++

This table is the main task queue of the system. Tasks are submitted to this
table by QCFractal_ and wait for a manager to pull it for computation. Each
task in the queue references a ``Result`` or a ``Procedure``, meaning that it is
corresponding to a specific Quantum computation. The table stores the status
of the task (``WAITING``, ``RUNNING``, ``COMPLETE``, and ``ERROR``) and also
keeps track of the execution manager and the modification dates.


7) QueueManagers
+++++++++++++++++

:term:`Managers<Manager>` are the registered servers for computing tasks from the ``TaskQueue``.
This table keep information about the server such as the host, cluster,
number of completed tasks, submissions, and failures.

The database only keeps track of what :term:`Tasks<Task>` have been handed out to
each :term:`Manager` and maintains a heartbeat to ensure the :term:`Manager` is still connected. More information about
the configuration and execution of managers can be found here: :doc:`managers`.


.. _QCSchema: https://github.com/MolSSI/QC_JSON_Schema
.. _QCFractal: https://github.com/MolSSI/QCFractal
