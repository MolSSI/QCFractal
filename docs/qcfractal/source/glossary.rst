Glossary
========

.. glossary::

    Procedures
      On-node computations, these can either be a single computation (energy,
      gradient, property, etc.) or a series of calculations such as a geometry
      optimization.

    Services
      Iterative workflows where the required computations are distributed via
      the queue and then are processed on the server to acquire the next iteration of
      calculations.

    Queue Adapter
      The interface between QCFractal's internal queue representation and other
      queueing systems such as Dask or Fireworks.

    Molecule
      A unique 3D representation of a molecule. Any changes to the protonation
      state, multiplicity, charge, fragments, coordinates, connectivity, isotope, or
      ghost atoms represent a change in the molecule.

    DB Socket
      A DB Socket (or Database Socket) is the interface layer between standard
      Python queries and raw SQL or MongoDB query language.

    DB Index
      A DB Index (or Database Index) is a commonly queried field used to speed up
      searches in a :term:`DB Table`.

    Hash Index
      A index that hashes the information contained in the object
      in a reproducible manner. This hash index is only used to find duplicates
      and should not be relied upon as it may change in the future.

    ObjectId
      A ObjectId (or Database ID) is a unique ID for a given row (a document or
      entry) in the database that uniquely defines that particular row in a
      :term:`DB Table`. These rows are automatically generated and will be
      different for every database, but outlines ways to reference other rows
      in the database quickly. A ObjectId is unique to a DB Table.

    DB Table
      A set of data inside the Database which has a common :term:`ObjectId`. The ``table``
      name follows SQL conventions which is also known as a ``collection`` in MongoDB.

    Manager
        The :doc:`Fractal Queue Manager<managers>`. The term "Manager" presented by itself refers to this object.

    Adapter
        The specific piece of software which accepts :term:`tasks<Task>` from the :term:`Manager` and sends them to the
        physical hardware. It is also the software which interacts with a cluster's :term:`Scheduler` to allocate said
        hardware and start :term:`Job`.

    Distributed Compute Engine
        A more precise, although longer-winded, term for the :term:`Adapter`.

    Scheduler
        The software running on a cluster which users request hardware from to run computational :term:`tasks<Task>`,
        e.g. PBS, SLURM,
        LSF, SGE, etc. This, by itself, does not have any concept of the :term:`Manager` or even the :term:`Adapter`
        as both interface with *it*, not the other way around. Individual users' clusters may, and in most every case,
        will have a different configuration, even amongst the same governing software. Therefore, no two Schedulers
        should be treated the same.

    Job
        The specific allocation of resources (CPU, Memory, wall clock, etc) provided by the :term:`Scheduler` to the
        :term:`Adapter`. This is identical to if you requested batch-like job on a cluster (e.g. though ``qsub`` or
        ``sbatch``), however, it is more apt to think of the resources allocated in this way as "resources to be
        distributed to the :term:`Task` by the :term:`Adapter`". Although a user running a :term:`Manager` will likely
        not directly interact with these, its important to track as these are what your :term:`Scheduler` is actually
        running and your allocations will be charged by.

    Task
        A single unit of compute as defined by the Fractal :term:`Server` (i.e. the item which comes from the Task
        Queue). These tasks are preserved as they pass to the distributed compute engine and are what are presented to
        each distributed compute engine's :term:`Worker`\s to compute

    Worker
        The process executed from the :term:`Adapter` on the allocated hardware inside a :term:`Job`. This process
        receives the :term:`tasks<Task>` tracked by the :term:`Adapter` and is responsible for their execution. There may
        be multiple Workers within a single :term:`Job`, and the resources allocated for said :term:`Job` will be
        distributed by the :term:`Adapter` using whatever the :term:`Adapter` is configured to do. This is often uniform,
        but not always.

    Server
        The Fractal Server that the :term:`Manager` connects to. This is the source of the
        :term:`Task`\s which are pulled from and pushed to.

    Tag
        Arbitrary categorization labels that different :term:`tasks<Task>` can be assigned when submitted to the
        :term:`Server`. :term:`Managers<Manager>` can pull these tags if configured, and will *exclusively* pull their
        defined tag if so. Similarly, :term:`tasks<Task>` set with a given tag can *only* be pulled if
        their :term:`Manager` is configured to do so.
