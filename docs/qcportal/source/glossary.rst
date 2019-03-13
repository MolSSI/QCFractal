Glossary
========

.. glossary::

    DB Index
      A DB Index (or Database Index) is a commonly queried field used to speed up
      searches in a :term:`DB Table`.

    DB Socket
      A DB Socket (or Database Socket) is the interface layer between standard
      Python queries and raw SQL or MongoDB query language.

    DB Table
      A set of data inside the Database which has a common :term:`ObjectId`. The ``table``
      name follows SQL conventions which is also known as a ``collection`` in MongoDB.

    Hash Index
      A index that hashes the information contained in the object
      in a reproducible manner. This hash index is only used to find duplicates
      and should not be relied upon as it may change in the future.

    Molecule
      A unique 3D representation of a molecule. Any changes to the protonation
      state, multiplicity, charge, fragments, coordinates, connectivity, isotope, or
      ghost atoms represent a change in the molecule.

    ObjectId
      A ObjectId (or Database ID) is a unique ID for a given row (a document or
      entry) in the database that uniquely defines that particular row in a
      :term:`DB Table`. These rows are automatically generated and will be
      different for every database, but outlines ways to reference other rows
      in the database quickly. A ObjectId is unique to a DB Table.

    Procedures
      On-node computations, these can either be a single computation (energy,
      gradient, property, etc.) or a series of calculations such as a geometry
      optimization.

    Queue Adapter
      The interface between QCFractal's internal queue representation and other
      queueing systems such as Dask or Fireworks.

    Record
      A document that contains all results (or links) of a given computation.

    Services
      Iterative workflows where the required computations are distributed via
      the queue and then are processed on the server to acquire the next iteration of
      calculations.






