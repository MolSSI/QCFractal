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

    DB ID
      A DB ID (or Database ID) is a unique ID for a given row (a document or
      entry) in the database that uniquely defines that particular row in a
      :term:`DB Table`. These rows are automatically generated and will be
      different for every database, but outlines ways to reference other rows
      in the database quickly. A DB ID is unique to a DB Table.

    DB Table
      A set of data inside the Database which has a common :term:`DB ID`. The ``table``
      name follows SQL conventions which is also known as a ``collection`` in MongoDB.
