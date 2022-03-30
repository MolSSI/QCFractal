Glossary
========

This glossary contains the common terms which appear over the entire Fractal project. There are other, specialized
glossaries for components of Fractal which are linked below to help group terms together with their contextual docs.
Some terms may appear in multiple glossaries, but will always have the same meaning, e.g. :term:`Queue Adapter` and
:term:`Adapter`.

.. glossary::
    :sorted:

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
      queueing systems such as Dask or Fireworks. Also see the :term:`Adapter` in the
      :term:`Manager` glossary.

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

    Fractal Config Directory
      The directory where QCFractal Server and Database configuration files live. This is
      also the home of the Database itself in the default configuration. Default path is
      ``~/.qca/qcfractal``


Contextually Organized Glossaries
---------------------------------

- :ref:`Queue Manager Glossary<manager_glossary>`
