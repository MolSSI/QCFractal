Overview
========

A :term:`Record` is the stored values of a completed computation. Each ``Record`` type corresponds to a specific operation that QCArchive has formatted.

Several examples are:

- :doc:`Results <results>` - A single quantum chemistry (or quantum chemistry-like) energy, gradient, Hesssian, or property computation.
- ``Optimization`` - A geometry optimization at a given level of theory.
- ``GridOptimization`` - Chains of geometry optimizations where starting structures depend on previous structures.
- ``TorsionDrive`` - A special type of GridOptimization specifically for torsion scans that is able to overcome local minimum structures to find globally optimal ones.

In general records are indexed based off a hash and are often found and queried through a Collection rather than directly.
