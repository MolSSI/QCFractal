Overview
========

:term:`Records` are the stored values of a completed computation.
Each ``Record`` type corresponds to a specific operation formatted by QCArchive.
Several ``Record`` examples include:

- :doc:`Result <record-result>` - data from a single (often quantum chemical) energy, gradient, Hesssian, or property computation,
- :doc:`Optimization <record-optimization>` - data resulted from a geometry optimization at a given level of theory,
- ``GridOptimization`` - results of a chain of geometry optimizations where starting structures at each step depends on previous structures, or
- ``TorsionDrive`` - the outcome of a special type of ``GridOptimization`` specifically for torsion scans that is able to find global minimum structures
  on the potential energy surfaces.

In general, ``Records`` are indexed by hashes and therefore, can be easily queried within a dataset ``Collection``.
