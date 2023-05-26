Computation Types
=====================================

QCArchive natively supports several different kinds of computations. Each computation has its own
record object. These objects share a common set of
properties and methods (:doc:`./base`), but each also has its own unique set of features.

.. table::

  =================================================   =========================================================================================
   Type                                                            Description
  =================================================   =========================================================================================
   :doc:`Base record <./base>`                         Common features across all records
   :doc:`Singlepoint <./singlepoint>`                  Energies & properties on a single geometry
   :doc:`Optimization <./optimization>`                Optimization of molecular geometry
   :doc:`Gridoptimization <./gridoptimization>`        Geometry optimizations across a set of constrained distances & angles
   :doc:`Torsiondrive <./torsiondrive>`                Geometry optimizations across a set of constrained torsion angles
   :doc:`Manybody <./manybody>`                        Manybody computations such as interaction energies
   :doc:`Reaction <./reaction>`                        Computation of reaction energies
   :doc:`NEB <./neb>`                                  Nudged Elastic Band computation of reaction profiles and transition states
  =================================================   =========================================================================================


.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Contents:

   base
   singlepoint
   optimization
   gridoptimization
   torsiondrive
   manybody
   reaction
   neb
