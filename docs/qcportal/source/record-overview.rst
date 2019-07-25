Overview
========

A :term:`Record` is a

A result is a single quantum chemistry method evaluation, which might be an energy, an analytic gradient or Hessian, or a property evaluation.
Collections of evaluations such
as finite-difference gradients, complete basis set extrapolation, or geometry
optimizations belong under the "Procedures" heading.

Single Results
--------------

Procedures
----------

A result can be found based off a unique tuple of ``(program, molecule_id, options_set, method, basis)``:

- ``program`` - A lowercase string representation of the quantum chemistry program used (``gamess``, ``nwchem``, ``psi4``)
- ``molecule_id`` - The :term:`ObjectId` of the molecule used in the computation.
- ``keywords_set`` - The key to the options set stored in the database (``default`` -> ``{"e_convergence": 1.e-7, "scf_type": "df", ...}``)
- ``method`` - A lowercase string representation of the method used in the computation (``b3lyp``, ``mp2``, ``ccsd(t)``).
- ``basis`` - A lowercase string representation of the basis used in the computation (``6-31g``, ``cc-pvdz``, ``def2-svp``)

