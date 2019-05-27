Results
========

A result is a single quantum chemistry method evaluation, which might be an energy, an analytic gradient or Hessian, or a property evaluation.
Collections of evaluations such
as finite-difference gradients, complete basis set extrapolation, or geometry
optimizations belong under the "Procedures" heading.

Indices
-------

A result can be found based off a unique tuple of ``(driver, program, molecule_id, keywords_set, method, basis)``

- ``driver`` - The type of calculation being evaluated (i.e. ``energy``, ``gradient``, ``hessian``, ``properties``)
- ``program`` - A lowercase string representation of the quantum chemistry program used (``gamess``, ``nwchem``, ``psi4``, etc.)
- ``molecule_id`` - The :term:`ObjectId` of the molecule used in the computation.
- ``keywords_set`` - The key to the options set stored in the database (e.g. ``default`` -> ``{"e_convergence": 1.e-7, "scf_type": "df", ...}``)
- ``method`` - A lowercase string representation of the method used in the computation (e.g. ``b3lyp``, ``mp2``, ``ccsd(t)``).
- ``basis`` - A lowercase string representation of the basis used in the computation (e.g. ``6-31g``, ``cc-pvdz``, ``def2-svp``)

Schema
------

All results are stored using the `QCSchema <https://molssi-qc-schema.readthedocs.io/en/latest/index.html>`_ so that the storage is quantum chemistry program agnostic. An example of the QCSchema input is shown below:

.. code-block:: python

    {
      "schema_name": "qc_json_input",
      "schema_version": 1,
      "molecule": {
        "geometry": [
          0.0,  0.0,    -0.1294,
          0.0, -1.4941,  1.0274,
          0.0,  1.4941,  1.0274
        ],
        "symbols": ["O", "H", "H"]
      },
      "driver": "energy",
      "model": {
        "method": "MP2",
        "basis": "cc-pVDZ"
      },
      "keywords": {},
    }

This input would correspond to the following output:

.. code-block:: python

    {
      "schema_name": "qc_json_output",
      "schema_version": 1,
      "molecule": {
        "geometry": [
          0.0,  0.0,    -0.1294,
          0.0, -1.4941,  1.0274,
          0.0,  1.4941,  1.0274
        ],
        "symbols": ["O", "H", "H"]
      },
      "driver": "energy",
      "model": {
        "method": "MP2",
        "basis": "cc-pVDZ"
      },
      "keywords": {},
      "provenance": {
        "creator": "QM Program",
        "version": "1.1",
        "routine": "module.json.run_json"
      },
      "return_result": -76.22836742810021,
      "success": true,
      "properties": {
        "calcinfo_nbasis": 24,
        "calcinfo_nmo": 24,
        "calcinfo_nalpha": 5,
        "calcinfo_nbeta": 5,
        "calcinfo_natom": 3,
        "return_energy": -76.22836742810021,
        "scf_one_electron_energy": -122.44534536383037,
        "scf_two_electron_energy": 37.62246494040059,
        "nuclear_repulsion_energy": 8.80146205625184,
        "scf_dipole_moment": [0.0, 0.0, 2.0954],
        "scf_iterations": 10,
        "scf_total_energy": -76.02141836717794,
        "mp2_same_spin_correlation_energy": -0.051980792916251864,
        "mp2_opposite_spin_correlation_energy": -0.15496826800602342,
        "mp2_singles_energy": 0.0,
        "mp2_doubles_energy": -0.20694906092226972,
        "mp2_total_correlation_energy": -0.20694906092226972,
        "mp2_total_energy": -76.22836742810021
      }
    }
