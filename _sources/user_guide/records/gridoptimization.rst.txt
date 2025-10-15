Gridoptimization calculations
=====================================

Grid optimizations perform a multi-dimensional scan over internal coordinates (distance, angle, dihedral).
At each grid point, a constrained geometry optimization is performed to minimize the energy while holding
selected coordinates fixed. This produces a grid of optimized structures and their final energies.


.. _gridoptimization_record:

Gridoptimization Records
------------------------

Gridoptimization records contain all the fields of a :doc:`base record <base>`, and additionally include:

- ``initial_molecule`` - The molecule used as the starting geometry for the scan
- ``starting_molecule`` - The molecule actually used to start the scan (after optional preoptimization)
- ``starting_grid`` - The grid indices where the scan started (if relative steps were used)
- ``optimizations`` - A mapping from grid keys to each constrained :class:`~qcportal.optimization.record_models.OptimizationRecord`
- ``specification`` - The scan/optimization setup (see below)

The optimizations can be accessed via the :attr:`~qcportal.gridoptimization.record_models.GridoptimizationRecord.optimizations`
property, which returns a dictionary mapping grid keys to optimization records. Grid keys are tuples of integers
(one per scan dimension) that index the steps defined in your scan. A special key ``"preoptimization"`` is used
for the optional initial unconstrained optimization.

The convenience property :attr:`~qcportal.gridoptimization.record_models.GridoptimizationRecord.final_energies`
returns a dictionary mapping grid keys (tuples) to the final energy from each optimization.

.. note:: The mapping keys are represented as strings when serialized;
   use :func:`~qcportal.gridoptimization.record_models.serialize_key` and
   :func:`~qcportal.gridoptimization.record_models.deserialize_key` for conversions if needed.


.. _gridoptimization_specification:

Gridoptimization Specification
------------------------------

The :ref:`glossary_specification` for a grid optimization is a
:class:`~qcportal.gridoptimization.record_models.GridoptimizationSpecification`. The key fields are:

- ``program`` - The gridoptimization driver program (``"gridoptimization"``)
- ``optimization_specification`` - How each constrained optimization should be run; see
  :ref:`optimization_specification`
- ``keywords`` - The scan definition and related options (see below)

The keywords are provided via :class:`~qcportal.gridoptimization.record_models.GridoptimizationKeywords` and include:

- ``scans`` - A list of :class:`~qcportal.gridoptimization.record_models.ScanDimension` objects
  describing each scan dimension:

  - ``type`` - One of :class:`~qcportal.gridoptimization.record_models.ScanTypeEnum` (``distance``, ``angle``, ``dihedral``)
  - ``indices`` - Atom indices. This is zero-indexed, so the first atom of the molecule has index 0. There are 2 indices
    for distance, 3 for angle, 4 for dihedral.
  - ``steps`` - A strictly monotonic list of values; units are Bohr for distances and degrees for angles/dihedrals
  - ``step_type`` - :class:`~qcportal.gridoptimization.record_models.StepTypeEnum` (``absolute`` or ``relative``)

- ``preoptimization`` - If ``True``, perform an unconstrained optimization before scanning. This is especially
  useful with ``relative`` step types so that relative steps are taken from a relaxed starting geometry.

Note: The constrained optimizations at each grid point use the specification supplied by the
:class:`~qcportal.optimization.record_models.OptimizationSpecification`, including the level of theory
and programs. See :ref:`optimization_specification`.


Absolute vs. Relative step_type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each :class:`~qcportal.gridoptimization.record_models.ScanDimension` declares how its ``steps`` should be
interpreted via the ``step_type`` field:

- ``absolute`` – The values in ``steps`` are the absolute target values for the corresponding internal coordinate.
  For example, a dihedral scan with ``steps=[-180, -165, ..., 180]`` constrains that dihedral to those exact
  angles at each grid point. Likewise, a bond-distance scan with ``steps=[2.5, 2.6, 2.7]`` (in Bohr) constrains the
  distance to those exact values.
- ``relative`` – The values in ``steps`` are offsets relative to the value measured on the starting molecule for the
  scan. At submission time the server constructs the constraints by adding the offset to the measured value on the
  starting geometry. Concretely, for a given grid index ``i`` the constraint value used is
  ``steps[i] + measure(starting_molecule, indices)``.

Starting molecule and starting grid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- If ``preoptimization`` is ``True``, the starting molecule is the final geometry from an initial unconstrained
  optimization of the input structure. If ``preoptimization`` is ``False``, the starting molecule is simply the
  initial molecule you submitted.
- The ``starting_grid`` stored on the record is the index tuple of the first constrained optimization that is launched:
  it is chosen as the grid point whose target value is closest to the starting molecule. For ``absolute`` dimensions
  this means picking the step closest to ``measure(starting_molecule, indices)``; for ``relative`` dimensions this means
  picking the step closest to ``0`` (no change).

Practical tips
~~~~~~~~~~~~~~

- Use ``relative`` steps when you want to explore around the current structure without having to compute absolute
  values ahead of time (for example, ``[-0.2, 0.0, 0.2]`` Bohr around the current bond length, or ``[-10, 0, 10]`` degrees
  around a torsion).
- Use ``absolute`` steps when you want the grid aligned to specific absolute values (for example, a canonical
  ``[-180, -165, ..., 180]`` dihedral scan).
- ``preoptimization=True`` is often helpful with ``relative`` scans so the offsets are applied from a relaxed geometry.

Examples of building specifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^



.. dropdown:: Single-dimension dihedral scan with preoptimization

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords, ScanDimension
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        go_spec = GridoptimizationSpecification(
            program="gridoptimization",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            ),
            keywords=GridoptimizationKeywords(
                scans=[
                    ScanDimension(type="dihedral", indices=[0, 1, 2, 3], step_type="absolute", steps=list(range(-180, 181, 15)))
                ],
                preoptimization=True,
            ),
        )

.. dropdown:: Two-dimensional scan combining a bond stretch and angle bend

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords, ScanDimension
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        go_spec = GridoptimizationSpecification(
            program="gridoptimization",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="wb97x-d", basis="def2-tzvp", driver="deferred"),
            ),
            keywords=GridoptimizationKeywords(
                scans=[
                    ScanDimension(type="distance", indices=[0, 1], step_type="relative", steps=[-0.2, 0.0, 0.2]),
                    ScanDimension(type="angle", indices=[1, 2, 3], step_type="absolute", steps=[90.0, 110.0, 130.0]),
                ],
                preoptimization=True,
            ),
        )

.. dropdown:: Relative dihedral offsets around the starting torsion (with preoptimization)

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords, ScanDimension
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        go_spec = GridoptimizationSpecification(
            program="gridoptimization",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            ),
            keywords=GridoptimizationKeywords(
                scans=[
                    # Offsets in degrees relative to the starting torsion value
                    ScanDimension(type="dihedral", indices=[0, 1, 2, 3], step_type="relative", steps=[-30.0, 0.0, 30.0])
                ],
                preoptimization=True,
            ),
        )

.. dropdown:: Relative bond-length scan without preoptimization

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords, ScanDimension
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        go_spec = GridoptimizationSpecification(
            program="gridoptimization",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="wb97x-d", basis="def2-tzvp", driver="deferred"),
            ),
            keywords=GridoptimizationKeywords(
                scans=[
                    # Offsets in Bohr relative to the starting bond length
                    ScanDimension(type="distance", indices=[0, 1], step_type="relative", steps=[-0.2, 0.0, 0.2])
                ],
                preoptimization=False,
            ),
        )

.. _gridoptimization_submission:

Submitting Records
------------------

Gridoptimization records can be submitted using a client via the :meth:`~qcportal.client.PortalClient.add_gridoptimizations` method.
This method takes the following key information:

- ``initial_molecules`` - A single molecule or list of molecules to scan
- ``program`` - The gridoptimization program (use ``"gridoptimization"``)
- ``optimization_specification`` - The optimization setup for each grid point (see :ref:`gridoptimization_specification`)
- ``keywords`` - The grid scan definition and options

See :doc:`../record_submission` for more information about other common fields such as compute tag and priority.


Client Examples
---------------

.. dropdown:: Obtain a single gridoptimization record by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_gridoptimizations(123)

.. dropdown:: Obtain multiple gridoptimization records by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_gridoptimizations([123, 456])

.. dropdown:: Include molecules and child optimizations during initial fetch

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_gridoptimizations(123, include=["**"])  # include all data

.. dropdown:: Query gridoptimizations by QC method/basis and include optimizations

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_gridoptimizations(qc_method='b3lyp', qc_basis='def2-svp', include=['optimizations'])
        for r in r_iter:
            print(r.id, len(r.optimizations))

.. dropdown:: Submit gridoptimizations

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords, ScanDimension
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        spec = GridoptimizationSpecification(
            program='gridoptimization',
            optimization_specification=OptimizationSpecification(
                program='geometric',
                qc_specification=QCSpecification(program='psi4', method='b3lyp', basis='def2-svp', driver='deferred'),
            ),
            keywords=GridoptimizationKeywords(
                scans=[ScanDimension(type='dihedral', indices=[0,1,2,3], step_type='absolute', steps=list(range(-180,181,15)))],
                preoptimization=True,
            ),
        )

        meta, ids = client.add_gridoptimizations([mol1, mol2], program='gridoptimization', optimization_specification=spec.optimization_specification, keywords=spec.keywords)


.. _gridoptimization_dataset:

Gridoptimization Datasets
-------------------------

Gridoptimization :ref:`datasets <glossary_dataset>` are collections of gridoptimization records.
:class:`Entries <qcportal.gridoptimization.dataset_models.GridoptimizationDatasetEntry>` contain a single initial molecule
and optional metadata. The :class:`dataset specifications <qcportal.gridoptimization.dataset_models.GridoptimizationDatasetSpecification>`
wrap a :class:`GridoptimizationSpecification <qcportal.gridoptimization.record_models.GridoptimizationSpecification>`.

See :doc:`../datasets/index` for general dataset operations and advanced usage.

Dataset Examples
----------------

.. dropdown:: Create a gridoptimization dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds = client.add_dataset("gridoptimization", "Grid Scan Dataset", "A demonstration grid scan dataset")

.. dropdown:: Add entries to a gridoptimization dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.gridoptimization import GridoptimizationDatasetEntry
        from qcportal.molecules import Molecule

        entries = []
        for name, geom in [("butane", butane_geom), ("ethane", ethane_geom)]:
            mol = Molecule.from_data(geom) if isinstance(geom, str) else Molecule(**geom)
            entries.append(GridoptimizationDatasetEntry(name=name, initial_molecule=mol))

        ds.add_entries(entries)

.. dropdown:: Add a specification to a gridoptimization dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords, ScanDimension
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        go_spec = GridoptimizationSpecification(
            program="gridoptimization",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            ),
            keywords=GridoptimizationKeywords(
                scans=[ScanDimension(type="dihedral", indices=[0, 1, 2, 3], step_type="absolute", steps=list(range(-180, 181, 60)))],
                preoptimization=True,
            ),
        )

        ds.add_specification("go-geometric/psi4-b3lyp-def2-svp", go_spec)


.. _gridoptimization_qcportal_api:

Gridoptimization QCPortal API
-----------------------------

* :mod:`Record models <qcportal.gridoptimization.record_models>`
* :mod:`Dataset models <qcportal.gridoptimization.dataset_models>`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.add_gridoptimizations`
  * :meth:`~qcportal.client.PortalClient.get_gridoptimizations`
  * :meth:`~qcportal.client.PortalClient.query_gridoptimizations`