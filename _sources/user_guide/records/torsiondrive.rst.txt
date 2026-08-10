Torsiondrive calculations
=====================================

Torsiondrives perform constrained scans over one or more dihedral angles.
At each grid point (a specific set of dihedral values), a constrained geometry
optimization is performed to minimize the energy while keeping the target
dihedral(s) fixed. This produces a map of optimized structures and energies
across the dihedral grid.


.. _torsiondrive_record:

Torsiondrive Records
--------------------

Torsiondrive records contain all the fields of a :doc:`base record <base>`, and additionally include:

- ``initial_molecules`` - The starting molecules for the scan (one torsiondrive can start from multiple initial geometries)
- ``optimizations`` - A mapping from grid keys to lists of constrained
  :class:`~qcportal.optimization.record_models.OptimizationRecord` objects
- ``minimum_optimizations`` - A mapping from grid keys to the selected lowest-energy
  :class:`~qcportal.optimization.record_models.OptimizationRecord` for each grid point
- ``specification`` - The scan/optimization setup (see below)

Grid keys identify points in the dihedral scan. They are tuples whose length equals the
number of scanned dihedrals.

Convenience properties include:

- :attr:`~qcportal.torsiondrive.record_models.TorsiondriveRecord.optimizations` -
  Returns a dictionary mapping grid keys (tuples) to lists of optimization records run at that key.
- :attr:`~qcportal.torsiondrive.record_models.TorsiondriveRecord.minimum_optimizations` -
  Returns a dictionary mapping grid keys (tuples) to the lowest-energy optimization record at each key.
- :attr:`~qcportal.torsiondrive.record_models.TorsiondriveRecord.final_energies` -
  Returns a dictionary mapping grid keys (tuples) to the final energy (a.u.) from the minimum optimization at that key.


.. _torsiondrive_specification:

Torsiondrive Specification
--------------------------

The :ref:`glossary_specification` for a torsiondrive is a
:class:`~qcportal.torsiondrive.record_models.TorsiondriveSpecification`. The key fields are:

- ``program`` - The torsiondrive driver program (``"torsiondrive"``)
- ``optimization_specification`` - How each constrained optimization should be run; see
  :ref:`optimization_specification`
- ``keywords`` - Torsiondrive scan definition and related options

Keywords are provided via :class:`~qcportal.torsiondrive.record_models.TorsiondriveKeywords` and include:

- ``dihedrals`` (list of 4-tuples of ints) - The dihedral indices to scan.
- ``grid_spacing`` (list of ints, degrees) - The grid spacing for each dihedral. If multiple
  values are given, they are paired element-wise with the dihedrals.
- ``dihedral_ranges`` (optional list of (lower, upper) degree pairs) - Limits the scan range for each dihedral.
- ``energy_decrease_thresh`` (float, optional) - Smallest energy decrease to trigger launching
  new optimizations from a grid point.
- ``energy_upper_limit`` (float, optional, a.u.) - Skip launching new optimizations whose starting energy is above
  the current global minimum by more than this amount.

Note: The constrained optimizations at each grid point use the details supplied by the
:class:`~qcportal.optimization.record_models.OptimizationSpecification`, which in turn references the
singlepoint QC settings inside its ``qc_specification``. See :ref:`singlepoint_specification` and
:ref:`optimization_specification` for more.

.. important::

  Every field of :class:`~qcportal.torsiondrive.record_models.TorsiondriveKeywords` has a default,
  so ``TorsiondriveKeywords()`` is valid and describes no scan at all. That is deliberate: in a
  :ref:`torsiondrive dataset <torsiondrive_dataset>` the keywords are normally left empty in the
  specification and supplied per entry instead, because ``dihedrals`` refers to atom indices that
  differ from molecule to molecule. See :ref:`torsiondrive_dataset_keywords`.

  When submitting a single record with :meth:`~qcportal.client.PortalClient.add_torsiondrives`
  there are no entries, so the keywords must be given here.


.. dropdown:: Basic TorsiondriveSpecification with a single dihedral

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        td_spec = TorsiondriveSpecification(
            program="torsiondrive",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            ),
            keywords=TorsiondriveKeywords(
                dihedrals=[(0, 1, 2, 3)],
                grid_spacing=[15],
            ),
        )


.. dropdown:: Multiple dihedrals with different spacings and ranges

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        td_spec = TorsiondriveSpecification(
            program="torsiondrive",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="wb97x-d", basis="def2-tzvp", driver="deferred"),
            ),
            keywords=TorsiondriveKeywords(
                dihedrals=[(0, 1, 2, 3), (1, 2, 3, 4)],
                grid_spacing=[15, 30],
                dihedral_ranges=[(-180, 180), (-120, 120)],
                energy_upper_limit=0.05,
            ),
        )


.. _torsiondrive_submission:

Submitting Records
------------------

Torsiondrive records can be submitted using a client via the :meth:`~qcportal.client.PortalClient.add_torsiondrives` method.
This method takes the following key information:

- ``initial_molecules`` - A list of lists of molecules. Each torsiondrive may start from multiple initial molecules,
  so pass a nested list (one inner list per record).
- ``program`` - The torsiondrive program (use ``"torsiondrive"``)
- ``optimization_specification`` - The optimization setup for each grid point (see :ref:`torsiondrive_specification`)
- ``keywords`` - The torsiondrive dihedral scan definition and options

See :doc:`../record_submission` for more information about other common fields such as compute tag and priority.


Client Examples
---------------

.. dropdown:: Obtain a single torsiondrive record by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_torsiondrives(123)

.. dropdown:: Obtain multiple torsiondrive records by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_torsiondrives([123, 456])

.. dropdown:: Include child optimizations and their data during fetch

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_torsiondrives(123, include=["optimizations", "**"])  # include full child data
        # Access optimizations via a mapping from grid keys (tuples) to lists of OptimizationRecord
        for key, opt_list in r.optimizations.items():
            print(key, [opt.id for opt in opt_list])

.. dropdown:: Query torsiondrives by QC method/basis and include optimizations

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_torsiondrives(qc_method='b3lyp', qc_basis='def2-svp', include=['optimizations'])
        for r in r_iter:
            print(r.id, len(r.optimizations))

.. dropdown:: Submit torsiondrives with two different starting geometries

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        td_spec = TorsiondriveSpecification(
            program='torsiondrive',
            optimization_specification=OptimizationSpecification(
                program='geometric',
                qc_specification=QCSpecification(program='psi4', method='b3lyp', basis='def2-svp', driver='deferred'),
            ),
            keywords=TorsiondriveKeywords(
                dihedrals=[(0,1,2,3)],
                grid_spacing=[15],
            ),
        )

        # Two initial conformers for a single torsiondrive
        meta, ids = client.add_torsiondrives(
            initial_molecules=[[mol_conf1, mol_conf2]],
            program='torsiondrive',
            optimization_specification=td_spec.optimization_specification,
            keywords=td_spec.keywords,
        )


Working with Results
--------------------

- Access the starting molecules via :attr:`~qcportal.torsiondrive.record_models.TorsiondriveRecord.initial_molecules`.
- Access all optimizations per grid point via :attr:`~qcportal.torsiondrive.record_models.TorsiondriveRecord.optimizations`.
- Access the lowest-energy optimization per grid point via
  :attr:`~qcportal.torsiondrive.record_models.TorsiondriveRecord.minimum_optimizations`.
- Get a simple map of final energies via :attr:`~qcportal.torsiondrive.record_models.TorsiondriveRecord.final_energies`.

When writing/reading keys (for example, when working with serialized data), use
:func:`~qcportal.torsiondrive.record_models.serialize_key` and
:func:`~qcportal.torsiondrive.record_models.deserialize_key`.


.. _torsiondrive_dataset:

Torsiondrive Datasets
---------------------

Torsiondrive :ref:`datasets <glossary_dataset>` are collections of torsiondrive records.
:class:`Entries <qcportal.torsiondrive.dataset_models.TorsiondriveDatasetEntry>` contain one or more
initial molecules, and the
:class:`dataset specifications <qcportal.torsiondrive.dataset_models.TorsiondriveDatasetSpecification>`
wrap a :class:`TorsiondriveSpecification <qcportal.torsiondrive.record_models.TorsiondriveSpecification>`.

Torsiondrive datasets differ from the other dataset types in one important way. For most dataset
types the specification is self-contained and the entry supplies only the molecule. Here, the
scan definition itself - which dihedral to drive - depends on the atom indices of the particular
molecule, and those differ from entry to entry. So a torsiondrive entry carries keywords of its
own, and the two are combined when records are created.

An entry has two keyword fields in addition to its molecules:

- ``additional_keywords`` - merged into the specification's torsiondrive
  :class:`~qcportal.torsiondrive.record_models.TorsiondriveKeywords`. This is where ``dihedrals``
  and ``grid_spacing`` normally live
- ``additional_optimization_keywords`` - merged into the specification's
  ``optimization_specification.keywords``

See :doc:`../datasets/index` for general dataset operations and advanced usage.


.. _torsiondrive_dataset_keywords:

How entry and specification keywords are combined
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a dataset is :ref:`submitted <dataset_submission>`, the server builds one torsiondrive
specification for each (entry, specification) pair by taking the dataset specification and
updating it from the entry:

.. table::

  ===================================================  ==============================================================
   Specification field                                  Updated from the entry's
  ===================================================  ==============================================================
   ``keywords``                                         ``additional_keywords``
   ``optimization_specification.keywords``              ``additional_optimization_keywords``
  ===================================================  ==============================================================

The merge is a plain dictionary update, one level deep. A key present in the entry replaces that
key in the specification outright; keys the entry does not mention are left as they were. Values
are not combined element-wise - an entry that sets ``grid_spacing`` replaces the whole list, it
does not append to it.

Nothing else in the specification is affected. The ``optimization_specification``'s ``program``,
``qc_specification``, and ``protocols`` come from the dataset specification alone and cannot be
varied per entry.

Because ``dihedrals`` is molecule-specific, the usual arrangement is:

- **The dataset specification** holds the level of theory - the optimization program, the QC
  program, method, and basis - and leaves ``keywords`` empty
- **Each entry** holds that molecule's ``dihedrals`` and ``grid_spacing``

Both halves are still ordinary keyword dictionaries, though, so anything that genuinely is common
to the whole dataset can go in the specification instead and be inherited by every entry. Settings
that are common to most entries but not all can be put in the specification and overridden by the
handful of entries that differ. The examples below show each arrangement.

.. warning::

  ``additional_keywords`` is stored as an unvalidated dictionary on the entry. It is only checked
  against :class:`~qcportal.torsiondrive.record_models.TorsiondriveKeywords`, which forbids unknown
  fields, at the point where the merged specification is built - that is, on
  :ref:`submission <dataset_submission>`, not when the entry is added.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> # Note the typo - 'grid_spacings'. This is accepted without complaint
        >>> ds.add_entry("butane", [butane_conf1], additional_keywords={"grid_spacings": [15]})

  Because that validation failure happens inside the server, it does not come back as a friendly
  message - on a server with the default ``hide_internal_errors`` setting you will get an internal
  server error and an error ID rather than the name of the offending key. If a submission fails for
  no apparent reason, check the spelling of the keys in your entries against the field names of
  :class:`~qcportal.torsiondrive.record_models.TorsiondriveKeywords`.

  ``additional_optimization_keywords`` is not checked this way, since
  ``OptimizationSpecification.keywords`` is an open dictionary passed through to the optimization
  program. A misspelled key there is silently ignored, or rejected later by the optimizer itself.

Dataset Examples
----------------

.. dropdown:: Create a torsiondrive dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds = client.add_dataset("torsiondrive", "Torsion Scan Dataset", "A demonstration torsiondrive dataset")

.. dropdown:: Add a specification with empty keywords (the usual case)

  The specification carries the level of theory only. ``keywords`` is left at its default, so
  every field of the scan definition comes from the entries.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        td_spec = TorsiondriveSpecification(
            program="torsiondrive",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            ),
            keywords=TorsiondriveKeywords(),
        )

        ds.add_specification("td-geometric/psi4-b3lyp-def2-svp", td_spec)

.. dropdown:: Add entries with per-entry dihedrals and grid spacing

  Each entry drives a different dihedral, given as atom indices into that entry's own molecules.
  This is the reason the scan definition lives on the entry rather than the specification.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        # The central C-C torsion of butane, and the C-C torsion of ethane -
        # different atom indices in each molecule
        ds.add_entry(
            "butane",
            [butane_conf1, butane_conf2],
            additional_keywords={"dihedrals": [(0, 1, 2, 3)], "grid_spacing": [15]},
        )

        ds.add_entry(
            "ethane",
            [ethane_conf],
            additional_keywords={"dihedrals": [(2, 0, 1, 5)], "grid_spacing": [15]},
        )

        ds.submit()

.. dropdown:: Add many entries at once, building the entry objects directly

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.torsiondrive import TorsiondriveDatasetNewEntry

        # Molecules and the dihedral to drive in each, worked out beforehand
        to_scan = [
            ("butane", [butane_conf1, butane_conf2], (0, 1, 2, 3)),
            ("ethane", [ethane_conf], (2, 0, 1, 5)),
            ("propane", [propane_conf], (0, 1, 2, 6)),
        ]

        entries = [
            TorsiondriveDatasetNewEntry(
                name=name,
                initial_molecules=mols,
                additional_keywords={"dihedrals": [dihedral], "grid_spacing": [15]},
            )
            for name, mols, dihedral in to_scan
        ]

        # Efficiently add all entries in a single call
        ds.add_entries(entries)

.. dropdown:: Put shared keywords in the specification instead

  Options that are the same for every entry can go in the specification, where they only have to
  be written once. Here the spacing and range are shared, and the entries supply only
  ``dihedrals``.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        td_spec = TorsiondriveSpecification(
            program="torsiondrive",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            ),
            keywords=TorsiondriveKeywords(
                grid_spacing=[15],
                dihedral_ranges=[(-165, 180)],
                energy_upper_limit=0.05,
            ),
        )

        ds.add_specification("coarse", td_spec)

        # Entries only need to say which dihedral to drive
        ds.add_entry("butane", [butane_conf1], additional_keywords={"dihedrals": [(0, 1, 2, 3)]})
        ds.add_entry("ethane", [ethane_conf], additional_keywords={"dihedrals": [(2, 0, 1, 5)]})

.. dropdown:: Override a specification keyword for one entry

  An entry's ``additional_keywords`` replaces matching keys and leaves the rest alone, so a single
  entry can deviate from the dataset-wide settings. With the ``"coarse"`` specification above:

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        # Uses the specification's grid_spacing of 15
        ds.add_entry("butane", [butane_conf1], additional_keywords={"dihedrals": [(0, 1, 2, 3)]})

        # Scanned more finely, but still inherits dihedral_ranges and energy_upper_limit
        ds.add_entry(
            "butane_fine",
            [butane_conf1],
            additional_keywords={"dihedrals": [(0, 1, 2, 3)], "grid_spacing": [5]},
        )

  The record created for ``butane_fine`` gets ``grid_spacing=[5]``,
  ``dihedral_ranges=[(-165, 180)]``, and ``energy_upper_limit=0.05``.

.. dropdown:: Pass keywords through to the optimizer per entry

  ``additional_optimization_keywords`` is merged into ``optimization_specification.keywords`` the
  same way, which is useful when one awkward molecule needs different optimizer settings.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        td_spec = TorsiondriveSpecification(
            program="torsiondrive",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
                keywords={"maxiter": 200},
            ),
            keywords=TorsiondriveKeywords(grid_spacing=[15]),
        )

        ds.add_specification("default", td_spec)

        # Inherits maxiter=200
        ds.add_entry("butane", [butane_conf1], additional_keywords={"dihedrals": [(0, 1, 2, 3)]})

        # Overrides maxiter and adds another optimizer keyword
        ds.add_entry(
            "stubborn_molecule",
            [stubborn_conf],
            additional_keywords={"dihedrals": [(4, 5, 6, 7)]},
            additional_optimization_keywords={"maxiter": 500, "coordsys": "dlc"},
        )

  The record for ``stubborn_molecule`` is optimized with
  ``keywords={"maxiter": 500, "coordsys": "dlc"}`` - ``maxiter`` replaced, ``coordsys`` added.


Notes and Tips
--------------

- Each grid point corresponds to a full :ref:`optimization record <optimization_record>`.
  You can access trajectory information and final structures/energies via those records.
- Torsiondrive keys represent dihedral angle positions; when serialized to strings (eg in JSON),
  use helper functions to convert to/from tuples if needed.
- See :ref:`singlepoint_specification` and :ref:`optimization_specification` for details on setting up the
  underlying QC and optimization parameters used at each grid point.
- In a dataset, ``dihedrals`` belongs on the entry, not the specification - see
  :ref:`torsiondrive_dataset_keywords`. The finished record's ``specification`` shows the merged
  result, so a record's keywords will not look identical to the dataset specification it came from.
- ``driver="deferred"`` is the conventional setting for the ``qc_specification`` inside an
  optimization specification, since the driver used at each step is decided by the optimizer.


.. _torsiondrive_qcportal_api:

Torsiondrive QCPortal API
-------------------------

* :mod:`Record models <qcportal.torsiondrive.record_models>`
* :mod:`Dataset models <qcportal.torsiondrive.dataset_models>`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.add_torsiondrives`
  * :meth:`~qcportal.client.PortalClient.get_torsiondrives`
  * :meth:`~qcportal.client.PortalClient.query_torsiondrives`
