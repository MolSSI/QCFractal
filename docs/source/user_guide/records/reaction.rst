Reaction calculations
=====================================

A reaction record computes a linear combination of energies - a reaction energy, a binding energy,
an atomization energy, or anything else that can be written as a sum of coefficients times
molecular energies.

A reaction is defined by its *stoichiometry*: a list of (coefficient, molecule) pairs. Each molecule
is computed once, and the total energy is the sum of each molecule's energy multiplied by its
coefficient.

Coefficients are given by the user and are usually negative for reactants and positive for products.
For example, the dissociation energy of a water dimer is written as ``[(-1.0, dimer), (2.0, monomer)]``.

Each component may be optimized before its energy is evaluated, evaluated at the geometry as given,
or both. Reactions are :ref:`services <glossary_service>`, so the individual optimizations and
singlepoints are ordinary records that can be inspected on their own.


.. _reaction_record:

Reaction Records
----------------

Reaction records contain all the fields of a :doc:`base record <base>`, and additionally include:

- ``specification`` - The programs, levels of theory, and other options (see below)
- ``total_energy`` - The computed reaction energy, in hartrees. ``None`` until the record is complete
- ``components`` - The individual pieces of the reaction; one per molecule

Each element of ``components`` is a
:class:`~qcportal.reaction.record_models.ReactionComponent` with the following fields:

- ``molecule`` - The molecule as it was given in the stoichiometry. Note that this is the *input*
  geometry; if the component was optimized, the optimized geometry is on the optimization record
- ``molecule_id`` - The ID of that molecule
- ``coefficient`` - The coefficient of this molecule in the reaction
- ``singlepoint_id`` / ``singlepoint_record`` - The singlepoint computation for this component,
  if the specification has a singlepoint specification
- ``optimization_id`` / ``optimization_record`` - The optimization computation for this component,
  if the specification has an optimization specification

The energy that a component contributes to ``total_energy`` depends on which specifications
were given:

.. table::

  ==========================================  =======================================================
   Specification                               Energy used for each component
  ==========================================  =======================================================
   singlepoint only                            The energy of the singlepoint record
   optimization only                           The final energy of the optimization record
   both                                        The energy of the singlepoint record, which was run
                                               on the optimized geometry
  ==========================================  =======================================================


.. _reaction_specification:

Reaction Specification
----------------------

The :ref:`glossary_specification` for a reaction is a
:class:`~qcportal.reaction.record_models.ReactionSpecification`. The fields are:

- ``program`` - The program that drives the reaction service (``"reaction"``)
- ``singlepoint_specification`` - How the energy of each component should be evaluated. See
  :ref:`singlepoint_specification`. May be ``None``
- ``optimization_specification`` - How each component should be optimized before its energy is
  evaluated. See :ref:`optimization_specification`. May be ``None``
- ``keywords`` - A :class:`~qcportal.reaction.record_models.ReactionKeywords` object

At least one of ``singlepoint_specification`` and ``optimization_specification`` must be given.
Which ones you give determines what the service does:

- **Singlepoint only** - each molecule is computed at the geometry given in the stoichiometry.
  This is the usual choice when the geometries are already optimized.
- **Optimization only** - each molecule is optimized, and the final energy of the optimization is
  used. The level of theory comes from the optimization's ``qc_specification``
- **Both** - each molecule is optimized first, and then a separate singlepoint is run on the
  optimized geometry. This is how you optimize at a cheap level of theory and evaluate energies at
  a a more expensive one

.. note::

  :class:`~qcportal.reaction.record_models.ReactionKeywords` currently has no fields at all, so
  ``ReactionKeywords()`` is the only meaningful value. It is still a required field of the
  specification. It exists so that options can be added later without changing the shape of the
  specification.

.. dropdown:: Singlepoint energies at fixed geometries

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionSpecification, ReactionKeywords
        from qcportal.singlepoint import QCSpecification

        rxn_spec = ReactionSpecification(
            program="reaction",
            singlepoint_specification=QCSpecification(
                program="psi4",
                driver="energy",
                method="b3lyp",
                basis="def2-svp",
            ),
            keywords=ReactionKeywords(),
        )

.. dropdown:: Optimize each component, use the optimized energy

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionSpecification, ReactionKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        rxn_spec = ReactionSpecification(
            program="reaction",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(
                    program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"
                ),
            ),
            keywords=ReactionKeywords(),
        )

.. dropdown:: Optimize cheaply, then evaluate energies at a higher level of theory

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionSpecification, ReactionKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        rxn_spec = ReactionSpecification(
            program="reaction",
            # Optimize with b3lyp/def2-svp ...
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(
                    program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"
                ),
            ),
            # ... but take the energies from ccsd(t)/def2-tzvp
            singlepoint_specification=QCSpecification(
                program="psi4",
                driver="energy",
                method="ccsd(t)",
                basis="def2-tzvp",
            ),
            keywords=ReactionKeywords(),
        )


.. _reaction_submission:

Submitting Records
------------------

Reaction records are submitted with :meth:`~qcportal.client.PortalClient.add_reactions`.
This method takes the following information:

- ``stoichiometries`` - The reactions to compute. Each reaction is a list of
  ``(coefficient, molecule)`` pairs, so this is a **nested** list - one inner list per record
- ``program`` - The reaction program (use ``"reaction"``)
- ``singlepoint_specification`` - The singlepoint details, or ``None``
- ``optimization_specification`` - The optimization details, or ``None``
- ``keywords`` - A :class:`~qcportal.reaction.record_models.ReactionKeywords` object

Note that ``singlepoint_specification``, ``optimization_specification``, and ``keywords`` have no
defaults; the unused specification must be passed explicitly as ``None``.

The molecules in a stoichiometry may be :doc:`Molecule <../molecule>` objects or molecule IDs, and
the two may be mixed.

See :doc:`../record_submission` for more information about other arguments.


.. _reaction_dataset:

Reaction Datasets
-----------------

Reaction :ref:`datasets <glossary_dataset>` are collections of reaction records.
An :class:`entry <qcportal.reaction.dataset_models.ReactionDatasetEntry>` holds one reaction:

- ``name`` - The name of the entry
- ``stoichiometries`` - The coefficients and molecules of this reaction
- ``additional_keywords`` - Per-entry reaction keywords (see the note below)
- ``attributes`` - A user-defined dictionary of metadata for this entry
- ``comment`` - A user-supplied comment

The stoichiometry is a property of the *entry*, not of the specification - a reaction dataset is a
collection of different reactions, all computed the same way. The
:class:`dataset specification <qcportal.reaction.dataset_models.ReactionDatasetSpecification>`
holds the :class:`ReactionSpecification <qcportal.reaction.record_models.ReactionSpecification>`
that every entry is computed with.

When adding entries, each stoichiometry may be given either as a list of ``(coefficient, molecule)``
tuples or as a list of
:class:`~qcportal.reaction.dataset_models.ReactionDatasetEntryStoichiometry` objects.

.. note::

  Entries have an ``additional_keywords`` field, which mirrors the per-entry keyword overrides
  available in :ref:`torsiondrive <torsiondrive_dataset_keywords>` and
  :ref:`neb <neb_dataset_keywords>` datasets. Since :class:`~qcportal.reaction.record_models.ReactionKeywords` has no fields, and
  ignores keys it does not recognize, **anything placed in a reaction entry's**
  ``additional_keywords`` **is silently discarded** when the record is created. Leave it empty until
  reaction keywords actually exist.

See :doc:`../datasets/index` for general dataset operations and advanced usage.


.. _reaction_client_examples:

Client Examples
---------------

.. dropdown:: Obtain a single reaction record by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_reactions(123)

.. dropdown:: Obtain multiple reaction records by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_reactions([123, 456])

.. dropdown:: Obtain multiple reactions by ID, ignoring missing records

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_reactions([123, 456, 789], missing_ok=True)

.. dropdown:: Include the components and all their data during the initial fetch

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_reactions(123, include=['**'])

.. dropdown:: Query reactions by QC method and basis

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_reactions(qc_method='b3lyp', qc_basis='def2-svp')
        for r in r_iter:
            print(r.id, r.total_energy)

.. dropdown:: Query reactions that contain a particular molecule

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_reactions(molecule_id=8231)
        for r in r_iter:
            print(r.id)

.. dropdown:: Add a reaction record - singlepoint energies only

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionKeywords
        from qcportal.singlepoint import QCSpecification

        # Dissociation of a water dimer
        meta, ids = client.add_reactions(
            [[(-1.0, water_dimer), (2.0, water)]],
            program='reaction',
            singlepoint_specification=QCSpecification(
                program='psi4', driver='energy', method='b3lyp', basis='def2-svp'
            ),
            optimization_specification=None,
            keywords=ReactionKeywords(),
        )

.. dropdown:: Add a reaction record - optimize each component first

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        meta, ids = client.add_reactions(
            [[(-1.0, water_dimer), (2.0, water)]],
            program='reaction',
            singlepoint_specification=QCSpecification(
                program='psi4', driver='energy', method='ccsd(t)', basis='def2-tzvp'
            ),
            optimization_specification=OptimizationSpecification(
                program='geometric',
                qc_specification=QCSpecification(
                    program='psi4', method='b3lyp', basis='def2-svp', driver='deferred'
                ),
            ),
            keywords=ReactionKeywords(),
        )

.. dropdown:: Add several reactions at once

  Each inner list is one reaction, and therefore one record.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionKeywords
        from qcportal.singlepoint import QCSpecification

        meta, ids = client.add_reactions(
            [
                [(-1.0, water_dimer), (2.0, water)],
                [(-1.0, methanol_dimer), (2.0, methanol)],
            ],
            program='reaction',
            singlepoint_specification=QCSpecification(
                program='psi4', driver='energy', method='b3lyp', basis='def2-svp'
            ),
            optimization_specification=None,
            keywords=ReactionKeywords(),
        )

.. dropdown:: Inspect the components of a completed reaction

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> r = client.get_reactions(123)
        >>> print(r.total_energy)
        -0.008132215079

        >>> for c in r.components:
        ...     print(c.coefficient, c.molecule.get_molecular_formula(), c.singlepoint_record.properties['return_energy'])
        -1.0 H4O2 -152.1032871
        2.0 H2O -76.0475774


.. _reaction_dataset_examples:

Dataset Examples
----------------

See :doc:`../datasets/index` for more information and advanced usage.
See the :ref:`specification <reaction_specification>` section for all the options in creating
specifications.

.. dropdown:: Create a reaction dataset with default options

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds = client.add_dataset(
                 "reaction",
                 "Dataset Name",
                 "An example of a reaction dataset"
        )

.. dropdown:: Add a single entry to a reaction dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds.add_entry("water dimer dissociation", [(-1.0, water_dimer), (2.0, water)])

.. dropdown:: Add many entries to a reaction dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionDatasetNewEntry

        # Dimer/monomer pairs worked out beforehand
        dissociations = [
            ("water", water_dimer, water),
            ("methanol", methanol_dimer, methanol),
            ("ammonia", ammonia_dimer, ammonia),
        ]

        new_entries = [
            ReactionDatasetNewEntry(
                name=f"{name} dimer dissociation",
                stoichiometries=[(-1.0, dimer), (2.0, monomer)],
            )
            for name, dimer, monomer in dissociations
        ]

        # Efficiently add all entries in a single call
        ds.add_entries(new_entries)

.. dropdown:: Add entries using explicit stoichiometry objects

  Equivalent to passing tuples, but easier to read when the reaction has several components.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionDatasetNewEntry
        from qcportal.reaction.dataset_models import ReactionDatasetEntryStoichiometry

        ent = ReactionDatasetNewEntry(
            name="methane combustion",
            stoichiometries=[
                ReactionDatasetEntryStoichiometry(coefficient=-1.0, molecule=methane),
                ReactionDatasetEntryStoichiometry(coefficient=-2.0, molecule=o2),
                ReactionDatasetEntryStoichiometry(coefficient=1.0, molecule=co2),
                ReactionDatasetEntryStoichiometry(coefficient=2.0, molecule=water),
            ],
        )

        ds.add_entries(ent)

.. dropdown:: Add a specification to a reaction dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.reaction import ReactionSpecification, ReactionKeywords
        from qcportal.singlepoint import QCSpecification

        rxn_spec = ReactionSpecification(
            program="reaction",
            singlepoint_specification=QCSpecification(
                program="psi4",
                driver="energy",
                method="b3lyp",
                basis="def2-svp",
            ),
            keywords=ReactionKeywords(),
        )

        ds.add_specification("psi4/b3lyp/def2-svp", rxn_spec)

.. dropdown:: Compare reaction energies across specifications

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> for e_name, s_name, record in ds.iterate_records(status='complete'):
        ...     print(e_name, s_name, record.total_energy)
        water dimer dissociation psi4/b3lyp/def2-svp -0.008132215079
        water dimer dissociation psi4/mp2/def2-tzvp -0.007984120012


.. _reaction_qcportal_api:

Reaction QCPortal API
---------------------

* :mod:`Record models <qcportal.reaction.record_models>`
* :mod:`Dataset models <qcportal.reaction.dataset_models>`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.add_reactions`
  * :meth:`~qcportal.client.PortalClient.get_reactions`
  * :meth:`~qcportal.client.PortalClient.query_reactions`
