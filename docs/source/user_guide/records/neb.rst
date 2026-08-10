NEB calculations
=====================================

A nudged elastic band (NEB) calculation locates the minimum energy path between two endpoints -
typically a reactant and a product - and from that path produces a guess at the transition state
structure connecting them.

The path is represented by a *chain*: an ordered list of molecular geometries (called *images*)
running from one endpoint to the other. Each iteration computes the gradient at every image, and
those gradients, together with spring forces that keep neighbouring images from sliding into each
other, are used to move the whole chain downhill toward the minimum energy path. When the chain
stops moving, the highest-energy image is a rough transition state.

NEB records are :ref:`services <glossary_service>`. Every gradient evaluation is an ordinary
:ref:`singlepoint record <singlepoint_record>`, and the optional endpoint and transition-state
optimizations are ordinary :ref:`optimization records <optimization_record>`, all of which can be
inspected individually. A single NEB record therefore accumulates a large number of child records:
the number of images multiplied by the number of iterations.

Optionally, the NEB service can also:

- optimize the two endpoints of the chain before starting (``optimize_endpoints``), and
- refine the guessed transition state into a true first-order saddle point, by computing its
  Hessian and running a transition-state optimization (``optimize_ts``).


.. _neb_record:

NEB Records
-----------

NEB records contain all the fields of a :doc:`base record <base>`, and additionally include:

- ``specification`` - The programs, level of theory, and NEB options (see below)
- ``initial_chain`` - The chain of molecules the calculation started from, as it was submitted
- ``singlepoints`` - The gradient calculations, as a dictionary keyed by chain iteration. Each value
  is the list of :class:`~qcportal.singlepoint.record_models.SinglepointRecord` for that iteration,
  ordered by position along the chain
- ``final_chain`` - Shorthand for the singlepoints of the last iteration; that is, the converged path
- ``result`` - The guessed transition state: the :doc:`molecule <../molecule>` of the
  highest-energy image of the final chain
- ``optimizations`` - Any endpoint or transition-state optimizations, as a dictionary (see below)
- ``ts_optimization`` - Shorthand for the transition-state optimization, or ``None`` if there was not
  one
- ``ts_hessian`` - The Hessian :class:`~qcportal.singlepoint.record_models.SinglepointRecord`
  computed on the guessed transition state before optimizing it, or ``None``

The keys of the ``optimizations`` dictionary describe which optimization each one is:

.. table::

  ================  =========================================================================
   Key               Optimization
  ================  =========================================================================
   ``initial``       The first image of the chain, optimized before the NEB started
   ``final``         The last image of the chain, optimized before the NEB started
   ``transition``    The guessed transition state, optimized to a first-order saddle point
  ================  =========================================================================

``initial`` and ``final`` are present only when ``optimize_endpoints`` was set, and ``transition``
only when ``optimize_ts`` was set, so the dictionary is frequently empty.

.. note::

  ``result`` raises ``ValueError: NEB result is only available after the calculation is complete.``
  unless the record's status is ``complete``. It is the *guessed* transition state taken straight
  off the chain, not the result of the transition-state optimization; for that, use
  ``ts_optimization.final_molecule``.

.. note::

  The record's underlying pydantic fields (``neb_result_``, ``singlepoints_``, ``optimizations_``,
  and so on) are private storage for what has been fetched from the server. Use the properties listed
  above - they fetch what they need on demand.


.. _neb_specification:

NEB Specification
-----------------

The :ref:`glossary_specification` for a NEB is a
:class:`~qcportal.neb.record_models.NEBSpecification`. The fields are:

- ``program`` - The program that drives the NEB (``"geometric"``)
- ``singlepoint_specification`` - The level of theory for the gradient calculation at each image.
  See :ref:`singlepoint_specification`
- ``optimization_specification`` - Optional; how the transition state should be optimized when
  ``optimize_ts`` is set. See :ref:`optimization_specification`
- ``keywords`` - A :class:`~qcportal.neb.record_models.NEBKeywords` object holding the NEB options

Unlike most other specifications, ``keywords`` is required and has no default - pass
``NEBKeywords()`` to accept every default.

.. _neb_keywords:

NEB Keywords
~~~~~~~~~~~~

:class:`~qcportal.neb.record_models.NEBKeywords` has the following fields:

.. table::

  ==============================  ===========  =========================================================
   Keyword                         Default      Description
  ==============================  ===========  =========================================================
   ``images``                      11           Number of images used to locate a rough transition
                                                state structure. Must be greater than 5
   ``spring_constant``             1.0          Spring constant, in kcal/mol/Ang^2
   ``spring_type``                 0            How spring forces and gradients are combined; see below
   ``maximum_force``               0.05         Convergence criterion: converge when the maximum
                                                RMS-gradient of the chain (eV/Ang) falls below this
   ``average_force``               0.025        Convergence criterion: converge when the average
                                                RMS-gradient of the chain (eV/Ang) falls below this
   ``maximum_cycle``               100          Maximum number of NEB iterations
   ``optimize_endpoints``          ``False``    Optimize the two ends of the initial chain before
                                                starting the NEB
   ``optimize_ts``                 ``False``    After convergence, optimize the guessed transition
                                                state to a first-order saddle point
   ``align``                       ``True``     Align the images before starting
   ``epsilon``                     1e-5         Small eigenvalue threshold for resetting the Hessian
  ==============================  ===========  =========================================================

``spring_type`` selects how the spring force and the gradient are projected:

.. table::

  =======  ==============================================================================
   Value    Meaning
  =======  ==============================================================================
   0        Nudged elastic band - parallel spring force + perpendicular gradients
   1        Hybrid elastic band - full spring force + perpendicular gradients
   2        Plain elastic band - full spring force + full gradients
  =======  ==============================================================================

Both convergence criteria must be satisfied for the chain to be considered converged.

.. important::

  Before the first iteration the chain is respaced by geomeTRIC (and aligned, if ``align`` is set),
  which may insert or remove images. The number of gradient calculations in an iteration therefore
  need not equal the number of molecules you submitted.

.. note::

  The ``driver`` of ``singlepoint_specification`` is ignored. The service overrides it with
  ``gradient`` for the chain calculations, and with ``hessian`` for the transition-state Hessian.
  As with :ref:`optimizations <optimization_specification>`, the convention is to set
  ``driver="deferred"``.

What ``optimization_specification`` is (and is not) used for
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``optimization_specification`` applies **only to the transition-state optimization**, and even then
the service adjusts it:

- ``program`` is forced to ``geometric``
- ``transition: True`` and the computed Hessian (``hess_data``) are added to its ``keywords``

The endpoint optimizations requested by ``optimize_endpoints`` do **not** use it. They always run
with ``geometric`` and ``coordsys: tric``, taking their level of theory from
``singlepoint_specification``. Similarly, when ``optimization_specification`` is omitted and
``optimize_ts`` is set, the transition-state optimization uses ``geometric`` with
``coordsys: tric`` and ``transition: True``, again at the level of theory of
``singlepoint_specification``.

One consequence is worth noting: when ``optimization_specification`` *is* given, the
transition-state Hessian is computed with that specification's ``qc_specification``, not with
``singlepoint_specification``. Keep the two at the same level of theory unless you specifically want
them to differ.

.. dropdown:: Basic NEBSpecification

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBSpecification, NEBKeywords
        from qcportal.singlepoint import QCSpecification

        neb_spec = NEBSpecification(
            program="geometric",
            singlepoint_specification=QCSpecification(
                program="psi4",
                driver="deferred",
                method="b3lyp",
                basis="def2-svp",
            ),
            keywords=NEBKeywords(),
        )

.. dropdown:: A coarser, cheaper chain

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBSpecification, NEBKeywords
        from qcportal.singlepoint import QCSpecification

        neb_spec = NEBSpecification(
            program="geometric",
            singlepoint_specification=QCSpecification(
                program="psi4", driver="deferred", method="hf", basis="sto-3g"
            ),
            keywords=NEBKeywords(
                images=7,
                maximum_cycle=50,
                maximum_force=0.1,
                average_force=0.05,
            ),
        )

.. dropdown:: Optimize the endpoints before starting

  Useful when the endpoints came from a scan, a docking program, or hand-built geometries, and are
  not themselves minima.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBSpecification, NEBKeywords
        from qcportal.singlepoint import QCSpecification

        neb_spec = NEBSpecification(
            program="geometric",
            singlepoint_specification=QCSpecification(
                program="psi4", driver="deferred", method="b3lyp", basis="def2-svp"
            ),
            keywords=NEBKeywords(optimize_endpoints=True),
        )

.. dropdown:: Refine the guessed transition state

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBSpecification, NEBKeywords
        from qcportal.singlepoint import QCSpecification

        neb_spec = NEBSpecification(
            program="geometric",
            singlepoint_specification=QCSpecification(
                program="psi4", driver="deferred", method="b3lyp", basis="def2-svp"
            ),
            keywords=NEBKeywords(optimize_ts=True),
        )

.. dropdown:: Control the transition-state optimization explicitly

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBSpecification, NEBKeywords
        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        qc_spec = QCSpecification(
            program="psi4", driver="deferred", method="b3lyp", basis="def2-svp"
        )

        neb_spec = NEBSpecification(
            program="geometric",
            singlepoint_specification=qc_spec,
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=qc_spec,
                keywords={"maxiter": 300},
            ),
            keywords=NEBKeywords(optimize_ts=True),
        )

.. dropdown:: A plain elastic band with a stiffer spring

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBSpecification, NEBKeywords
        from qcportal.singlepoint import QCSpecification

        neb_spec = NEBSpecification(
            program="geometric",
            singlepoint_specification=QCSpecification(
                program="psi4", driver="deferred", method="b3lyp", basis="def2-svp"
            ),
            keywords=NEBKeywords(
                spring_type=2,
                spring_constant=5.0,
            ),
        )


.. _neb_submission:

Submitting Records
------------------

NEB records are submitted with :meth:`~qcportal.client.PortalClient.add_nebs`.
This method takes the following information:

- ``initial_chains`` - The chains to run. Each NEB starts from a single chain (a list of molecules),
  so this is a **nested** list - one inner list per record
- ``program`` - The NEB program (use ``"geometric"``)
- ``singlepoint_specification`` - The level of theory for the gradient calculations
- ``optimization_specification`` - The transition-state optimization details, or ``None``
- ``keywords`` - A :class:`~qcportal.neb.record_models.NEBKeywords` object

``optimization_specification`` and ``keywords`` have no defaults, so both must be given;
pass ``None`` for the former if you do not need it.

The molecules in a chain may be :doc:`Molecule <../molecule>` objects or molecule IDs,
and the two may be mixed. The chain must be ordered from one endpoint to the other - the service
treats the first and last elements as the endpoints.

See :doc:`../record_submission` for more information about other arguments.


.. _neb_dataset:

NEB Datasets
------------

NEB :ref:`datasets <glossary_dataset>` are collections of NEB records.
An :class:`entry <qcportal.neb.dataset_models.NEBDatasetEntry>` holds one chain:

- ``name`` - The name of the entry
- ``initial_chain`` - The ordered list of molecules making up this chain
- ``additional_keywords`` - Per-entry NEB keywords, merged into the specification's
  :class:`~qcportal.neb.record_models.NEBKeywords`
- ``additional_singlepoint_keywords`` - Per-entry keywords, merged into the specification's
  ``singlepoint_specification.keywords``
- ``attributes`` - A user-defined dictionary of metadata for this entry
- ``comment`` - A user-supplied comment

The :class:`dataset specification <qcportal.neb.dataset_models.NEBDatasetSpecification>` holds the
:class:`NEBSpecification <qcportal.neb.record_models.NEBSpecification>` that every entry is computed
with.

.. _neb_dataset_keywords:

Per-entry keyword overrides
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a dataset is :ref:`submitted <dataset_submission>`, the server builds one NEB specification for
each (entry, specification) pair by taking the dataset specification and updating it from the entry:

.. table::

  ===================================================  ==============================================
   Specification field                                  Updated from the entry's
  ===================================================  ==============================================
   ``keywords``                                         ``additional_keywords``
   ``singlepoint_specification.keywords``               ``additional_singlepoint_keywords``
  ===================================================  ==============================================

The merge is a plain dictionary update, one level deep: a key present in the entry replaces that key
in the specification outright, and keys the entry does not mention are left alone. Nothing else in
the specification is affected - the programs, method, basis, and ``optimization_specification`` come
from the dataset specification alone and cannot be varied per entry.

The normal arrangement is to put everything in the dataset specification and leave the entry
keywords empty, reaching for ``additional_keywords`` only for the occasional chain that needs a
looser convergence criterion or a different number of images.

.. warning::

  ``additional_keywords`` is stored as an unvalidated dictionary on the entry. It is only checked
  against :class:`~qcportal.neb.record_models.NEBKeywords`, which forbids unknown fields, at the
  point where the merged specification is built - that is, on :ref:`submission <dataset_submission>`,
  not when the entry is added. A misspelled keyword is accepted without complaint and then fails the
  submission, and because the failure happens inside the server it comes back as an internal server
  error and an error ID rather than the name of the offending key. If a submission fails for no
  apparent reason, check your entries' keys against the field names of
  :class:`~qcportal.neb.record_models.NEBKeywords`.

  ``additional_singlepoint_keywords`` is not checked this way, since ``QCSpecification.keywords``
  is an open dictionary passed through to the QC program.

See :doc:`../datasets/index` for general dataset operations and advanced usage.


.. _neb_client_examples:

Client Examples
---------------

.. dropdown:: Obtain a single NEB record by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_nebs(123)

.. dropdown:: Obtain multiple NEB records by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_nebs([123, 456])

.. dropdown:: Obtain multiple NEB records by ID, ignoring missing records

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_nebs([123, 456, 789], missing_ok=True)

.. dropdown:: Fetch the chain singlepoints and optimizations up front

  A NEB record has a lot of children, so ``include=['**']`` can be very expensive. Ask only for
  what you need.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_nebs(123, include=['initial_chain', 'singlepoints'])

.. dropdown:: Query NEB records by QC method and basis

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_nebs(qc_method='b3lyp', qc_basis='def2-svp')
        for r in r_iter:
            print(r.id)

.. dropdown:: Query NEB records containing a particular molecule in their initial chain

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_nebs(molecule_id=8231)
        for r in r_iter:
            print(r.id)

.. dropdown:: Add a NEB record

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBKeywords
        from qcportal.singlepoint import QCSpecification

        # chain is an ordered list of molecules from reactant to product
        meta, ids = client.add_nebs(
            [chain],
            program='geometric',
            singlepoint_specification=QCSpecification(
                program='psi4', driver='deferred', method='b3lyp', basis='def2-svp'
            ),
            optimization_specification=None,
            keywords=NEBKeywords(images=11),
        )

.. dropdown:: Add a NEB record that also locates the transition state

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBKeywords
        from qcportal.singlepoint import QCSpecification

        meta, ids = client.add_nebs(
            [chain],
            program='geometric',
            singlepoint_specification=QCSpecification(
                program='psi4', driver='deferred', method='b3lyp', basis='def2-svp'
            ),
            optimization_specification=None,
            keywords=NEBKeywords(optimize_endpoints=True, optimize_ts=True),
        )

.. dropdown:: Follow the energy profile of the converged path

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> r = client.get_nebs(123)
        >>> for sp in r.final_chain:
        ...     print(sp.properties['return_energy'])
        -78.5873142
        -78.5841027
        -78.5766319
        ...

.. dropdown:: Watch the chain converge over the iterations

  ``singlepoints`` is keyed by iteration number, so the barrier height can be tracked as the
  calculation proceeds.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> r = client.get_nebs(123)
        >>> for iteration in sorted(r.singlepoints):
        ...     energies = [sp.properties['return_energy'] for sp in r.singlepoints[iteration]]
        ...     print(iteration, max(energies) - energies[0])
        1 0.0421783
        2 0.0398215
        3 0.0391044

.. dropdown:: Retrieve the guessed and optimized transition states

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        >>> r = client.get_nebs(123)

        >>> # The highest-energy image of the converged chain
        >>> guess = r.result

        >>> # The refined saddle point, if optimize_ts was set
        >>> if r.ts_optimization is not None:
        ...     ts = r.ts_optimization.final_molecule


.. _neb_dataset_examples:

Dataset Examples
----------------

See :doc:`../datasets/index` for more information and advanced usage.
See the :ref:`specification <neb_specification>` section for all the options in creating
specifications.

.. dropdown:: Create a NEB dataset with default options

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds = client.add_dataset(
                 "neb",
                 "Dataset Name",
                 "An example of a NEB dataset"
        )

.. dropdown:: Add a single entry to a NEB dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        # hcn_chain is an ordered list of molecules
        ds.add_entry("HCN isomerization", hcn_chain)

.. dropdown:: Add many entries to a NEB dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBDatasetNewEntry

        # Chains built beforehand, for example by interpolating between endpoints
        new_entries = [
            NEBDatasetNewEntry(name=name, initial_chain=chain)
            for name, chain in all_chains.items()
        ]

        # Efficiently add all entries in a single call
        ds.add_entries(new_entries)

.. dropdown:: Add a specification to a NEB dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.neb import NEBSpecification, NEBKeywords
        from qcportal.singlepoint import QCSpecification

        neb_spec = NEBSpecification(
            program="geometric",
            singlepoint_specification=QCSpecification(
                program="psi4",
                driver="deferred",
                method="b3lyp",
                basis="def2-svp",
            ),
            keywords=NEBKeywords(images=11, optimize_ts=True),
        )

        ds.add_specification("psi4/b3lyp/def2-svp", neb_spec)

.. dropdown:: Loosen the convergence criteria for one difficult chain

  Everything else about the entry is inherited from the dataset specification.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        # Uses the specification's keywords as-is
        ds.add_entry("HCN isomerization", hcn_chain)

        # Same level of theory, but fewer images and a looser force criterion
        ds.add_entry(
            "C4H3N2 isomerization",
            c4h3n2_chain,
            additional_keywords={"images": 7, "maximum_force": 0.1},
        )

.. dropdown:: Pass keywords through to the QC program for one entry

  ``additional_singlepoint_keywords`` is merged into the specification's
  ``singlepoint_specification.keywords`` the same way, which is useful when one chain has a molecule
  that converges badly.

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds.add_entry(
            "stubborn chain",
            stubborn_chain,
            additional_singlepoint_keywords={"maxiter": 500, "guess": "sad"},
        )


.. _neb_qcportal_api:

NEB QCPortal API
----------------

* :mod:`Record models <qcportal.neb.record_models>`
* :mod:`Dataset models <qcportal.neb.dataset_models>`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.add_nebs`
  * :meth:`~qcportal.client.PortalClient.get_nebs`
  * :meth:`~qcportal.client.PortalClient.query_nebs`
