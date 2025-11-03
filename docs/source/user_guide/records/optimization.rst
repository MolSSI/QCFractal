Optimization calculations
=====================================

Geometry optimizations are multi-step procedures that iteratively minimize the energy of a molecule by
running a sequence of singlepoint calculations at different geometries. An optimization record stores
the initial and final (optimized) molecules, as well as these singlepoint calculations (trajectory)


.. _optimization_record:

Optimization Records
--------------------

Optimization records contain all the fields of a :doc:`base record <base>`, and additionally include:

- ``initial_molecule`` - The molecule used as the starting geometry
- ``final_molecule`` - The optimized molecule (may be ``None`` if the record is not complete)
- ``energies`` - A list of energies during the optimization trajectory
- ``trajectory`` - The sequence of :class:`~qcportal.singlepoint.record_models.SinglepointRecord` records
  representing the individual steps of the optimization trajectory
- ``specification`` - The program, level of theory, and other options for running the optimization

The trajectory can be accessed either as a list (:attr:`~qcportal.optimization.record_models.OptimizationRecord.trajectory`)
or via indexing using :meth:`~qcportal.optimization.record_models.OptimizationRecord.trajectory_element`.
The latter is more efficient if only one singlepoint record is needed as it will fetch only that record from the
server if it does not exist locally.


.. _optimization_specification:

Optimization Specification
--------------------------

The :ref:`glossary_specification` for an optimization is a
:class:`~qcportal.optimization.record_models.OptimizationSpecification`. The key fields are:

- ``program`` - The optimization program (for example, ``geometric``). This is not necessarily the same as the program
  used for the singlepoint calculations.
- ``qc_specification`` - The singlepoint QC details (method, basis, program, keywords). See :ref:`Singlepoint Specification <singlepoint_specification>`.
- ``keywords`` - Program-specific keywords for the optimization program
- ``protocols`` - Controls storage/return of additional information (see :class:`~qcportal.optimization.record_models.OptimizationProtocols`)

.. note::
   The QC singlepoint computations within an optimization always use a ``deferred`` driver. Any driver
   passed into the QCSpecification is overridden internally. The ``deferred`` value is a placeholder,
   as the true driver is controlled by the optimization program

.. note::
   Currently the ``protocols`` field of the QC singlepoint specification is ignored. This will be fixed in the future

:class:`~qcportal.optimization.record_models.OptimizationSpecification` objects can be created manually (for example,
when adding to datasets).

.. dropdown:: Basic OptimizationSpecification with a QCSpecification

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        # Use geometric for the optimization, psi4 for the individual gradient calculations
        opt_spec = OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="psi4",
                method="b3lyp",
                basis="def2-svp",
                driver="deferred",
            ),
        )

.. dropdown:: Pass optimization program-specific keywords

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        opt_spec = OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            keywords={
                # See your optimization program's docs (e.g., geometric) for available options
                "maxiter": 200,
            },
        )

.. dropdown:: Control protocols of what is stored/returned

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
        from qcportal.singlepoint import QCSpecification

        opt_spec = OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
            protocols=OptimizationProtocols(trajectory="initial_and_final"),
        )


.. _optimization_submission:

Submitting Records
------------------

Optimization records can be submitted using a client via the :meth:`~qcportal.client.PortalClient.add_optimizations` method.
This method takes the following information:

- ``initial_molecules`` - A single molecule or list of molecules to optimize
- ``program`` - The optimization program (e.g., ``geometric``)
- ``qc_specification`` - The QC details used for each step (see :ref:`above <optimization_specification>`)
- ``keywords`` - Program-specific keywords for the optimization
- ``protocols`` - Protocols for storing/returning data for the optimization

See :doc:`../record_submission` for more information about other arguments.


.. _optimization_dataset:

Optimization Datasets
---------------------

Optimization :ref:`datasets <glossary_dataset>` are collections of optimization records.
:class:`Entries <qcportal.optimization.dataset_models.OptimizationDatasetEntry>` contain a single initial molecule
and optional metadata.

The :class:`dataset specifications <qcportal.optimization.dataset_models.OptimizationDatasetSpecification>`
contain an :class:`OptimizationSpecification <qcportal.optimization.record_models.OptimizationSpecification>`.


.. _optimization_client_examples:

Client Examples
---------------

.. dropdown:: Obtain a single optimization record by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_optimizations(123)

.. dropdown:: Obtain multiple optimization records by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_optimizations([123, 456])

.. dropdown:: Obtain multiple optimizations by ID, ignoring missing records

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_optimizations([123, 456, 789], missing_ok=True)

.. dropdown:: Include trajectory and all data for a record during initial fetch

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_optimizations([123, 456], include=['**'])

.. dropdown:: Query optimizations by optimization program and QC method/basis

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_optimizations(program='geometric', qc_method='b3lyp', qc_basis='def2-svp')
        for r in r_iter:
            print(r.id)

.. dropdown:: Query optimizations by QC program and when the record was created, include trajectory

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_optimizations(qc_program='psi4',
                                            created_after='2024-03-21 12:34:56',
                                            include=['trajectory'])
        for r in r_iter:
            print(r.id, len(r.trajectory))

.. dropdown:: Add optimization records

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        meta, ids = c.add_optimizations([mol1, mol2],
                                        program='geometric',
                                        qc_specification=QCSpecification(program='psi4', method='b3lyp', basis='def2-svp', driver='deferred'))

.. dropdown:: Add optimization records, set optimization keywords

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        meta, ids = client.add_optimizations([mol1, mol2],
                                             program='geometric',
                                             qc_specification=QCSpecification(program='psi4', method='b3lyp', basis='def2-svp', driver='deferred'),
                                             keywords={'convergence_set': 'tight'})

.. dropdown:: Add optimization records, adjust protocols

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification
        from qcelemental.models.procedures import OptimizationProtocols

        meta, ids = client.add_optimizations([mol1, mol2],
                                             program='geometric',
                                             qc_specification=QCSpecification(program='psi4', method='b3lyp', basis='def2-svp', driver='deferred'),
                                             protocols=OptimizationProtocols(trajectory='initial_and_final'))


.. _optimization_dataset_examples:

Dataset Examples
----------------

See :doc:`../datasets/index` for more information and advanced usage.
See the :ref:`specification <optimization_specification>` section for all the options in creating specifications.

.. dropdown:: Create an optimization dataset with default options

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds = client.add_dataset(
                 "optimization",
                 "Optimization Dataset Name",
                 "An example of an optimization dataset"
        )

.. dropdown:: Add a single entry to an optimization dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.molecules import Molecule

        h2_mol = Molecule(symbols=['h', 'h'], geometry=[0, 0, 0, 0, 0, 1.5])
        ds.add_entry("hydrogen", h2_mol)

.. dropdown:: Add many entries to an optimization dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.optimization import OptimizationDatasetEntry
        from qcportal.molecules import Molecule

        new_entries = []
        for element in ['h', 'n', 'o']:
            mol = Molecule(symbols=[element], geometry=[0, 0, 0])
            ent = OptimizationDatasetEntry(name=f"{element}_atom", initial_molecule=mol)
            new_entries.append(ent)

        ds.add_entries(new_entries)

.. dropdown:: Add a specification to an optimization dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.optimization import OptimizationSpecification
        from qcportal.singlepoint import QCSpecification

        opt_spec = OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(program="psi4", method="b3lyp", basis="def2-svp", driver="deferred"),
        )

        ds.add_specification("geometric/psi4-b3lyp-def2-svp", opt_spec)


.. _optimization_qcportal_api:

Optimization QCPortal API
-------------------------

* :mod:`Record models <qcportal.optimization.record_models>`
* :mod:`Dataset models <qcportal.optimization.dataset_models>`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.add_optimizations`
  * :meth:`~qcportal.client.PortalClient.get_optimizations`
  * :meth:`~qcportal.client.PortalClient.query_optimizations`