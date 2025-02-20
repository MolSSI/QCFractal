Singlepoint calculations
=====================================

Singlepoints represent the core of quantum chemistry calculations. They are the simplest type of calculation
and are used to evaluate the energy and properties of a molecule at a particular fixed geometry.


.. _singlepoint_record:

Singlepoint Records
-------------------

Singlepoint records contain all the fields of a :doc:`base record <base>`, but also contain:

- ``molecule`` - The molecule that was used in the calculation
- ``return_result`` - The overall requested result of the calculation (energy, gradient, etc.)
- ``wavefunction`` - The final wavefunction (orbitals, density, etc)


.. _singlepoint_specification:

Singlepoint Specification (QCSpecification)
-------------------------------------------

The :ref:`glossary_specification` for a singlepoint record is
a :class:`~qcportal.singlepoint.record_models.QCSpecification`. The main fields this contains are:

- ``program`` - The program the record is to be run with (or was run with)
- ``driver`` - The main target of this calculation (see :class:`~qcportal.singlepoint.record_models.SinglepointDriver`)
- ``method`` - The quantum chemistry (or similar) method
- ``basis`` - Basis set to use. May be ``None`` or an empty string if the method does not use them (for example, classical or machine learning methods)
- ``keywords`` - Program-specific keywords
- ``protocols`` - Additional keywords controlling the return of information (see :class:`~qcportal.singlepoint.record_models.SinglepointProtocols`)

Protocols control additional flags for the computation. For singlepoint calculations, this includes whether
to save the raw outputs, wavefunction, or native files.

See the the :class:`~qcportal.singlepoint.SinglepointProtocols` API
documentation for more information.

:class:`~qcportal.singlepoint.record_models.QCSpecification` objects can be created manually (for example,
when adding to datasets).

.. dropdown:: Basic QCSpecification

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="psi4",
            driver="energy",
            method="b3lyp",
            basis="def2-svp",
        )

.. dropdown:: Request a gradient calculation

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="psi4",
            driver="gradient",
            method="b3lyp",
            basis="def2-svp",
        )


.. dropdown:: A method without a basis set

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="rdkit",
            driver="energy",
            method="uff",
            basis=None,
        )

.. dropdown:: Pass in program-specific keywords

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="psi4",
            driver="energy",
            method="b3lyp",
            basis="def2-svp",
            keywords={
                "guess": "sad",
                "maxiter": 1000,
                "mp2_type": "df",
                "scf_type": "df",
                "freeze_core": True,
                "d_convergence": 8,
                "e_convergence": 8
            }
        )

.. dropdown:: Request outputs (stdout) not be saved

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="psi4",
            driver="energy",
            method="b3lyp",
            basis="def2-svp",
            protocols={
                "stdout": False
            }
        )

.. dropdown:: Save full wavefunction objects

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="psi4",
            driver="energy",
            method="b3lyp",
            basis="def2-svp",
            protocols={
                "wavefunction": "all"
            }
        )

.. dropdown:: Save only orbitals and eigenvalues of the wavefunction, and various other files

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="psi4",
            driver="energy",
            method="b3lyp",
            basis="def2-svp",
            protocols={
                "wavefunction": "orbitals_and_eigenvalues",
                "native_files": "all"
            }
        )


.. _singlepoint_submission:

Submitting Records
------------------

Singlepoint records can be submitted using a client via the :meth:`~qcportal.client.PortalClient.add_singlepoints` method.
This method takes the following information:

- ``molecules`` - A single molecule or list of molecules to compute
- ``program``, ``driver``, ``method``, ``basis``, ``keywords``, ``protocols`` - The computational details of the calculation (see :ref:`above <singlepoint_specification>`)

See :doc:`../record_submission` for more information about other fields.


.. _singlepoint_dataset:

Singlepoint Datasets
--------------------

Singlepoint :ref:`datasets <glossary_dataset>` are collections of singlepoint records.
:class:`Entries <qcportal.singlepoint.dataset_models.SinglepointDatasetEntry>` contain a single molecule.
The :class:`dataset specifications <qcportal.singlepoint.dataset_models.SinglepointDatasetSpecification>`
contain a singlepoint specification (see :ref:`above <singlepoint_specification>`)

.. _singlepoint_dataset_add_entries_from:

Adding entries from other types of datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Singlepoint datasets contain a :meth:`~qcportal.singlepoint.dataset_models.SinglepointDataset.add_entries_from`
method which can be used to add entries from another singlepoint dataset or from an optimization dataset.

When copying from an optimization dataset, a specification must be given. The new entries
will have the same name and metadata as in the source dataset, however will contain the optimized
molecules from the records for the given specification. If a particular record is not complete,
the given entry will not be added.

If an entry with the same name already exists, it will be ignored.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> ds = client.add_dataset("singlepoint", "Dataset from optimization")
      >>> ds.add_entries_from(377, 'default') # from an optimization dataset
      InsertCountsMetadata(n_inserted=20, n_existing=0, error_description=None, errors=[])

      >>> print(ds.entry_names)
      ['000280960', '000524682', '010464300', ...


.. _singlepoint_client_examples:

Client Examples
---------------

.. dropdown:: Obtain a single record by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r = client.get_singlepoints(123)

.. dropdown:: Obtain multiple records by ID

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_singlepoints([123, 456])

.. dropdown:: Obtain multiple records by ID, ignoring missing records

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_singlepoints([123, 456, 789], missing_ok=True)

.. dropdown:: Include all data for a record during initial fetch

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_lst = client.get_singlepoints([123, 456], include=['**'])

.. dropdown:: Query singlepoints by program, method, basis

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_singlepoints(program='psi4', method='b3lyp', basis='def2-svp')
        for r in r_iter:
            print(r.id)

.. dropdown:: Query singlepoints by program and when the record was created, include all data

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        r_iter = client.query_singlepoints(program='psi4',
                                           created_after='2024-03-21 12:34:56',
                                           include=['**'])
                                           limit=50)
        for r in r_iter:
            print(r.id)

.. dropdown:: Add a singlepoint record

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        meta, ids = client.add_singlepoints([mol1, mol2],
                                            program='psi4',
                                            driver='energy',
                                            method='b3lyp',
                                            basis='def2-svp')

.. dropdown:: Add a singlepoint record, specify program-specific keywords

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        meta, ids = client.add_singlepoints([mol1, mol2],
                                            program='psi4',
                                            driver='energy',
                                            method='b3lyp',
                                            basis='def2-svp',
                                            keywords={'scf_type': 'df'}

.. dropdown:: Add a singlepoint record, don't store raw output

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        meta, ids = client.add_singlepoints([mol1, mol2],
                                            program='psi4',
                                            driver='energy',
                                            method='b3lyp',
                                            basis='def2-svp',
                                            protocols={'stdout': False})

.. dropdown:: Add a singlepoint record, store wavefunction and native files

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        meta, ids = client.add_singlepoints([mol1, mol2],
                                            program='psi4',
                                            driver='energy',
                                            method='b3lyp',
                                            basis='def2-svp',
                                            protocols={'wavefunction': 'all', 'native_files': 'all'})


.. _singlepoint_dataset_examples:

Dataset Examples
----------------

See :doc:`../datasets/index` for more information and advanced usage.
See the :ref:`specification <singlepoint_specification>` section for all the options in creating specifications.

.. dropdown:: Create a singlepoint dataset with default options

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        ds = client.add_dataset(
                 "singlepoint",
                 "Dataset Name",
                 "An example of a singlepoint dataset"
        )

.. dropdown:: Add a single entry to a singlepoint dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        h2_mol = Molecule(symbols=['h', 'h'], geometry=[0, 0, 0, 0, 0, 1.5])
        ds.add_entry("hydrogen", h2_mol)

.. dropdown:: Add many entries to a singlepoint dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        # Construct a list of entries to add somehow
        new_entries = []
        for element in ['h', 'n', 'o']:
            mol = Molecule(symbols=[element], geometry=[0, 0, 0])
            ent = SinglepointDatasetEntry(name=f"{element}_atom", molecule=mol)
            new_entries.append(ent)

        # Efficiently add all entries in a single call
        ds.add_entries(new_entries)

.. dropdown:: Add a specification to a singlepoint dataset

  .. tab-set::

    .. tab-item:: PYTHON

      .. code-block:: py3

        from qcportal.singlepoint import QCSpecification

        spec = QCSpecification(
            program="psi4",
            driver="energy",
            method="b3lyp",
            basis="def2-svp",
        )

        ds.add_specification("psi4/b3lyp/def2-svp", spec)


.. _singlepoint_api_links:

Singlepoint QCPortal API
------------------------

* :mod:`Record models <qcportal.singlepoint.record_models>`
* :mod:`Dataset models <qcportal.singlepoint.dataset_models>`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.add_singlepoints`
  * :meth:`~qcportal.client.PortalClient.get_singlepoints`
  * :meth:`~qcportal.client.PortalClient.query_singlepoints`