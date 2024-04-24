Singlepoint calculations
=====================================

Singlepoints represent the core of quantum chemistry calculations. They are the simplest type of calculation
and are used to evaluate the energy and properties of a molecule at a given geometry.

.. _singlepoint_record:


Singlepoint Records
-------------------

Singlepoint records contain all the fields of a :doc:`base record <base>`, but also contain:

- ``molecule`` - The molecule that was used in the calculation
- ``return_result`` - The overall requested result of the calculation (energy, gradient, etc.)
- ``wavefunction`` - The final wavefunction (orbitals, density, etc)


Submitting Records
------------------

Singlepoint records can be submitted using a client via the :meth:`~qcportal.PortalClient.add_singlepoints` method.
This method takes the following information:

- ``molecules`` - A single molecule or list of molecules to compute
- ``program``, ``driver``, ``method``, ``basis``, ``keywords`` - The computational details of the calculation
- ``protocols`` - Additional flags for controlling the computation

See :doc:`../record_submission` for more information about other fields.

.. _singlepoint_protocols:

Protocols
~~~~~~~~~

Protocols control additional flags for the computation. For singlepoint calculations, this includes whether
to save the raw outputs, wavefunction, or native files.

See the :ref:`examples below <singlepoint_examples>` and the :class:`~qcportal.singlepoint.SinglepointProtocols` API
documentation for more information.


.. _singlepoint_dataset:

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


Singlepoint Datasets
--------------------

Singlepoint :doc:`datasets <../datasets>` are collections of singlepoint records.
:class:`Entries <qcportal.singlepoint.SinglepointDatasetEntry>` contain a single


.. _singlepoint_examples:


Dataset Examples
----------------



.. _singlepoint_qcportal_api:

Singlepoint QCPortal API
------------------------

PortalClient methods
~~~~~~~~~~~~~~~~~~~~

.. automethod:: qcportal.PortalClient.add_singlepoints

.. automethod:: qcportal.PortalClient.get_singlepoints

.. automethod:: qcportal.PortalClient.query_singlepoints


Singlepoint Classes
~~~~~~~~~~~~~~~~~~~

.. autoclass:: qcportal.singlepoint.SinglepointRecord

.. autoclass:: qcportal.singlepoint.QCSpecification

.. autoclass:: qcportal.singlepoint.SinglepointDriver
  :no-inherited-members:

.. autoclass:: qcportal.singlepoint.SinglepointProtocols

.. autoclass:: qcportal.singlepoint.SinglepointDataset

.. autoclass:: qcportal.singlepoint.SinglepointDatasetEntry
