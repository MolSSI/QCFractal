Base Record
===========

A *record* is an object that represents a single computation. Records can be simple,
such as a :doc:`singlepoint record <../records/singlepoint>` or complex with
lots of child records of different types (such as an :doc:`NEB record <neb>`).

However, all records share a set of common methods and properties.

Records can be :doc:`submitted <../record_submission>`, :doc:`retrieved <../record_retrieval>`,
and :doc:`modified <../record_management>` using the QCPortal client.

.. tip::

  All records are `Pydantic <https://docs.pydantic.dev/latest/>`_ models, and so incorporates all the
  features from that package.

Metadata
~~~~~~~~

All records share a common set of metadata fields. These fields are not directly modifiable
by the user, but are managed by the server.

These fields are

- ``id`` - The identifier for the record. This integer is unique across all records on this server.
- ``status`` - The current status of the record. See :ref:`record_status` for a description of statuses.
- ``created_on`` - The date and time the record was created.
- ``modified_on`` - The date and time the record was last modified.
- ``owner_user`` - The user that owns the record.
- ``owner_user`` - The group (that the user belongs to) that owns the record.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(1)
      >>> print(r.status)
      RecordStatusEnum.complete

      >>> print(r.created_on)
      2024-04-19 15:14:23.017093+00:00


Computation History and Provenance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All records also store provenance and computation history information for records that have ben computed. Each record
may be computed multiple times due to errors. In general, there should only be one history entry that is successful.

- ``compute_history`` - A list of compute history information. Each entry contains:

  - ``status`` - The resulting status of this computation. See :ref:`record_status` for a description of statuses.
  - ``manager_name`` - The name of the manager that ran thee computation (for :ref:`task <tasks>`-based records)
  - ``modified_on`` - The date and time this history entry was created/modified
  - ``provenance`` - The provenance information for the record. This is a :class:`~qcportal.record_models.Provenance` object that contains information about the software and versions used to compute the record.
  - ``stdout`` - The standard output of the computation
  - ``stderr`` - Any error information that was printed to stderr
  - ``error`` - Any error information that was raised during the computation

The record itself also contains the ``manager_name`` field, which represents the ``manager_name`` from the last
computation.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(1)
      >>> print(r.manager_name)
      snowflake_compute-abc-df04e41a-5973-4a4d-83f0-1b4b41b9fbe0

      >>> print(len(r.compute_history))
      2

      >>> print(r.compute_history[0].status)
      RecordStatusEnum.error

      >>> print(r.compute_history[0].provenance)
      Provenance(creator='Psi4', version='1.9.1'...

      >>> print(r.compute_history[0].modified_on)
      2024-04-19 15:14:31.128937+00:00

      >>> print(r.compute_history[1].status)
      RecordStatusEnum.complete

      >>> print(r.compute_history[1].modified_on)
      2024-04-19 15:14:35.1281723+00:00

      >>> print(r.compute_history[1].provenance.walltime)
      2.1928390123

      >>> print(r.compute_history[1].stdout)
      -----------------------------------------------------------------------
            Psi4: An Open-Source Ab Initio Electronic Structure Package
                                 Psi4 1.9.1 release

                           Git: Rev {} zzzzzzz


      D. G. A. Smith, L. A. Burns, A. C. Simmonett, R. M. Parrish,
      ...


A record also has top-level fields for some of this information. For these, the record will automatically
use the latest entry in the ``compute_history`` list.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(1)
      >>> print(r.provenance.walltime)
      2.1928390123

      >>> print(r.stdout)
      -----------------------------------------------------------------------
            Psi4: An Open-Source Ab Initio Electronic Structure Package
                                 Psi4 1.9.1 release

                           Git: Rev {} zzzzzzz


      D. G. A. Smith, L. A. Burns, A. C. Simmonett, R. M. Parrish,
      ...


Errors
~~~~~~~~~~~~~~~~~~~

The ``error`` field contains information about an error that occurred. This is usually populated if the
status of the record is ``error``. Like ``stdout`` and ``stderr``, errors are attached to compute history entries,
where the ``error`` field of the latest entry is accessible from the top-level record.

This field is a dictionary with the following keys:

- ``error_type`` - The type or category of the error
- ``error_message`` - A human-readable error message

It is generally useful to just print the error message. The ``stdout`` may or may not contain any information.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(1)
      >>> print(r.status)
      RecordStatusEnum.error

      >>> print(r.error["error_message"])
      QCEngine Unknown Error:
          -----------------------------------------------------------------------
                Psi4: An Open-Source Ab Initio Electronic Structure Package
                                     Psi4 1.8.2 release

                               Git: Rev {} zzzzzzz


          D. G. A. Smith, L. A. Burns, A. C. Simmonett, R. M. Parrish,
          M. C. Schieber, R. Galvelis, P. Kraus, H. Kruse, R. Di Remigio,

      ...

      File "/home/users/qcfuser/miniconda3/envs/qcfractal-worker-psi4-18.1/lib/python3.10/site-packages/psi4/driver/procrouting/scf_proc/scf_iterator.py", line 85, in scf_compute_energy
        self.iterations()
      File "/home/users/qcfuser/miniconda3/envs/qcfractal-worker-psi4-18.1/lib/python3.10/site-packages/psi4/driver/procrouting/scf_proc/scf_iterator.py", line 526, in scf_iterate
        raise SCFConvergenceError("""SCF iterations""", self.iteration_, self, Ediff, Dnorm)
      psi4.driver.p4util.exceptions.SCFConvergenceError: Could not converge SCF iterations in 200 iterations.


Other outputs
~~~~~~~~~~~~~
Calculations may also produce other outputs. All records support the following:

- ``properties`` - A dictionary of properties that were computed by the calculation
- ``extras`` - A dictionary of any miscellaneous information that was produced by the calculation

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(1)
      >>> print(r.properties.keys())
      dict_keys(['pe energy', 'scf dipole', 'calcinfo_nmo',...

      >>> print(r.properties["scf_dipole"])
      [0.5734967483313045, 0.5734967483328919, 0.0]


.. note::

      The ``native_files``, ``properties``, and ``extras`` fields are not standardized and may vary between different
      types of records. The contents of these fields are generally specific to the software that was used to compute the record.
      In addition, whether these fields are populated may depend on the settings used to compute the record. See, for example,
      the :ref:`singlepoint protocols <singlepoint_specification>`.

Native Files
~~~~~~~~~~~~~~~~~~~

Records contain a dictionary of "native" or raw files that were produced by the calculation.
This is a dictionary of file names and their contents, which can be written to disk or otherwise manipulated
in python. See :class:`~qcportal.record_models.NativeFile`.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(1)
      >>> print(r.native_files.keys())
      dict_keys(['output.dat', 'input.dat'])

      >>> print(r.native_files["input.dat"])
      Input file contents

      >>> r.native_files["input.dat"].save_file("./", new_name="input_copy.dat")


Comments
~~~~~~~~~~~~~~~~~~~

Users may attach comments to a record. These are arbitrary strings.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.add_comment([1], "My very first computation")

      >>> r = client.get_records(1)
      >>> print(r.comments)
      [RecordComment(id=1, record_id=1, username='ben',...

      >>> print(r.comments[0].comment)
      My very first computation

      >>> print(r.comments[0].username)
      ben

      >>> print(r.comments[0].timestamp)
      2024-04-21 14:42:31.266329+00:00

Task or Service
~~~~~~~~~~~~~~~

Records contain fields the represent their task or service. In general, these do not need to be accessed but
can be helpful for debugging purposes.

See: :doc:`../../overview/tasks_services`

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> r = client.get_records(1)
      >>> print(r.is_service)
      False

      >>> print(r.task)
      RecordTask(id=13, ...)

      >>> print(r.task.tag)
      tag1

      >>> print(r.task.priority)
      PriorityEnum.normal

      >>> print(r.task.function)
      qcengine.compute

      >>> r = client.get_records(2)
      >>> print(r.is_service)
      True

      >>> print(r.service.tag)
      tag1

      >>> print(r.service.priority)
      PriorityEnum.high

      >>> print(r.service.dependencies)
      [ServiceDependency(record_id=127494009, extras=...


Base Record API
---------------

* :doc:`../qcportal_reference/records/base_record_models`
