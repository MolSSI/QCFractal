Base Record
===========

A *record* is an object that represents a single computation. Records can be simple,
such as a :doc:`singlepoint record <../records/singlepoint>` or complex with
lots of subrecords of different types (such as a :doc:`NEB record <neb>`.

However, all records share a set of common methods and properties.

Records can be :doc:`retrieved using the QCPortal client <../record_retrieval>`

Metadata
~~~~~~~~

ID, status, created_on, modified_on, comments, owner_user, owner_group, extras


Computation History
~~~~~~~~~~~~~~~~~~~

compute_history, manager_name, provenance, stdout, stderr, error, native_files


Task or Service
~~~~~~~~~~~~~~~

task, service, is_service

See: :doc:`../../overview/tasks_services`


Base Record API
================

.. autopydantic_model:: qcportal.record_models.BaseRecord

.. autopydantic_model:: qcportal.dataset_models.BaseDataset

.. autoclass:: qcportal.record_models.RecordStatusEnum

.. autoclass:: qcportal.record_models.PriorityEnum

.. autoclass:: qcportal.record_models.RecordQueryIterator
