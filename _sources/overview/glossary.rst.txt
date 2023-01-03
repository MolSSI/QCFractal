Glossary of Common Concepts
===========================

.. _glossary_record:

record
--------------------------

An object that represents a single computation. Records can be simple (a singlepoint computation), or can
contain a complex :ref:`glossary_service`, which may contain many child records.
The record stores the input information and, if the record has been successfully computed,
the results of the computation.

.. _glossary_molecule:

molecule
--------------------------

An object containing symbols and geometry of atoms. It may also contain information such as bonding
and identifiers. Molecules in this way are defined to be a specific geometry.

See :doc:`../user_guide/molecule`


.. _glossary_specification:

specification
--------------------------

A *specification* details how a computation should be run. For example, an
:class:`~qcportal.optimization.OptimizationSpecification` contains
information about the program used to run the optimization, which method or basis, and other
input information.

A specification applied to an input molecule results in a :ref:`glossary_record`.


.. _glossary_task:

task
--------------------------

A *task* is a self-contained object that represents a computation to be run. *Tasks* are mostly for internal use
by the server and managers, although they can be retrieved for help in debugging problems.

Every task is associated with a :ref:`glossary_record`, although not every record has a task. Tasks that are
completed are typically deleted from the server, with the data being copied to the record.
See :doc:`tasks_services`.


.. _glossary_service:

service
--------------------------

A hard-coded workflow that run on the server. The workflow is responsible for creating new
:ref:`records <glossary_record>`, and then iterating when all those records have been successfully
computed. See :doc:`tasks_services`.


.. _glossary_internal_job:

internal job
-------------

An *internal job* is a specific action or piece of work that is to be run
on the server. This is in contrast to a :ref:`glossary_task` which are to be run on
distributed :ref:`managers <glossary_manager>`.

See :doc:`internal_jobs`



.. _glossary_manager:

compute manager
---------------

A *compute manager* (or just *manager*) is a process that requests :ref:`tasks <glossary_task>` from the
server and then sends them to be computed. Simple managers may just compute them by itself, although
for production-level infrastructure these tasks are then queued to be run elsewhere (on an HPC cluster, for example).
See :doc:`Compute Managers <../admin_guide/managers/index>` in the user guide.


.. _glossary_tag:

routing tag
-----------

A *routing tag* (or just *tag*) is a user-specified string to assist in the routing of :ref:`tasks <glossary_task>`.
:ref:`Managers <glossary_manager>` can be set up to only requests tasks that are
assigned a specific tag. This can assist with directing certain tasks to special hardware, for example.
See :ref:`routing_tags`.


.. _glossary_dataset:

dataset
--------------------------

A *dataset* is a collection of similar :ref:`records <glossary_record>`.
A dataset contains :ref:`entries <glossary_dataset_entry>` which typically correspond to input
:ref:`molecules <glossary_molecule>`, and :ref:`specifications <glossary_dataset_specification>` that
define how a computation is to be run.

See :doc:`../user_guide/datasets`


.. _glossary_dataset_entry:

dataset entry
--------------------------

An object that represents a :ref:`glossary_molecule` or similar input in a dataset.
The :ref:`specifications <glossary_dataset_specification>` of the dataset are then applied to the
entries.

See :doc:`../user_guide/datasets`


.. _glossary_dataset_specification:

dataset specification
--------------------------

An object that represents a :ref:`glossary_specification` in a dataset. Typically these are just
:ref:`specifications <glossary_specification>` with an attached name, making it easier to
organize a dataset.

Specifications are then applied to :ref:`entries <glossary_dataset_entry>` to
form :ref:`records <glossary_record>` that are associated with the dataset.

See :doc:`../user_guide/datasets`
