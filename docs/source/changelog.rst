Changelog
=========

.. X.Y.0 / 2019-MM-DD
.. -------------------
..
.. New Features
.. ++++++++++++

.. Enhancements
.. ++++++++++++
..
.. Bug Fixes
.. +++++++++

0.5.0a / 2019-01-29
-------------------

This is the fifth alpha release for QCFractal, refactoring adapters, queue managers
and adding in some of the QCElemental Pydantic primitives.

New Features
++++++++++++
- (:pr:`114`) A ``Generic`` Collection object added for developers to template from, and still have an object be
  serializable through QCFractal.
- (:pr:`125`) The migration to QCElemental's Pydantic models begins by replacing ``Molecule`` class. This continues
  the unification of data objects over QCArchive project by ensuring data models are all pulled from a common source.
  This change should reduce maintenance burden and proliferation data models in the future.


Enhancements
++++++++++++
- (:pr:`110`) CI tools cleaned up a bit to be more consistent across platforms.
- (:pr:`116`) Parsl Queue manger added to the available managers.
- (:pr:`120`) Queue adapters now have a base class which all others derive from. Execution through Python Executor will
  be attempted in most cases to handle parallel task launches. CLI has been refactored to reflect the generic
  adapter changes.

Bug Fixes
+++++++++
- (:pr:`123`) Queue Manager testing now more robust to catch programming errors earlier

0.4.0a / 2019-01-15
-------------------

This is the fourth alpha release of QCFractal focusing on the database backend
and compute manager enhancements.

New Features
++++++++++++
- (:pr:`78`) Migrates Mongo backend to MongoEngine.
- (:pr:`78`) Overhauls tasks so that results or procedures own a task and ID.
- (:pr:`78`) Results and procedures are now inserted upon creation, not just completion. Added a status field to results and procedures.
- (:pr:`78`) Overhauls storage API to no longer accept arbitrary JSON queries, but now pinned kwargs.
- (:pr:`106`) Compute managers now have heartbeats and tasks are recycled after a manager has not been heard from after a preset interval.
- (:pr:`106`) Managers now also quietly shutdown on SIGTERM as well as SIGINT.

Bug Fixes
+++++++++
- (:pr:`102`) Py37 fix for pydantic and better None defaults for ``options``.
- (:pr:`107`) ``FractalClient.get_collections`` now raises an exception when no collection is found.


0.3.0a / 2018-11-02
-------------------

This is the third alpha release of QCFractal focusing on a command line
interface and the ability to have multiple queues interacting with a central
server.

New Features
++++++++++++
- (:pr:`72`) Queues are no longer required of FractalServer instances, now separate QueueManager instances can be created that push and pull tasks to the server.
- (:pr:`80`) A `Parsl <http://parsl-project.org>`_ queue manager was written.
- (:pr:`75`) CLI's have been added for the `qcfractal-server` and `qcfractal-manager` instances.
- (:pr:`83`) The status of server tasks and services can now be queried from a FractalClient.
- (:pr:`82`) OpenFF Workflows can now add single optimizations for fragments.

Enhancements
++++++++++++

- (:pr:`74`) The documentation now has flowcharts showing task and service pathways through the code.
- (:pr:`73`) Collection `.data` attributes are now typed and validated with pydantic.
- (:pr:`85`) The CLI has been enhanced to cover additional features such as `queue-manager` ping time.
- (:pr:`84`) QCEngine 0.4.0 and geomeTRIC 0.9.1 versions are now compatible with QCFractal.


Bug Fixes
+++++++++

- (:pr:`92`) Fixes an error with query OpenFFWorkflows.

0.2.0a / 2018-10-02
-------------------

This is the second alpha release of QCFractal containing architectural changes
to the relational pieces of the database. Base functionality has been expanded
to generalize the collection idea with BioFragment and OpenFFWorkflow
collections.

Documentation
+++++++++++++
- (:pr:`58`) A overview of the QCArchive project was added to demonstrate how all modules connect together.

New Features
++++++++++++
- (:pr:`57`) OpenFFWorkflow and BioFragment collections to support OpenFF uses cases.
- (:pr:`57`) Requested compute will now return the id of the new submissions or the id of the completed results if duplicates are submitted.
- (:pr:`67`) The OpenFFWorkflow collection now supports querying of individual geometry optimization trajectories and associated data for each torsiondrive.

Enhancements
++++++++++++
- (:pr:`43`) Services and Procedures now exist in the same unified table when complete as a single procedure can be completed in either capacity.
- (:pr:`44`) The backend database was renamed to storage to prevent misunderstanding of the Database collection.
- (:pr:`47`) Tests can that require an activate Mongo instance are now correctly skipped.
- (:pr:`51`) The queue now uses a fast hash index to determine uniqueness and prevent duplicate tasks.
- (:pr:`52`) QCFractal examples are now tested via CI.
- (:pr:`53`) The MongoSocket `get_generic_by_id` was deprecated in favor of `get_generic` where an ID can be a search field.
- (:pr:`61`, :pr:`64`) TorsionDrive now tracks tasks via ID rather than hash to ensure integrity.
- (:pr:`63`) The Database collection was renamed Dataset to more correctly illuminate its purpose.
- (:pr:`65`) Collection can now be aquired directly from a client via the `client.get_collection` function.

Bug Fixes
+++++++++
- (:pr:`52`) The molecular comparison technology would occasionally incorrectly orientate molecules.


0.1.0a / 2018-09-04
-------------------

This is the first alpha release of QCFractal containing the primary structure
of the project and base functionality.

New Features
++++++++++++

- (:pr:`41`) Molecules can now be queried by molecule formula
- (:pr:`39`) The server can now use SSL protection and auto-generates SSL certificates if no certificates are provided.
- (:pr:`31`) Adds authentication to the FractalServer instance.
- (:pr:`26`) Adds TorsionDrive (formally Crank) as the first service.
- (:pr:`26`) Adds a "services" feature which can create large-scale iterative workflows.
- (:pr:`21`) QCFractal now maintains its own internal queue and uses queuing services such as Fireworks or Dask only for the currently running tasks

Enhancements
++++++++++++


- (:pr:`40`) Examples can now be testing through PyTest.
- (:pr:`38`) First major documentation pass.
- (:pr:`37`) Canonicalizes string formatting to the ``"{}".format`` usage.
- (:pr:`36`) Fireworks workflows are now cleared once complete to keep the active entries small.
- (:pr:`35`) The "database" table can now be updated so that database entries can now evolve over time.
- (:pr:`32`) TorsionDrive services now track all computations that are completed rather than just the last iteration.
- (:pr:`30`) Creates a Slack Community and auto-invite badge on the main readme.
- (:pr:`24`) Remove conda-forge from conda-envs so that more base libraries can be used.

Bug Fixes
+++++++++

- Innumerable bug fixes and improvements in this alpha release.
