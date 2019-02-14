Changelog
=========

.. Use headers commented below commented as templates

.. X.Y.0 / 2019-MM-DD
.. -------------------
..
.. New Features
.. ++++++++++++
..
.. Enhancements
.. ++++++++++++
..
.. Bug Fixes
.. +++++++++

0.5.0b1 / 2019-02-14
-------------------

New Features
++++++++++++
- (:pr:`114`) A new Collection: ``Generic``, has been added to allow semi-structured user defined data to be built without relying only on implemented collections.
- (:pr:`125`) QCElemental common PyDantic models have been integrated throughout the QCFractal code base, making a common model repository for the prevalent ``Molecule`` object come from a single source
  Also converted QCFractal to use pass serialized PyDantic objects between QCFractal and QCEngine to allow validation and (de)serialization of objects automatically.
- (:pr:`130`, :pr:`142`, and :pr:`145`) PyDantic serialization has been added to all REST calls leaving and entering both QCFractal Servers and QCFractal Portals. This allows automatic REST call validation and formatting on both server and client sides.
- (:pr:`141` and :pr:`152`) A new GridOptimization service has been added to QCFractal. This feature supports relative starting positions from the input molecule.

Enhancements
++++++++++++
- (:pr:`110`) QCFractal now depends on QCElemental and QCEngine to improve consistent imports
- (:pr:`116`) Queue Manger Adapters are now more generalized and inherit more from the base classes
- (:pr:`118`) Single and Optimization procedures have been streamlined to have simpler submission specifications and less redundancy
- (:pr:`133`) Fractal Server and Queue Manager startups are much more verbose and include version information.
- (:pr:`135`) The TorsionDriveService has a much more regular structure based on PyDantic models and a new TorsionDrive model has been created to enforce both validation and regularity
- (:pr:`143`) ``Task``s in the Mongo database can now be referenced by multiple ``Results`` and ``Procedures`` (i.e. a single ``Result`` or ``Procedure`` does not have ownership of a ``Task``)
- (:pr:`147`) Services submission has been overhauled such that all services submit to a single source. Right now only one service can be submitted at a time (future feature).
  TorsionDrive can now have multiple molecule inputs
- (:pr:`149`) Package import logic has been reworked to reduce the boot-up time of QCFractal from 3000ms at the worst to about 600ms
- (:pr:`150`) ``KeywordSet`` are now modeled much more consistently through PyDantic models and are consistently hashed to survive round trip serialization.
  ``KeywordSet`` is the new name for the old ``Options`` objects to better match their goal (See :pr:`155`)
- (:pr:`153`) Datasets now support option aliases which map to the consistent ``KeywordSet`` models from :pr:`150`.
- (:pr:`155`) Adding multiple ``Molecule`` or ``Result`` objects to the database at the same time now always return their Database ID's if added, and order of returned list of ID's matches input order.
  This PR also renamed ``Options`` to ``KeywordSet`` to properly reflect the goal of the object.
- (:pr:`156`) Memory and Number of Cores per Task can be specified when spinning up a Queue Manager and/or Queue Adapter objects.
  These settings are passed on to QCEngine. These must be hard-set by users.
- (:pr:`162`) Services can now be saved and fetched from the database through MongoEngine with document validation on both actions.

Bug Fixes
+++++++++
- (:pr:`132`) Fixed MongoEngine Socket bug where calling some functions before others resulted in a bug due to lack of initialized variables
- (:pr:`133`) Molecule objects cannot be oriented once they enter the QCFractal ecosystem. Molecules also cannot be oriented by programs invoked by the QCFractal ecosystem
- (:pr:`146`) CI environments have been simplified to make maintaining them easier and improve test coverage and find more bugs
- (:pr:`158`) Database addition documents in general will strip IDs from the input dictionary which caused issues from MongoEngine having a special treatment for the dictionary key "id".


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
