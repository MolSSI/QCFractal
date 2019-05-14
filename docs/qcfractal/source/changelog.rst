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

0.6.1 / 2019-??-??
------------------

Enhancements
++++++++++++

- (:pr:`??`) Tasks which fail are now more verbose in the log as to why they failed. This is additional information
  on top of the number of pass/fail.
- (:pr:`255`) OpenFF conda environment now installs ``dask`` and ``dask-jobqueue`` by default


0.6.0 / 2019-03-30
------------------

Enhancements
++++++++++++

- (:pr:`236` and :pr:`237`) A large number of docstrings have been improved to be both more uniform,
  complete, and correct.
- (:pr:`239`) DFT-D3 can now be queried through the ``Dataset`` and ``ReactionDataset``.
- (:pr:`239`) ``list_collections`` now returns Pandas Dataframes.


0.5.5 / 2019-03-26
------------------

New Features
++++++++++++

- (:pr:`228`) ReactionDatasets visualization statistics plots can now be generated through Plotly! This feature includes
  bar plots and violin plots and is designed for interactive use through websites, Jupyter notebooks, and more.
- (:pr:`233`) TorsionDrive Datasets have custom visualization statistics through Plotly! This allows plotting 1-D
  torsion scans against other ones.

Enhancements
++++++++++++

- (:pr:`226`) LSF can now be specified for the Queue Managers for Dask managers.
- (:pr:`228`) Plotly is an optional dependency overall, it is not required to run QCFractal or QCPortal but will be
  downloaded in some situations. If you don't have Plotly installed, more graceful errors beyond just raw
  ``ImportErrors`` are given.
- (:pr:`234`) Queue Managers now report the number of passed and failed jobs they return to the server and can also
  have verbose (debug level) outputs to the log.
- (:pr:`234`) Dask-driven queue managers can now be set to simply scale up to a fixed number of workers instead of
  trying to adapt the number of workers on the fly.

Bug Fixes
+++++++++

- (:pr:`227`) SGE Clusters specified in Queue Manager under Dask correctly process ``job_extra`` for additional
  scheduler headers. This is implemented in a stable way such that if the upstream Dask Jobqueue implements a fix, the
  manager will keep working without needing to get a new release.
- (:pr:`234`) Fireworks managers now return the same pydantic models as every other manager instead of raw dictionaries.


0.5.4 / 2019-03-21
------------------

New Features
++++++++++++

- (:pr:`216`) Jobs submitted to the queue can now be assigned a priority to be served out to the managers.
- (:pr:`219`) Temporary, pop-up, local instances of ``FractalServer`` can now be created through the
  ``FractalSnowflake``. This creates an instance of ``FractalServer``, with its database structure, which is entirely
  held in temporary storage and memory, all of which is deleted upon exit/stop. This feature is designed for those
  who want to tinker with Fractal without needed to create their own database or connect to a production
  ``FractalServer``.
- (:pr:`220`) Queue managers can now set the ``scratch_directory`` variable that is passed to QCEngine and its workers.

Enhancements
++++++++++++

- (:pr:`216`) Queue managers now report what programs and procedures they have access to and will only pull jobs they
  think they can execute.
- (:pr:`222`) All of ``FractalClient``'s methods now have full docstrings and type annotations for clairy
- (:pr:`222`) Massive overhaul to the REST interface to simplify internal calls from the client and server side.
- (:pr:`223`) ``TorsionDriveDataset`` objects are modeled through pydantic objects to allow easier interface with the
  database back end and data validation.

Bug Fixes
+++++++++

- (:pr:`215`) Dask Jobqueue for the ``qcfractal-manager`` is now tested and working. This resolve the outstanding issue
  introduced in :pr:`211` and pushed in v0.5.3.
- (:pr:`216`) Tasks are now stored as ``TaskRecord`` pydantic objects which now preempts a bug introduced
  from providing the wrong schema.
- (:pr:`217`) Standalone QCPortal installs now report the correct version
- (:pr:`221`) Fixed a bug in ``ReactionDataset.query`` where passing in ``None`` was treated as a string.


0.5.3 / 2019-03-13
------------------

New Features
++++++++++++

- (:pr:`207`) All compute operations can now be augmented with a ``tag`` which can be later consumed by different
  ``QueueManager``s to only carry out computations with specified tags.
- (:pr:`210`) Passwords in the database can now be generated for new users and user information can be updated (server-side only)
- (:pr:`210`) ``Collections`` can now be updated automatically from the defaults
- (:pr:`211`) The ``qcfractal-manager`` CLI command now accepts a config file for more complex managers through Dask JobQueue.
  As such, many of the command line flags have been altered and can be used to either spin up a PoolExecutor, or overwrite the
  config file on-the-fly. As of this PR, the Dask Jobqueue component has been untested. Future updates will indicate
  when this has been tested.


Enhancements
++++++++++++

- (:pr:`203`) ``FractalClient``'s ``get_X`` methods have been renamed to ``query_X`` to better reflect what they actually do.
  An exception to this is the ``get_collections`` method which is still a true ``get``.
- (:pr:`207`) ``FractalClient.list_collections`` now respects show case sensitive results and queries are case
  insensitive
- (:pr:`207`) ``FractalServer`` can now compress responses to reduce the amount of data transmitted over the serialization.
  The main benefactor here is the ``OpenFFWorkflow`` collection which has significant transfer speed improvements due to compression.
- (:pr:`207`) The ``OpenFFWorkflow`` collection now has better validation on input and output data.
- (:pr:`210`) The ``OpenFFWorkflow`` collection only stores database ``id`` to reduce duplication and data transfer quantities.
  This results in about a 50x duplication reduction.
- (:pr:`211`) The ``qcfractal-template`` command now has fields for Fractal username and password.
- (:pr:`212`) The docs for QCFractal and QCPortal have been split into separate structures. They will be hosted on
  separate (although linked) pages, but their content will all be kept in the QCFractal source code. QCPortal's docs
  are for most users whereas QCFractal docs will be for those creating their own managers, Fractal instances, and
  developers.

Bug Fixes
+++++++++

- (:pr:`207`) ``FractalClient.get_collections`` is now correctly case insensitive.
- (:pr:`210`) Fixed a bug in the ``iterate`` method of services which returned the wrong status if everything completed right away.
- (:pr:`210`) The ``repr`` of the MongoEngine Socket now displays correctly instead of crashing the socket due to missing attribute


0.5.2 / 2019-03-08
------------------

New Features
++++++++++++

- (:pr:`197`) New ``FractalClient`` instances will automatically connect to the central MolSSI Fractal Server

Enhancements
++++++++++++

- (:pr:`195`) Read-only access has been granted to many objects separate from their write access.
  This is in contrast to the previous model where either there was no access security, or
  everything was access secure.
- (:pr:`197`) Unknown stoichiometry are no longer allowed in the ``ReactionDataset``
- (:pr:`197`) CLI for FractalServer uses Executor only to encourage using the
  Template Generator introduced in :pr:`177`.
- (:pr:`197`) ``Dataset`` objects can now query keywords from aliases as well.


Bug Fixes
+++++++++

- (:pr:`195`) Manager cannot pull too many tasks and potentially loose data due to query limits.
- (:pr:`195`) ``Records`` now correctly adds Provenance information
- (:pr:`196`) ``compute_torsion`` example update to reflect API changes
- (:pr:`197`) Fixed an issue where CLI input flags were not correctly overwriting default values
- (:pr:`197`) Fixed an issue where ``Collections`` were not correctly updating when the ``save`` function was called
  on existing objects in the database.
- (:pr:`197`) ``_qcfractal_tags`` are no longer carried through the ``Records`` objects in errant.
- (:pr:`197`) Stoichiometry information is no longer accepted in the ``Dataset`` object since this is not
  used in this class of object anymore (see ``ReactionDataset``).


0.5.1 / 2019-03-04
------------------

New Features
++++++++++++
- (:pr:`177`) Adds a new ``qcfractal-template`` command to generate ``qcfractal-manager`` scripts.
- (:pr:`181`) Pagination is added to queries, defaults to 1000 matches.
- (:pr:`185`) Begins setup documentation.
- (:pr:`186`) Begins database design documentation.
- (:pr:`187`) Results add/update is now simplified to always store entire objects rather than update partials.
- (:pr:`189`) All database compute records now go through a single ``BaseRecord`` class that validates and hashes the objects.

Enhancements
++++++++++++

- (:pr:`175`) Refactors query massaging logic to a single function, ensures all program queries are lowercase, etc.
- (:pr:`175`) Keywords are now lazy reference fields.
- (:pr:`182`) Reworks models to have strict fields, and centralizes object hashing with many tests.
- (:pr:`183`) Centralizes duplicate checking so that accidental mixed case duplicate results could go through.
- (:pr:`190`) Adds QCArchive sphinx theme to the documentation.

Bug Fixes
+++++++++

- (:pr:`176`) Benchmarks folder no longer shipped with package


0.5.0 / 2019-02-20
------------------

New Features
++++++++++++

- (:pr:`165`) Separates datasets into a Dataset, ReactionDataset, and OptimizationDataset for future flexability.
- (:pr:`168`) Services now save their Procedure stubs automatically, the same as normal Procedures.
- (:pr:`169`) ``setup.py`` now uses the README.md and conveys Markdown to PyPI.
- (:pr:`171`) Molecule addition now takes in a flat list and returns a flat list of IDs rather than using a dictionary.
- (:pr:`173`) Services now return their correspond Procedure ID fields.


Enhancements
++++++++++++

- (:pr:`163`) Ignores pre-existing IDs during storage add operations.
- (:pr:`167`) Allows empty queries to successfully return all results rather than all data in a collection.
- (:pr:`172`) Bumps pydantic version to 0.20 and updates API.

Bug Fixes
+++++++++

- (:pr:`170`) Switches Parsl from IPPExecutor to ThreadExecutor to prevent some bad semaphore conflicts with PyTest.

0.5.0rc1 / 2019-02-15
---------------------

New Features
++++++++++++
- (:pr:`114`) A new Collection: ``Generic``, has been added to allow semi-structured user defined data to be built without relying only on implemented collections.
- (:pr:`125`) QCElemental common pydantic models have been integrated throughout the QCFractal code base, making a common model repository for the prevalent ``Molecule`` object (and others) come from a single source.
  Also converted QCFractal to pass serialized pydantic objects between QCFractal and QCEngine to allow validation and (de)serialization of objects automatically.
- (:pr:`130`, :pr:`142`, and :pr:`145`) Pydantic serialization has been added to all REST calls leaving and entering both QCFractal Servers and QCFractal Portals. This allows automatic REST call validation and formatting on both server and client sides.
- (:pr:`141` and :pr:`152`) A new GridOptimizationRecord service has been added to QCFractal. This feature supports relative starting positions from the input molecule.

Enhancements
++++++++++++

General note: ``Options`` objects have been renamed to ``KeywordSet`` to better match their goal (See :pr:`155`.)

- (:pr:`110`) QCFractal now depends on QCElemental and QCEngine to improve consistent imports.
- (:pr:`116`) Queue Manger Adapters are now more generalized and inherit more from the base classes.
- (:pr:`118`) Single and Optimization procedures have been streamlined to have simpler submission specifications and less redundancy.
- (:pr:`133`) Fractal Server and Queue Manager startups are much more verbose and include version information.
- (:pr:`135`) The TorsionDriveService has a much more regular structure based on pydantic models and a new TorsionDrive model has been created to enforce both validation and regularity.
- (:pr:`143`) ``Task``s in the Mongo database can now be referenced by multiple ``Results`` and ``Procedures`` (i.e. a single ``Result`` or ``Procedure`` does not have ownership of a ``Task``.)
- (:pr:`147`) Service submission has been overhauled such that all services submit to a single source. Right now, only one service can be submitted at a time (to be expanded in a future feature.)
  TorsionDrive can now have multiple molecule inputs.
- (:pr:`149`) Package import logic has been reworked to reduce the boot-up time of QCFractal from 3000ms at the worst to about 600ms.
- (:pr:`150`) ``KeywordSet``s are now modeled much more consistently through pydantic models and are consistently hashed to survive round trip serialization.
- (:pr:`153`) Datasets now support option aliases which map to the consistent ``KeywordSet`` models from :pr:`150`.
- (:pr:`155`) Adding multiple ``Molecule`` or ``Result`` objects to the database at the same time now always return their Database ID's if added, and order of returned list of ID's matches input order.
  This PR also renamed ``Options`` to ``KeywordSet`` to properly reflect the goal of the object.
- (:pr:`156`) Memory and Number of Cores per Task can be specified when spinning up a Queue Manager and/or Queue Adapter objects.
  These settings are passed on to QCEngine. These must be hard-set by users and no environment inspection is done. Users may continue to choose
  not to set these and QCEngine will consume everything it can when it lands on a compute.
- (:pr:`162`) Services can now be saved and fetched from the database through MongoEngine with document validation on both actions.

Bug Fixes
+++++++++

- (:pr:`132`) Fixed MongoEngine Socket bug where calling some functions before others resulted in an error due to lack of initialized variables.
- (:pr:`133`) ``Molecule`` objects cannot be oriented once they enter the QCFractal ecosystem (after optional initial orientation.) ``Molecule``s also cannot be oriented by programs invoked by the QCFractal ecosystem so orientation is preserved post-calculation.
- (:pr:`146`) CI environments have been simplified to make maintaining them easier, improve test coverage, and find more bugs.
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
