Changelog
=========

.. Use headers commented below commented as templates

.. X.Y.0 / 2020-MM-DD
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

0.15.8 / 2022-02-04
-------------------

Some minor additions/fixes, mostly to to qcportal and the user interface. No database migrations.

- (:pr:`694`, :pr:`701`) Make QCFractal compatible with new QCElemental and QCEngine versions
- (:pr:`700`) Replace concurrent.futures with multiprocessing in ExecutorAdapter to help with process termination issues 
- (:pr:`704`) Eliminate issues when retrieving datasets with duplicate records
- (:pr:`704`) Make saving optional when calling Dataset.compute



0.15.7 / 2021-11-05
-------------------

Some minor additions/fixes, mostly to improve performance of some operations
and to prevent consistency issues. The database migrations in (:pr:`676`) have
been thoroughly tested, however please backup you database before ugrading!

- (:pr:`683`) Fixes issue with keyword aliases not being compared case-insensitive
- (:pr:`685`) Fixes issues with duplicate calculations, large client bodies, and efficiency of certain searches
- (:pr:`686`) Pins qcelemental/qcengine versions
- (:pr:`691`) Fix for some dftd3 calculations not working due to composition_planner
- (:pr:`693`) Allow for additional keywords in torsiondrive optimizations
- (:pr:`694`) Support (and pin) QCElemental v0.23 and QCEngine 0.20



0.15.6 / 2021-06-06
-------------------

Some minor additions/fixes, mostly to the user interface. The database migration in (:pr:`676`) has been thoroughly tested,
however please backup you database before ugrading!

Client and managers should not need to be upgraded.

- (:pr:`672`) Adds ability to add compute specs to only a subset of entries in a Dataset
- (:pr:`673`) Allow for selecting by status in dataset get_records()
- (:pr:`676`) A migration for fixing fields in the database which have been changed in QCSchema/QCElemental
- (:pr:`678`) Fixes errors related to str vs. bytes in collection views
- (:pr:`679`) Fix incorrect status reporting in collections


0.15.3 / 2021-03-15
-------------------

This is a small release focused on some database migrations to improve performance.
This should greatly improve performance of certain actions (particularly task submission)
with large databases.

This release also drops support for python < 3.7

Client and managers should not need to be upgraded.

- (:pr:`663`) Adds indices to base_result and molecule (improves ability to delete orphan kvstore)
- (:pr:`664`) Adds indices to base_result and access_log (improves existing procedure lookup)


0.15.0 / 2020-11-11
-------------------

This release is focused on bugfixes, and laying some foundation for larger changes to come.

New features
++++++++++++

- (:pr:`636`) Add ability to profile fractal instances
- (:pr:`642`) Add (experimental!) qcexport code to devtools

Enhancements
++++++++++++

- (:pr:`629`) (Standard) Output of torsion drive service is now captured and stored in the procedure record
- (:pr:`631`) Compress errors on server

Bug Fixes
+++++++++

- (:pr:`624`) Lock task queue rows to prevent multiple managers requesting the same task
- (:pr:`626`) Fix printing of client version during version check failure
- (:pr:`632`) Fix ordering of initial/final molecule in torsion drives
- (:pr:`637`) Fix inability to shutdown ProcessPoolExecutor workers
- (:pr:`638`) Fix incorrect error in datasets
- (:pr:`641`) Fix exception in web handler that was polluting log files

Miscellaneous
+++++++++++++
- (:pr:`633`, :pr:`634`, :pr:`635`, :pr:`639`) Miscellaneous cleanup and removal of unused database columns


0.14.0 / 2020-09-30
-------------------

New Features
++++++++++++

- (:pr:`597`) Add ability to query managers
- (:pr:`612`) Enabled compression of KVStore (generally, outputs)
- (:pr:`617`) Ability to control level of logging via the command line
- (:pr:`620`) Add ability to regenerate and modify tasks


Enhancements
++++++++++++

- (:pr:`592` and :pr:`615`) Improved performance of task retrieval of managers
- (:pr:`620`) Improve performance of task submission, and add additional logging

Bug Fixes
+++++++++

- (:pr:`603`) Fix error when running older computations missing 'protocols'
- (:pr:`617`) Fix printing of base folder with the CLI


0.13.1 / 2020-02-18
-------------------

New Features
++++++++++++
- (:pr:`566`) A ``list_keywords`` function was added to ``Dataset``.

Enhancements
++++++++++++
- (:pr:`547`, :pr:`553`) Miscellaneous documentation edits and improvements.
- (:pr:`556`) Molecule queries filtered on molecular formula no longer depend on the order of elements.
- (:pr:`565`) ``query`` method for ``Datasets`` now returns collected records.

Bug Fixes
+++++++++
- (:pr:`559`) Fixed an issue where Docker images did not have qcfractal in their PATH.
- (:pr:`561`) Fixed a bug that caused errors with pandas v1.0.
- (:pr:`564`) Fixes a bug where optimization protocols were not respected during torsiondrives and grid optimizations.


0.13.0 / 2020-01-15
-------------------

New Features
++++++++++++
- (:pr:`541`) Managers can now accept multiple tags. Tasks are pulled first in order of tag, then priority, then creation time.
- (:pr:`544`) Adds backup/restore commands to the QCFractal CLI to allow for easier backup and restore options.

Enhancements
++++++++++++
- (:pr:`507`) Automatically adds collection molecules in chunks if more than the current limit needs to be submitted.
- (:pr:`515`) Conda environments now correspond to docker images in all deployed cases.
- (:pr:`524`) The ``delete_collection`` function was added to ``qcportal.FractalClient``.
- (:pr:`530`) Adds the ability to specify cores per rank for node-parallel tasks in QCEngine.
- (:pr:`507`) Adds a formatting and lint check to CI during pull requests.
- (:pr:`535`) Allows dftd3 to be computed for all stoichiometries rather than just defaults.
- (:pr:`542`) Important: TaskRecord.base_result is now an ObjectId (int or str), and no more a ``DBRef``. So, code that uses ``my_task.base_result.id`` should change to simply use ``my_task.base_result``.

Bug Fixes
+++++++++
- (:pr:`506`) Fixes repeated visualize calls where previously the visualize call would corrupt local state.
- (:pr:`521`) Fixes an issue where ProcessPoolExecutor returned the incorrect number of currently running tasks.
- (:pr:`522`) Fixes a bug where ``ProcedureDataset.status()`` failed for specifications where only a subset was computed.
- (:pr:`525`) This PR fixes ENTRYPOINT of the qcarchive_worker_openff worker. (Conda and Docker are not friends.)
- (:pr:`532`) Fixes a testing subprocess routine when coverage is enabled for coverage 5.0 breaking changes.
- (:pr:`543`) Fixes a bug where ``qcfractal-server`` "start" before an "upgrade" prevented the "upgrade" command from correctly running.
- (:pr:`545`) Fixed an issue in Dataset.get_records() that could occur when the optional arguments keywords and basis were not provided.



0.12.2 / 2019-12-07
-------------------

Enhancements
++++++++++++
- (:pr:`477`) Removes 0.12.x xfails when connecting to the server.
- (:pr:`481`) Expands Parsl Manager Adapter to include ALCF requirements.
- (:pr:`483`) Dataset Views are now much faster to load in HDF5.
- (:pr:`488`) Allows gzipped dataset views.
- (:pr:`490`) Computes checksums on gzipped dataset views.
- (:pr:`542`) ``TaskRecord.base_result`` is now an ``ObjectId``, and no more a ``DBRef``. So, code that uses ``my_task.base_result.id`` should change to simply be ``my_task.base_result``.

Bug Fixes
+++++++++
- (:pr:`486`) Fixes pydantic ``__repr__`` issues after update.
- (:pr:`492`) Fixes error where ``ReactionDataset`` didn't allow a minimum number of n-body expansion to be added.
- (:pr:`493`) Fixes an issue with ``ReactionDataset.get_molecules`` when a subset is present.
- (:pr:`494`) Fixes an issue where queries with ``limit=0`` erroneously returned all results.
- (:pr:`496`) TorsionDrive tests now avoid 90 degree angles with RDKit to avoid some linear issues in the forcefield and make them more stable.
- (:pr:`497`) ``TorsionDrive.get_history`` now works for extremely large (1000+) optimizations in the procedure.

0.12.1 / 2019-11-08
-------------------

Enhancements
++++++++++++
- (:pr:`472`) Update to GitHub ISSUE templates.
- (:pr:`473`) Server ``/information`` endpoint now contains the number of records for molecules, results, procedures, and collections.
- (:pr:`474`) Dataset Views can now be of arbitrary shape.
- (:pr:`475`) Changes the default formatting of the codebase to Black.


Bug Fixes
+++++++++
- (:pr:`470`) Dataset fix for non-energy units.

0.12.0 / 2019-11-06
-------------------

Highlights
++++++++++

- The ability to handle very large datasets (1M+ entries) quickly and efficiently.
- Store and compute Wavefunction information.
- Build, serve, and export views for Datasets that can stored in journal supplementary information or services like Zenodo.
- A new GUI dashboard to observe the current state of the server, see statistics, and fix issues.

New Features
++++++++++++
- (:pr:`433` and :pr:`462`) ``Dataset`` and ``ReactionDataset`` (``interface.collections``) now have a ``download``` method which
  downloads a frozen view of the dataset. This view is used to speed up calls to ``get_values``, ``get_molecules``,
  ``get_entries``, and ``list_values``.
- (:pr:`440`) Wavefunctions can now be stored in the database using Result ``protocols``.
- (:pr:`453`) The server now periodically logs manager and current state to provide data over time.
- (:pr:`460`) Contributed values are now in their own table to speed up access of Collections.
- (:pr:`461`) Services now update their corresponding record every iteration. An example is a torsiondrive which now updates the ``optimization_history`` field each iteration.

Enhancements
++++++++++++
- (:pr:`429`) Enables protocols for ``OptimizationDataset`` collections.
- (:pr:`430`) Adds additional QCPortal type hints.
- (:pr:`433`, :pr:`443`) ``Dataset`` and ``ReactionDataset`` (``interface.collections``) are now faster for calls to calls to ``get_values``, ``get_molecules``,
  ``get_entries``, and ``list_values`` for large datasets if the server is configured to use frozen views. See "Server-side Dataset Views" documentation. Subsets
  may be passed to ``get_values``, ``get_molecules``, and ``get_entries``
- (:pr:`447`) Enables the creation of plaintext (xyz and csv) output from Dataset Collections.
- (:pr:`455`) Projection queries should now be much faster as excluded results are not pulled to the server.
- (:pr:`458`) Collections now have a metadata field.
- (:pr:`463`) ``FractalClient.list_collections`` by default only returns collections whose visibility flag is set to true,
  and whose group is "default". This change was made to filter out in-progress, intermediate, and specialized collections.
- (:pr:`464`) Molecule insert speeds are now 4-16x faster.

Bug Fixes
+++++++++
- (:pr:`424`) Fixes a ``ReactionDataset.visualize`` bug with ``groupby='D3'``.
- (:pr:`456`, :pr:`452`) Queries that project hybrid properties should now work as expected.


Deprecated Features
+++++++++++++++++++
- (:pr:`426`) In ``Dataset`` and ``ReactionDataset`` (``interface.collections``),
  the previously deprecated functions ``query``, ``get_history``, and ``list_history`` have been removed.

Optional Dependency Changes
+++++++++++++++++++++++++++
- (:pr:`454`) Users of the optional Parsl queue adapter are required to upgrade to Parsl v0.9.0, which fixes
  issues that caused SLURM managers to crash.

0.11.0 / 2019-10-01
-------------------

New Features
++++++++++++

- (:pr:`420`) Pre-storage data handling through Elemental's ``Protocols`` feature are now present in Fractal. Although
  only optimization protocols are implemented functionally, the database side has been upgraded to store protocol
  settings.

Enhancements
++++++++++++

- (:pr:`385`, :pr:`404`, :pr:`411`) ``Dataset`` and ``ReactionDataset`` have five new functions for accessing data.
  ``get_values`` returns the canonical headline value for a dataset (e.g. the interaction energy for S22) in data
  columns with caching, both for result-backed values and contributed values. This function replaces the now-deprecated
  ``get_history`` and ``get_contributed_values``. ``list_values`` returns the list of data columns available from
  ``get_values``. This function replaces the now-deprecated ``list_history`` and ``list_contributed_values``.
  ``get_records`` either returns ``ResultRecord`` or a projection. For the case of ``ReactionDataset``, the results are
  broken down into component calculations. The function replaces the now-deprecated ``query``.
  ``list_records`` returns the list of data columns available from ``get_records``.
  ``get_molecules`` returns the ``Molecule`` associated with a dataset.
- (:pr:`393`) A new feature added to ``Client`` to be able to have more custom and fast queries, the ``custom_query``
  method.
  Those fast queries are now used in ``torsiondrive.get_final_molecules`` and ``torsiondrive.get_final_results``. More
  Advanced queries will be added.
- (:pr:`394`) Adds ``tag`` and ``manager`` selector fields to ``client.query_tasks``.
  This is helpful for managing jobs in the queue and detecting failures.
- (:pr:`400`, :pr:`401`, :pr:`410`) Adds Dockerfiles corresponding to builds on
  `Docker Hub <https://cloud.docker.com/u/molssi/repository/list>`_.
- (:pr:`406`) The ``Dataset`` collection's primary indices (database level) have been updated to reflect its new
  understanding.


Bug Fixes
+++++++++

- (:pr:`396`) Fixed a bug in internal ``Dataset`` function which caused ``ComputeResponse`` to be truncated when the
  number of calculations is larger than the query_limit.
- (:pr:`403`) Fixed ``Dataset.get_values`` for any method which involved DFTD3.
- (:pr:`409`) Fixed a compatibility bug in specific version of Intel-OpenMP by skipping version
  2019.5-281.

Documentation Improvements
++++++++++++++++++++++++++

- (:pr:`399`) A Kubernetes quickstart guide has been added.

0.10.0 / 2019-08-26
-------------------

.. note:: Stable Beta Release

    This release marks Fractal's official Stable Beta Release. This means that future, non-backwards compatible
    changes to the API will result in depreciation warnings.


Enhancements
++++++++++++

- (:pr:`356`) Collections' database representations have been improved to better support future upgrade paths.
- (:pr:`375`) Dataset Records are now copied alongside the Collections.
- (:pr:`377`) The ``testing`` suite from Fractal now exposes as a PyTest entry-point when Fractal is installed so
  that tests can be run from anywhere with the ``--pyargs qcfractal`` flag of ``pytest``.
- (:pr:`384`) "Dataset Records" and "Reaction Dataset Records" have been renamed to "Dataset Entry" and "Reaction
  Dataset Entry" respectively.
- (:pr:`387`) The auto-documentation tech introduced in :pr:`321` has been replaced by the improved implementation in
  Elemental.

Bug Fixes
+++++++++

- (:pr:`388`) Queue Manager shutdowns will now signal to reset any running tasks they own.

Documentation Improvements
++++++++++++++++++++++++++

- (:pr:`372`, :pr:`376`) Installation instructions have been updated and typo-corrected such that they are accurate
  now for both Conda and PyPi.

0.9.0 / 2019-08-16
------------------

New Features
++++++++++++

- (:pr:`354`) Fractal now takes advantage of Elemental's new Msgpack serialization option for Models. Serialization
  defaults to msgpack when available (``conda install msgpack-python [-c conda-forge]``), falling back to JSON
  otherwise. This results in substantial speedups for both serialization and deserialization actions and should be a
  transparent replacement for users within Fractal, Engine, and Elemental themselves.
- (:pr:`358`) Fractal Server now exposes a CLI for user/permissions management through the ``qcfractal-server user``
  command. `See the full documentation for details <https://qcfractal.readthedocs.io/en/latest/server_user.html>`_.
- (:pr:`358`) Fractal Server's CLI now supports user manipulations through the ``qcfractal-server user`` subcommand.
  This allows server administrators to control users and their access without directly interacting with the storage
  socket.

Enhancements
++++++++++++

- (:pr:`330`, :pr:`340`, :pr:`348`, :pr:`349`) Many Pydantic based Models attributes are now documented and in an
  on-the-fly manner derived from the Pydantic Schema of those attributes.
- (:pr:`335`) Dataset's ``get_history`` function is fixed by allowing the ability to force a new query even if one has
  already been cached.
- (:pr:`338`) The Queue Manager which generated a ``Result`` is now stored in the ``Result`` records themselves.
- (:pr:`341`) Skeletal Queue Manager YAML files can now be generated through the ``--skel`` or ``--skeleton`` CLI flag
  on ``qcfractal-manager``
- (:pr:`361`) Staged DB's in Fractal copy Alembic alongside them.
- (:pr:`363`) A new REST API hook for services has been added so Clients can manage Services.

Bug Fixes
+++++++++

- (:pr:`359`) A ``FutureWarning`` from Pandas has been addressed before it becomes an error.

Documentation Improvements
++++++++++++++++++++++++++

- (:pr:`351`, :pr:`352`, :pr:`353`, :pr:`360`, :pr:`362`, :pr:`364`, :pr:`366`, :pr:`368`) The documentation has been
  significantly edited to be up to date, fix numerous typos, reworded and refined for clarity, and overall flow better
  between pages.

0.8.0 / 2019-07-25
------------------

Breaking Changes
++++++++++++++++

.. warning:: PostgreSQL is now the only supported database backend.

    Fractal has officially dropped support for MongoDB in favor of PostgreSQL as our
    database backend. Although MongoDB served the start of Fractal well, our database design
    as evolved since then and will be better served by PostgreSQL.

New Features
++++++++++++

- (:pr:`307`, :pr:`319` :pr:`321`) Fractal's Server CLI has been overhauled to more intuitively and intelligently
  control Server creation, startup, configuration, and upgrade paths. This is mainly reflected in a Fractal Server
  config file, a config folder
  (default location ``~/.qca``, and sub-commands ``init``, ``start``, ``config``, and ``upgrade`` of the
  ``qcfractal-server (command)`` CLI.
  `See the full documentation for details <https://qcfractal.readthedocs.io/en/latest/server_config.html>`_
- (:pr:`323`) First implementation of the ``GridOptimizationDataset`` for collecting Grid Optimization calculations.
  Not yet fully featured, but operational for users to start working with.


Enhancements
++++++++++++

- (:pr:`291`) Tests have been formally added for the Queue Manager to reduce bugs in the future. They cannot test on
  actual Schedulers yet, but its a step in the right direction.
- (:pr:`295`) Quality of life improvement for Mangers which by default will be less noisy about heartbeats and trigger
  a heartbeat less frequently. Both options can still be controlled through verbosity and a config setting.
- (:pr:`296`) Services are now prioritized by the date they are created to properly order the compute queue.
- (:pr:`301`) ``TorsionDriveDataset`` status can now be checked through the ``.status()`` method which shows the
  current progress of the computed data.
- (:pr:`310`) The Client can now modify tasks and restart them if need be in the event of random failures.
- (:pr:`313`) Queue Managers now have more detailed statistics about failure rates, and core-hours consumed (estimated)
- (:pr:`314`) The ``PostgresHarness`` has been improved to include better error handling if Postgress is not found, and
  will not try to stop/start if the target data directory is already configured and running.
- (:pr:`318`) Large collections are now automatically paginated to improve Server/Client response time and reduce
  query sizes. See also :pr:`322` for the Client-side requested pagination.
- (:pr:`322`) Client's can request paginated queries for quicker responses. See also :pr:`318` for the Server-side
  auto-pagination.
- (:pr:`322`) ``Record`` models and their derivatives now have a ``get_molecule()`` method for fetching the molecule
  directly.
- (:pr:`324`) Optimization queries for its trajectory pull the entire trajectory in one go and keep the correct order.
  ``get_trajectory`` also pulls the correct order.
- (:pr:`325`) Collections' have been improved to be more efficient. Previous queries are cached locally and the
  ``compute`` call is now a single function, removing the need to make a separate call to the submission formation.
- (:pr:`326`) ``ReactionDataset`` now explicitly groups the fragments to future-proof this method from upstream
  changes to ``Molecule`` fragmentation.
- (:pr:`329`) All API requests are now logged server side anonymously.
- (:pr:`331`) Queue Manager jobs can now auto-retry failed jobs a finite number of times through QCEngine's retry
  capabilities. This will only catch RandomErrors and all other errors are raised normally.
- (:pr:`332`) SQLAlchemy layer on the PostgreSQL database has received significant polish


Bug Fixes
+++++++++

- (:pr:`291`) Queue Manager documentation generation works on Pydantic 0.28+. A number as-of-yet uncaught/unseen bugs
  were revealed in tests and have been fixed as well.
- (:pr:`300`) Errors thrown in the level between Managers and their Adapters now correctly return a ``FailedOperation``
  instead of ``dict`` to be consistent with all other errors and not crash the Manager.
- (:pr:`301`) Invalid passwords present a helpful error message now instead of raising an Internal Server Error to the
  user.
- (:pr:`306`) The Manager CLI option ``tasks-per-worker`` is correctly hyphens instead of underscores to be consistent
  with all other flags.
- (:pr:`316`) Queue Manager workarounds for older versions of Dask-Jobqueue and Parsl have been removed and implicit
  dependency on the newer versions of those Adapters is enforced on CLI usage of ``qcfractal-manager``. These packages
  are *not required* for Fractal, so their versions are only checked when specifically used in the Managers.
- (:pr:`320`) Duplicated ``initial_molecules`` in the ``TorsionDriveDataset`` will no longer cause a failure in adding
  them to the database while still preserving de-duplication.
- (:pr:`327`) Jupyter Notebook syntax highlighting has been fixed on Fractal's documentation pages.
- (:pr:`331`) The BaseModel/Settings auto-documentation function can no longer throw an error which prevents
  using the code.


Deprecated Features
+++++++++++++++++++

- (:pr:`291`) Queue Manager Template Generator CLI has been removed as its functionality is superseded by the
  ``qcfractal-manager`` CLI.


0.7.2 / 2019-05-31
------------------

New Features
++++++++++++

- (:pr:`279`) Tasks will be deleted from the ``TaskQueue`` once they are completed successfully.
- (:pr:`271`) A new set of scripts have been created to facilitate migration between MongoDB and PostgreSQL.

Enhancements
++++++++++++

- (:pr:`275`) Documentation has been further updated to be more contiguous between pages.
- (:pr:`276`) Imports and type hints in Database objects have been improved to remove ambiguity and make imports easier
  to follow.
- (:pr:`280`) Optimizations queried in the database are done with a more efficient lazy ``selectin``. This should make
  queries much faster.
- (:pr:`281`) Database Migration tech has been moved to their own folder to keep them isolated from normal
  production code. This PR also called the testing database ``test_qcarchivedb`` to avoid
  clashes with production DBs. Finally, a new keyword for testing geometry optimizations
  has been added.

Bug Fixes
+++++++++

- (:pr:`280`) Fixed a SQL query where ``join`` was set instead of ``noload`` in the lazy reference.
- (:pr:`283`) The monkey-patch for Dask + LSF had a typo in the keyword for its invoke. This has
  been fixed for the monkey-patch, as the upstream change was already fixed.


0.7.1 / 2019-05-28
------------------

Bug Fixes
+++++++++

- (:pr:`277`) A more informative error is thrown when Mongo is not found by ``FractalSnowflake``.
- (:pr:`277`) ID's are no longer presented when listing Collections in Portal to minimize extra data.
- (:pr:`278`) Fixed a bug in Portal where the Server was not reporting the correct unit.


0.7.0 / 2019-05-27
------------------

.. warning:: Final MongoDB Supported Release

    **This is the last major release which support MongoDB.** Fractal is moving towards a PostgreSQL for database to
    make upgrades more stable and because it is more suited to the nature of QCArchive Data. The upgrade path from
    MongoDB to PostgreSQL will be provided by the Fractal developers in the next release. Due to the complex nature
    of the upgrade, the PostgreSQL upgrade will through scripts which will be provided. After the PostgreSQL upgrade,
    there will be built-in utilities to upgrade the Database.

New Features
++++++++++++

- (:pr:`206`, :pr:`249`, :pr:`264`, :pr:`267`) SQL Database is now feature complete and implemented. As final testing in
  production is continued, MongoDB will be phased out in the future.
- (:pr:`242`) Parsl can now be used as an ``Adapter`` in the Queue Managers.
- (:pr:`247`) The new ``OptimizationDataset`` collection has been added! This collection returns a set of optimized
  molecular structures given an initial input.
- (:pr:`254`) The QCFractal Server Dashboard is now available through a Dash interface. Although not fully featured yet,
  future updates will improve this as features are requested.
- (:pr:`260`) Its now even easier to install Fractal/Portal through conda with pre-built environments on the
  ``qcarchive`` conda channel. This channel only provides environment files, no packages (and there are not plans to
  do so.)
- (:pr:`269`) The Fractal Snowflake project has been extended to work in Jupyter Notebooks. A Fractal Snowflake can
  be created with the ``FractalSnowflakeHandler`` inside of a Jupyter Session.

Database Compatibility Updates
++++++++++++++++++++++++++++++

- (:pr:`256`) API calls to Elemental 0.4 have been updated. This changes the hashing system and so upgrading your
  Fractal Server instance to this (or higher) will require an upgrade path to the indices.

Enhancements
++++++++++++

- (:pr:`238`) ``GridOptimizationRecord`` supports the helper function ``get_final_molecules`` which returns the
  set of molecules at each final, optimized grid point.
- (:pr:`259`) Both ``GridOptimizationRecord`` and ``TorsionDriveRecord`` support the helper function
  ``get_final_results``, which is like ``get_final_molecules``, but for x
- (:pr:`241`) The visualization suite with Plotly has been made more general so it can be invoked in different classes.
  This particular PR updates the TorsionDriveDataSet objects.
- (:pr:`243`) TorsionDrives in Fractal now support the updated Torsion Drive API from the underlying package. This
  includes both the new arguments and the "extra constraints" features.
- (:pr:`244`) Tasks which fail are now more verbose in the log as to why they failed. This is additional information
  on top of the number of pass/fail.
- (:pr:`246`) Queue Manager ``verbosity`` level is now passed down into the adapter programs as well and the log
  file (if set) will continue to print to the terminal as well as the physical file.
- (:pr:`247`) Procedure classes now all derive from a common base class to be more consistent with one another and
  for any new Procedures going forward.
- (:pr:`248`) Jobs which fail, or cannot be returned correctly, from Queue Managers are now better handled in the
  Manager and don't sit in the Manager's internal buffer. They will attempt to be returned to the Server on later
  updates. If too many jobs become stale, the Manager will shut itself down for safety.
- (:pr:`258` and :pr:`268`) Fractal Queue Managers are now fully documented, both from the CLI and through the doc pages
  themselves. There have also been a few variables renamed and moved to be more clear the nature of what they do.
  See the PR for the renamed variables.
- (:pr:`251`) The Fractal Server now reports valid minimum/maximum allowed client versions. The Portal Client will try
  check these numbers against itself and fail to connect if it is not within the Server's allowed ranges. Clients
  started from Fractal's ``interface`` do not make this check.

Bug Fixes
+++++++++

- (:pr:`248`) Fixed a bug in Queue Managers where the extra worker startup commands for the Dask Adapter were not being
  parsed correctly.
- (:pr:`250`) Record objects now correctly set their provenance time on object creation, not module import.
- (:pr:`253`) A spelling bug was fixed in GridOptimization which caused hashing to not be processed correctly.
- (:pr:`270`) LSF clusters not in ``MB`` for the units on memory by config are now auto-detected (or manually set)
  without large workarounds in the YAML file and the CLI file itself. Supports documented settings of LSF 9.1.3.

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

- (:pr:`226`) LSF can now be specified for the Queue Managers for Dask Managers.
- (:pr:`228`) Plotly is an optional dependency overall, it is not required to run QCFractal or QCPortal but will be
  downloaded in some situations. If you don't have Plotly installed, more graceful errors beyond just raw
  ``ImportErrors`` are given.
- (:pr:`234`) Queue Managers now report the number of passed and failed jobs they return to the server and can also
  have verbose (debug level) outputs to the log.
- (:pr:`234`) Dask-driven Queue Managers can now be set to simply scale up to a fixed number of workers instead of
  trying to adapt the number of workers on the fly.

Bug Fixes
+++++++++

- (:pr:`227`) SGE Clusters specified in Queue Manager under Dask correctly process ``job_extra`` for additional
  scheduler headers. This is implemented in a stable way such that if the upstream Dask Jobqueue implements a fix, the
  Manager will keep working without needing to get a new release.
- (:pr:`234`) Fireworks managers now return the same pydantic models as every other manager instead of raw dictionaries.


0.5.4 / 2019-03-21
------------------

New Features
++++++++++++

- (:pr:`216`) Jobs submitted to the queue can now be assigned a priority to be served out to the Managers.
- (:pr:`219`) Temporary, pop-up, local instances of ``FractalServer`` can now be created through the
  ``FractalSnowflake``. This creates an instance of ``FractalServer``, with its database structure, which is entirely
  held in temporary storage and memory, all of which is deleted upon exit/stop. This feature is designed for those
  who want to tinker with Fractal without needed to create their own database or connect to a production
  ``FractalServer``.
- (:pr:`220`) Queue Managers can now set the ``scratch_directory`` variable that is passed to QCEngine and its workers.

Enhancements
++++++++++++

- (:pr:`216`) Queue Managers now report what programs and procedures they have access to and will only pull jobs they
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
  ``QueueManager``\s to only carry out computations with specified tags.
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
  are for most users whereas QCFractal docs will be for those creating their own Managers, Fractal instances, and
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
- (:pr:`150`) ``KeywordSet`` objects are now modeled much more consistently through pydantic models and are consistently hashed to survive round trip serialization.
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
- (:pr:`133`) ``Molecule`` objects cannot be oriented once they enter the QCFractal ecosystem (after optional initial orientation.) ``Molecule`` objects also cannot be oriented by programs invoked by the QCFractal ecosystem so orientation is preserved post-calculation.
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
- (:pr:`80`) A `Parsl <http://parsl-project.org>`_ Queue Manager was written.
- (:pr:`75`) CLI's have been added for the ``qcfractal-server`` and ``qcfractal-manager`` instances.
- (:pr:`83`) The status of server tasks and services can now be queried from a FractalClient.
- (:pr:`82`) OpenFF Workflows can now add single optimizations for fragments.

Enhancements
++++++++++++

- (:pr:`74`) The documentation now has flowcharts showing task and service pathways through the code.
- (:pr:`73`) Collection ``.data`` attributes are now typed and validated with pydantic.
- (:pr:`85`) The CLI has been enhanced to cover additional features such as ``queue-manager`` ping time.
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
- (:pr:`53`) The MongoSocket ``get_generic_by_id`` was deprecated in favor of ``get_generic`` where an ID can be a search field.
- (:pr:`61`, :pr:`64`) TorsionDrive now tracks tasks via ID rather than hash to ensure integrity.
- (:pr:`63`) The Database collection was renamed Dataset to more correctly illuminate its purpose.
- (:pr:`65`) Collection can now be aquired directly from a client via the ``client.get_collection`` function.

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
