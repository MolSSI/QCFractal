Release Notes
=============

0.58 / 2025-01-31
-----------------

This release has some big new features! These new features should be stable, but report
any bugs you may find.

1. Dataset views can now be attached to a dataset and stored in S3-compatible storage. The views are
created server-side, which is very efficient.
2. Dataset record submission can now be handled server-side as well. A call to `background_submit`
will return immediately, while the submission happens via an internal job on the server
3. New manybody implementation

**Note:** The new manybody implementation is not compatible with old manybody data. The upgrade
will check if manybody records exist and will not upgrade if any are present. I don't expect this to
cause issues, but if it does, contact me.

In addition, there are many other small features and fixes

Notable PRs:

- (:pr:`866`) Better error message with snowflake import of qcfractalcompute
- (:pr:`867`) Fix an awkward exception when a dataset record could not be found
- (:pr:`869`) Allow for manual specification of snowflake host
- (:pr:`871`) Improvement for the way specifications are added to the database
- (:pr:`872`) Simplification of how task queue sorts records that belong to services
- (:pr:`874`) Improvements to internal job handling
- (:pr:`876`) Ability to specify times in configurations files as user-friendly strings
- (:pr:`877`) Option to shut down managers that are idle for too long (ie, do not pick up tasks)
- (:pr:`878`) Dataset internal jobs, external files, and server-side view creation
- (:pr:`879`) New manybody implementation
- (:pr:`880`) Ability to create a portal client object from environment variables
- (:pr:`881`) Ability to submit dataset records as a background job
- (:pr:`882`) Improvements to processing of returned data
- (:pr:`885`) Use officially-released geometric
- (:pr:`886`) Use retries for JWT fetching (to help with some sporadic errors)
- (:pr:`889`) Implement jitter for manager heartbeats and updates


0.57 / 2024-12-12
-----------------

A couple new features, some nice improvements, and of course some bug fixes!
This update should be backwards compatible - new clients can access old servers, and old clients can
access new servers. Same for compute managers.

**Note:** this release drops support for Python 3.8, which is now EOL.

Notable PRs:

- (:pr:`848`) Pin APSW (an SQLite wrapper) to a recent version
- (:pr:`851`) Mark managers as modified when they claim or return something (to prevent busy managers from being inactivated)
- (:pr:`852`) Improve performance of task claiming by managers
- (:pr:`853`) Reduce number of SQLAlchemySocket instances created on startup
- (:pr:`854`) Extras fields of records should not be None/NULL
- (:pr:`855`) Improve script startup time by lazy-loading pandas
- (:pr:`856`) Drop support for python 3.8
- (:pr:`857`) Add option for opt-in usage tracking for Parsl
- (:pr:`858`) Pin pyjwt & enable invalid subject handling
- (:pr:`861`) Ability to add entries to a singlepoint dataset from other datasets
- (:pr:`863`) Remove channels from Parsl config


0.56 / 2024-07-09
-------------------

Two small bugfixes. One is related to how the cache works, the other is a slight modification of the NEB specification. 

The fix to the caching behavior should help with a lot of unexpected behavior related to caching. See :issue:`844`.

- (:pr:`841`) Remove hessian_reset keyword from NEB (:contrib:`hjnpark`)
- (:pr:`843`) Write records to cache immediately after fetching


0.55 / 2024-05-23
-------------------

Some moderate improvements - mainly, the use of ASPW for SQLite, and the removal of the never-used manager log and
serverinfo log tables. Also adds maintenance jobs for removing old access log and completed internal jobs.

This release also adds the ability to more-strictly handle queue tags (to prevent managers with `*` as a tag from pulling
everything).

In addition, lots of smaller bug fixes and improvements.

Notable PRs:

- (:pr:`819`) Fix native_files fields return from server & ORM
- (:pr:`821`) Enable dumping a database from a snowflake 
- (:pr:`822`) Fix missing client on cached dataset records
- (:pr:`825`) Fix database constraint violation when program version contains uppercase characters 
- (:pr:`826`) Fix type of results in ServiceSubtaskRecord
- (:pr:`829`) Replace sqlite3 (python stdlib module) with APSW
- (:pr:`830`) Remove server stats & compute manager logs
- (:pr:`831`) Add internal job to delete old access log entries
- (:pr:`832`) Add internal job to delete old, finished internal jobs
- (:pr:`834`) Add property to get errored child records
- (:pr:`835`) Better handling of queue tags (strict queue tags and case insensitivity)
- (:pr:`836`) Replace gunicorn with waitress and fix logging issues
- (:pr:`837`) Commit to database after every returned task is processed (to help prevent deadlocks)
- (:pr:`838`) Better handling of passwords with `init-db`


0.54.1 / 2024-04-12
-------------------

This is a minor fix-up release that fixes a few issues from the v0.54 release.

- (:pr:`815`) Adds tag to the task queue sort index
- (:pr:`816`) Fixes a few issues related to caching


0.54 / 2024-04-09
-----------------

Two big features of this release is client-side caching (including views) and the ability to download more of records.
As part of this, fetching lots of records from a server will automatically scale to keep a relatively constant
request time, rather than use a fixed batch size.

Client-side caching is relatively functional, but this was a major change, so feel free to report issues as always.

In addition, there is some of the usual cleanup.

- (:pr:`802`) Implement client-side caching using SQLite
- (:pr:`808`) Better handling of missing tags/programs from managers
- (:pr:`809`) Improve fetching speed by allowing for including more of records
- (:pr:`811`) Improve task queue performance by storing time in the task queue table directly


0.53 / 2024-01-09
-----------------

The only real thing to report is fixing of molecules returned from the server. Other than that,
a little bit of cleanup in preparation for implementing new features in the future
No breaking changes. Upgrading qcportal is recommended, but is not required.

Notable pull requests and features:

- (:pr:`798`) Mark molecules coming from the server as already validated, and remove `fix_com` and `fix_orientation` from the database.


0.52 / 2023-11-29
-----------------

Some improvements and bugfixes, but no breaking changes. Upgrading qcportal is recommended
due to fixes related to JWTs, but is not required. The same is true with compute managers.

Notable pull requests and features:

- (:pr:`781`) Fixes issues related to shutdown of snowflakes, particularly with Python 3.12
- (:pr:`783`, :pr:`793`) Fixes JWT refresh issues that cause errors in clients
- (:pr:`785`) Some cleanups related to Python 3.12 (including removing use of removing `pkg_resources` module)
- (:pr:`787`) Pydantic v1/v2 dual compatibility (L. Naden :contrib:`lnaden`, M. Thompson :contrib:`mattwthompson`, L. Burns :contrib:`loriab`)
- (:pr:`792`) Add ability to get status overview of child records (such as optimizations of a torsiondrive)
- (:pr:`794`) Remove use of now-deprecated `utctime` function and improve handling of timezones


0.51 / 2023-10-19
-----------------

Many new improvements, but very little in the way of breaking changes. Upgrading qcportal is recommended
due to efficiency gains, but is not required.

Notable pull requests and features:

- (:pr:`745`) Compute manager documentation (D. Dotson :contrib:`dotsdl`)
- (:pr:`750`) Use a `computed/generated column <https://www.postgresql.org/docs/current/ddl-generated-columns.html>`_ for lower-case dataset names (lname)
- (:pr:`751`) Some cleanups, including removing dependence of ``geometric_nextchain.py`` on QCPortal
- (:pr:`752`) Tests requiring geoip test data are now automatically skipped if not available
- (:pr:`753`) Improve/Fix JWT handling
- (:pr:`757`) Gracefully handle missing User-Agent
- (:pr:`758`) Requests now will be automatically retried in case of connection or networking issues
- (:pr:`759`) Implement functionality for checking why a record is in the waiting state
- (:pr:`760`) Add existing_ok=True for add_dataset
- (:pr:`761`) Handle duplicates and renames in qcvars
- (:pr:`762`) Add display of number of records in a dataset, and ability to get number of records in a dataset
- (:pr:`763`) reset_records() only resets errored records now
- (:pr:`764`) Add ability to get a list of properties computed in a dataset
- (:pr:`765`) Improve compile_values and related functions (J. Nash :contrib:`janash`)
- (:pr:`768`) Enable use of environment variables when specifying paths in the compute manager config
- (:pr:`769`) Improve the efficiency of adding large numbers of entries to a dataset
- (:pr:`773`) Improve manager logging of task and record information
- (:pr:`774`) Removed forced version checks between client and server
- (:pr:`775`) Add automatic batching in ds.add_entries() and ds.submit()


0.50 / 2023-09-12
-----------------

Major refactoring of everything. Too many changes to enumerate, but see `docs <https://molssi.github.io/QCFractal>`_ for details.
