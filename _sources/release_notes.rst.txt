Release Notes
=============

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
