Changelog
=========

X.Y.0 / 2018-MM-DD
-------------------

New Features
++++++++++++

Enhancements
++++++++++++

Bug Fixes
+++++++++

0.2.0a / 2018-10-DD
-------------------

This is the second alpha release of QCFractal containing architectural changes
to the relational pieces of the database. Base functionality has been expanded
to generalize the collection idea with BioFragment and OpenFFWorkflow
collections. 

New Features
++++++++++++
 - (:pr:`57`) OpenFFWorkflow and BioFragment collections to support OpenFF uses cases.
 - (:pr:`57`) Requested compute will now return the id of the new submissions or the id of the completed results if duplicates are submitted.

Enhancements
++++++++++++
- (:pr:`43`) Services and Procedures now exist in the same unified table when complete as a single procedure can be completed in either capacity. 
- (:pr:`44`) The backend database was renamed to storage to prevent misunderstanding of the Database collection. 
- (:pr:`47`) Tests can that require an activate Mongo instance are now correctly skipped.
- (:pr:`51`) The queue now uses a fast hash index to determine uniqueness and prevent duplicate tasks. 
- (:pr:`52`) QCFractal examples are now tested via CI.
- (:pr:`53`) The MongoSocket `get_generic_by_id` was deprecated in favor of `get_generic` where an ID can be a search field.

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
