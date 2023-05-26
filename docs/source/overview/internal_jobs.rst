Internal Jobs
==================

The server contains an internal job queue for periodic maintenance or otherwise asynchronous tasks.
This job queue is managed completely by the QCFractal server. While a user (typically an admin)
can view, cancel, or delete tasks, there is no general way to add tasks to this queue.

When it is time for an internal job to be run, it is picked up by an internal job worker.
This worker is a separate process from the main server process, but is generally on the same
server or closely-related server. Internal jobs are *not* run on compute worker, and the
internal job worker has a lower-level access to the database than compute workers -- compute workers
can only interact with the web API, while job workers can directly interact with the database.

Examples of internal jobs include:
  * Iterating on services
  * Updating server statistics
  * Checking for dead managers
  * Asynchronous submission/deletion of data