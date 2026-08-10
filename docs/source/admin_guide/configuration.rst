.. _server_configuration:

Server Configuration
====================

This page documents the QCFractal server configuration file format and all available options.
Configuration is defined in YAML and mirrors the Pydantic settings in
``qcfractal/qcfractal/config.py``. Options are grouped by sections according to the
configuration hierarchy shown in that file.

Quick start
-----------

- Create a configuration file (for example ``server.yaml``).
- Run ``qcfractal-server init-config --config server.yaml`` to create an example config with secrets.
- Start the server: ``qcfractal-server start --config server.yaml``.

How configuration is loaded
---------------------------

QCFractal merges configuration from multiple sources (later items have higher priority):

1. YAML files passed to ``--config`` in the order given (earlier files are lower priority).
2. ``extra_config`` from tooling (eg, CLI flags) if provided.
3. Environment variables. Environment variables override values provided by files.

Base folder detection
~~~~~~~~~~~~~~~~~~~~~

Many options are paths that can be relative. Relative paths are resolved against a
"base folder" using the following order:

1. ``QCF_BASE_FOLDER`` environment variable, if set.
2. ``base_folder`` key in the YAML.
3. The directory containing the last (highest-priority) config file passed to ``--config``.

Durations and retention windows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Options documented as "seconds" accept either integers (seconds) or duration strings
  like ``"1h"``, ``"30m"``, ``"2d 6h"``. They are converted internally.
- Retention windows (eg, ``access_log_keep``) accept days as integers or as duration strings
  (eg, ``7`` or ``"7d"``).

Environment variables
---------------------

You can set or override any option via environment variables. Every variable starts with the
``QCF_`` prefix, followed by the name of the option in the YAML file (case does not matter).

For options at the root of the YAML document, that is all you need:

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

      export QCF_LOGLEVEL=DEBUG
      export QCF_BASE_FOLDER=/srv/qcfractal
      export QCF_MAX_ACTIVE_SERVICES=50

Options that live inside a nested section (``database``, ``api``, ``api_limits``, ``cors``,
``auto_reset``, ``s3``) are reached by joining the section name and the option name with a
**double underscore**. The section name is spelled exactly as it appears in the YAML - so
``api_limits`` becomes ``QCF_API_LIMITS__``, not ``QCF_APILIMIT_``.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

      export QCF_DATABASE__HOST=db.example.org
      export QCF_DATABASE__PORT=5432
      export QCF_API__PORT=7777
      export QCF_API_LIMITS__GET_RECORDS=2000
      export QCF_AUTO_RESET__ENABLED=true
      export QCF_S3__ENABLED=true
      export QCF_CORS__ENABLED=true

.. warning::

  Earlier versions of QCFractal used a separate flat prefix for each section
  (``QCF_DB_``, ``QCF_API_``, ``QCF_APILIMIT_``, ``QCF_AUTORESET_``, ``QCF_S3_``).
  None of these work any more, and rather than being ignored they are rejected - the
  server refuses to start and names the replacement:

  .. tab-set::

    .. tab-item:: SHELL

      .. code-block:: bash

        $ export QCF_DB_HOST=db.example.org
        $ qcfractal-server start --config server.yaml
        RuntimeError: Environment variable QCF_DB_HOST is deprecated. Use QCF_DATABASE__HOST instead.

  The replacements are ``QCF_DB_`` → ``QCF_DATABASE__``,
  ``QCF_API_`` → ``QCF_API__``, ``QCF_APILIMIT_`` → ``QCF_API_LIMITS__``,
  ``QCF_AUTORESET_`` → ``QCF_AUTO_RESET__``, and ``QCF_S3_`` → ``QCF_S3__``.
  The check is case-insensitive, so a lowercase ``qcf_db_host`` is caught too.

Top-level settings (FractalConfig)
----------------------------------

These settings live at the root of the YAML document.

Required
~~~~~~~~

.. config-table:: qcfractal.config.FractalConfig

   base_folder

If omitted from the file it is inferred, as described under
:ref:`base folder detection <server_configuration>` above.

General
~~~~~~~

.. config-table:: qcfractal.config.FractalConfig

   name
   enable_security
   allow_unauthenticated_read
   strict_compute_tags

The old name ``strict_queue_tags`` is still accepted for ``strict_compute_tags``, but
logs a deprecation warning.

Logging
~~~~~~~

.. config-table:: qcfractal.config.FractalConfig

   logfile
   loglevel
   hide_internal_errors

``loglevel`` is one of ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``,
case-insensitive. A relative ``logfile`` is resolved against ``base_folder``; leaving it
unset logs to standard output.

Background operations and heartbeats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. config-table:: qcfractal.config.FractalConfig

   service_frequency
   max_active_services
   heartbeat_frequency
   heartbeat_frequency_jitter
   heartbeat_max_missed

A manager is considered dead after ``heartbeat_max_missed`` consecutive missed
heartbeats - by default five checks at 1800 seconds, so about 2.5 hours. Its running
tasks are returned to ``waiting`` to be claimed by another manager.

Access logging and internal jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. config-table:: qcfractal.config.FractalConfig

   log_access
   access_log_keep
   internal_job_processes
   internal_job_keep

Both retention windows are in days, or a duration string, and ``0`` means keep
indefinitely.

GeoIP2 (optional)
~~~~~~~~~~~~~~~~~

.. config-table:: qcfractal.config.FractalConfig

   maxmind_license_key
   geoip2_dir = [base_folder]/geoip2
   geoip2_filename

Static homepage and uploads (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. config-table:: qcfractal.config.FractalConfig

   homepage_redirect_url
   homepage_directory
   upload_directory
   temporary_dir = [base_folder]/qcf_tmp

If ``base_folder`` cannot be used, ``temporary_dir`` falls back to a ``qcf_tmp``
directory under the system temporary directory. It is created if it does not exist.

Nested sections
~~~~~~~~~~~~~~~

The remaining top-level keys are the nested sections documented below.

.. config-table:: qcfractal.config.FractalConfig

   database
   api
   api_limits
   cors
   auto_reset
   s3

Database (database)
-------------------

Settings for the Postgres database connection and, optionally, managing the server’s own DB.
Environment variable prefix: ``QCF_DATABASE__``.

Connection
~~~~~~~~~~

.. config-table:: qcfractal.config.DatabaseConfig
   :omit: base_folder

   full_uri
   host
   port
   database_name
   username
   password
   query

``query`` holds extra connection parameters appended to the URI, such as
``sslmode: require``. Note that ``port`` is ignored when ``host`` is a Unix socket
directory, though it is still used when rendering the URI.

Server-managed database
~~~~~~~~~~~~~~~~~~~~~~~

.. config-table:: qcfractal.config.DatabaseConfig

   own
   data_directory = [base_folder]/postgres
   logfile = [base_folder]/qcfractal_database.log
   pg_tool_dir
   pool_size
   maintenance_db
   echo_sql

The two path defaults apply when ``own`` is true. ``base_folder`` is inherited from the
top level and is not set in this section.

Examples
~~~~~~~~

Minimal, managed Postgres (auto-start):

.. code-block:: yaml

  base_folder: /srv/qcfractal
  database:
    own: true
    username: qcfractal
    password: "<generated>"

External Postgres:

.. code-block:: yaml

  database:
    own: false
    host: db.example.org
    port: 5432
    database_name: qcarchive
    username: qcfractal
    password: "<secret>"

Using a Unix domain socket directory:

.. code-block:: yaml

  database:
    host: /var/run/postgresql
    port: 5432
    database_name: qcarchive
    username: qcfractal
    password: "<secret>"

Web API (api)
-------------

Settings for the HTTP API server. Environment variable prefix: ``QCF_API__``.

Runtime
~~~~~~~

.. config-table:: qcfractal.config.WebAPIConfig

   host
   port
   num_threads_per_worker
   worker_timeout

Security and sessions
~~~~~~~~~~~~~~~~~~~~~

Both secret keys are required, and ``qcfractal-server init-config`` generates them for
you.

.. config-table:: qcfractal.config.WebAPIConfig

   secret_key
   jwt_secret_key
   jwt_access_token_expires
   jwt_refresh_token_expires
   user_session_max_age
   user_session_cookie_name
   user_session_cookie_domain
   user_session_cookie_samesite
   user_session_cookie_partitioned
   user_session_cookie_secure
   user_session_cookie_httponly

For cross-site cookie behaviour set ``user_session_cookie_samesite`` to ``Lax`` or
``None`` as needed. ``user_session_cookie_partitioned`` sets the Partitioned flag, for
CHIPS-style storage partitioning in modern browsers.

Advanced
~~~~~~~~

.. config-table:: qcfractal.config.WebAPIConfig

   extra_flask_options
   extra_waitress_options

These are passed straight through to Flask and to the waitress ``serve`` function
respectively, so anything those accept is valid here and nothing is validated by
QCFractal.

API limits (api_limits)
-----------------------

Limits on sizes and pagination for common API calls. Environment variable prefix: ``QCF_API_LIMITS__``.

Every option here is an integer, and every one is a hard ceiling: a request asking for
more than the limit is not an error, it is silently truncated to the limit. Clients page
through the results, so raising these mainly trades server memory for fewer round trips.

.. config-table:: qcfractal.config.APILimitConfig

.. _server_configuration_autoreset_error:

Automatic resets (auto_reset)
-----------------------------

Limits on how often tasks may be automatically retried based on error type. Environment variable prefix: ``QCF_AUTO_RESET__``.

.. config-table:: qcfractal.config.AutoResetConfig

The counts are per record: once a record has been auto-reset that many times for that
category of error, it is left in ``error`` for a human to look at. See
:ref:`troubleshooting_errors`.

CORS (cors)
-----------

Cross-Origin Resource Sharing settings for the API. Configure in YAML.

.. config-table:: qcfractal.config.CORSconfig

Example:

.. code-block:: yaml

  cors:
    enabled: true
    origins: ["https://example.org", "http://localhost:3000"]
    supports_credentials: true
    headers: ["Content-Type", "Authorization"]
    methods: ["GET", "POST", "OPTIONS"]

S3 external files (s3)
----------------------

Settings for storing large external files in S3-compatible storage. Environment variable prefix: ``QCF_S3__``.

.. config-table:: qcfractal.config.S3Config

``bucket_map`` maps each logical file type to a bucket name:

.. config-table:: qcfractal.config.S3BucketMap

.. note::

  Bucket names must be valid S3 bucket names: 3-63 characters, lowercase letters, digits and
  hyphens only, ending in a letter or digit. Underscores are **not** allowed, so a value like
  ``dataset_attachment`` will be rejected at startup.

If ``enabled: true`` you must specify ``endpoint_url``, ``access_key_id``, and ``secret_access_key``.

Example:

.. code-block:: yaml

  s3:
    enabled: true
    endpoint_url: https://s3.us-west-2.amazonaws.com
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    verify: true
    bucket_map:
      dataset_attachment: qcarchive-attachments
      project_attachment: qcarchive-projects

Minimal and full configuration examples
---------------------------------------

Minimal (sensible defaults; secrets generated by ``qcfractal-server init-config``):

.. code-block:: yaml

  base_folder: /srv/qcfractal
  name: QCFractal Server
  enable_security: true
  allow_unauthenticated_read: true

  database:
    own: true
    username: qcfractal
    password: "<generated>"

  api:
    host: 0.0.0.0
    port: 7777
    secret_key: "<generated>"
    jwt_secret_key: "<generated>"

  api_limits: {}
  cors: {}
  auto_reset: {}

Full skeleton (all options with defaults; adjust as needed):

.. code-block:: yaml

  base_folder: /srv/qcfractal
  name: QCFractal Server
  enable_security: true
  allow_unauthenticated_read: true
  strict_compute_tags: false
  logfile: null
  loglevel: INFO
  hide_internal_errors: true
  service_frequency: 60
  max_active_services: 20
  heartbeat_frequency: 1800
  heartbeat_frequency_jitter: 0.1
  heartbeat_max_missed: 5
  log_access: false
  access_log_keep: 0
  internal_job_processes: 1
  internal_job_keep: 0
  maxmind_license_key: null
  geoip2_dir: geoip2
  geoip2_filename: GeoLite2-City.mmdb
  homepage_redirect_url: null
  homepage_directory: null
  upload_directory: null
  temporary_dir: null

  database:
    full_uri: null
    host: localhost
    port: 5432
    database_name: qcfractal_default
    username: qcfractal
    password: "<secret>"
    query: {}
    own: true
    data_directory: postgres
    logfile: qcfractal_database.log
    echo_sql: false
    pg_tool_dir: null
    pool_size: 5
    maintenance_db: postgres

  api:
    num_threads_per_worker: 4
    worker_timeout: 120
    host: localhost
    port: 7777
    secret_key: "<secret>"
    jwt_secret_key: "<secret>"
    jwt_access_token_expires: 3600
    jwt_refresh_token_expires: 86400
    user_session_max_age: 86400
    user_session_cookie_name: qcf_session
    user_session_cookie_domain: null
    user_session_cookie_samesite: null
    user_session_cookie_partitioned: false
    user_session_cookie_secure: false
    user_session_cookie_httponly: false
    extra_flask_options: null
    extra_waitress_options: null

  api_limits:
    get_records: 1000
    add_records: 500
    get_dataset_entries: 2000
    get_molecules: 1000
    add_molecules: 1000
    get_managers: 1000
    manager_tasks_claim: 200
    manager_tasks_return: 10
    get_access_logs: 1000
    get_error_logs: 100
    get_internal_jobs: 1000

  cors:
    enabled: false
    origins: []
    supports_credentials: false
    headers: []
    methods: []

  auto_reset:
    enabled: false
    unknown_error: 2
    compute_lost: 5
    random_error: 5

  s3:
    enabled: false
    verify: true
    passthrough: false
    endpoint_url: null
    access_key_id: null
    secret_access_key: null
    auto_create_buckets: false
    bucket_map:
      dataset_attachment: dataset-attachments
      project_attachment: project-attachments

Environment variable examples
-----------------------------

- Override the base folder and database host on the fly:

  .. code-block:: bash

     export QCF_BASE_FOLDER=/srv/qcfractal
     export QCF_DATABASE__HOST=db.internal
     qcfractal-server start --config server.yaml

- Bind the API to a different port and enable verbose logging:

  .. code-block:: bash

     export QCF_API__PORT=8888
     export QCF_LOGLEVEL=DEBUG
     qcfractal-server start --config server.yaml

Notes on path handling
----------------------

- Paths that are not absolute are resolved relative to ``base_folder``.
- If a path option is ``null`` and a default is described as ``[base_folder]/...``, QCFractal will apply that default at runtime.
- ``temporary_dir`` is created automatically if it does not exist.


.. _server_configuration_reference:

Generated reference
-------------------

The sections above are the documentation for these options - they explain what the
options are for, how they interact, and what sensible values look like. What follows is
generated directly from the pydantic models in ``qcfractal/qcfractal/config.py``, and is
here as a cross-check: it is guaranteed to list every option that actually exists, with
its real type and default.

Where the two disagree, this section is right about *what* the code accepts and the prose
above is right about *why*.

.. note::

  A few defaults are shown here as ``None`` but are filled in at runtime - ``geoip2_dir``,
  ``temporary_dir``, the database ``data_directory`` and ``logfile``. The prose above gives
  the effective values (``[base_folder]/...``). Likewise, options described above as
  accepting duration strings appear here as ``int``, since that is the type they are
  converted to.

.. autoclass:: qcfractal.config.FractalConfig
   :exclude-members: settings_customise_sources, __init__

.. autoclass:: qcfractal.config.DatabaseConfig

.. autoclass:: qcfractal.config.WebAPIConfig

.. autoclass:: qcfractal.config.APILimitConfig

.. autoclass:: qcfractal.config.AutoResetConfig

.. autoclass:: qcfractal.config.CORSconfig

.. autoclass:: qcfractal.config.S3Config

.. autoclass:: qcfractal.config.S3BucketMap

