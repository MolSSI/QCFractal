QCFractal Snowflake
===================

The QCFractal Snowflake is a self-contained server and :ref:`compute manager <glossary_manager>`.
The snowflake provides a way to experiment or test QCArchive without needing to set up a standalone server.

There are a few caveats to using the snowflake

* The database is stored in a temporary directory and deleted when the snowflake is destructed
* By default, security is not enabled
* You are generally limited to a couple compute workers, and they run on the same computer the server is
  running on
* Programs for running the computations (like Psi4) must be available on the computer (and in the environment you are
  running the snowflake in)
* Postgres software must be available

While there are ways around the above limitations, they are generally against the spirit of using the snowflake,
and you are probably better off setting up a complete server instead.

.. warning::

  When a snowflake is destructed, the database is destroyed and all data is lost. It is not meant to permanently
  store data.

Using the Snowflake
-------------------

The snowflake can be used by constructing a FractalSnowflake. From there, a client can be obtained and
used as if it were any other client.

.. code-block:: py3

  >>> from qcfractal.snowflake import FractalSnowflake
  >>> snowflake = FractalSnowflake()
  >>> client = snowflake.client()

  >>> # Add computations, create datasets, do whatever you want
  >>> client.add_singlepoints(mol, 'psi4', 'energy', 'hf', 'sto-3g')




Snowflake API
-------------

.. autoclass:: qcfractal.snowflake.FractalSnowflake