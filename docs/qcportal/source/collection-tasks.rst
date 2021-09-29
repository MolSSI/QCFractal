Common Tasks
============

Check computation progress
++++++++++++++++++++++++++

The :meth:`FractalClient.query_tasks() <qcportal.FractalClient.query_tasks>` 
method returns information on active tasks.

.. code-block:: python

    >>> client = qcportal.FractalClient(...)
    >>> client.query_tasks(status='WAITING')  # return tasks in queue
    >>> client.query_tasks(status='RUNNING')  # return running tasks

The ``tag`` and ``manager`` fields can be used to find user-defined tasks.


Finding Errors and Restarting Jobs
**********************************

During each task's lifetime, errors might occur and the jobs
may end up in the ``ERROR`` state.

.. code-block:: python

    >>> myerrs = client.query_tasks(status='ERROR')  # return errored tasks

Failed jobs may be inspected as follows:

.. code-block:: python

    >>> record = client.query_results(mye[0].base_result.id)[0]
    >>> print(record.stdout)  # Standard output
    >>> print(record.stderr)  # Standard error
    >>> print(client.query_kvstore(record.error)[0])  # Error message

One can restart the failed tasks via the following steps:

.. code-block:: python

    >>> res = client.modify_tasks("restart", [e.base_result.id for e in myerrs])
    >>> print(res.n_updated)

Delete a column
+++++++++++++++

Models are stored in ``Dataset.data.history``,

.. code-block:: python

    >>> print(ds.data.history)
        # Example output
        # {('energy', 'psi4', 'b3lyp', 'def2-svp', 'scf_default'),
        # ('energy', 'psi4', 'hf', 'sto-3g', 'scf_default'),
        # ('energy', 'psi4', 'lda', 'sto-3g', 'scf_default')}

and can be removed from a
:class:`Dataset <qcportal.collections.Dataset>` or
:class:`ReactionDataset <qcportal.collections.ReactionDataset>` via:

.. code-block:: python

    >>> ds.data.history.remove(('energy', 'psi4', 'lda', 'sto-3g', 'scf_default'))
    >>> ds.save()

    >>> ds = client.get_collection(...)
    >>> print(ds.data.history)
        # Example output
        # {('energy', 'psi4', 'b3lyp', 'def2-svp', 'scf_default'),
        # ('energy', 'psi4', 'hf', 'sto-3g', 'scf_default')}

For further examples on how to interact with dataset collections hosted on 
QCArchive see the `QCArchive examples <https://qcarchive.molssi.org/examples/>`_.
