Common Tasks
============

Check computation progress
++++++++++++++++++++++++++

The :meth:`FractalClient.query_tasks <qcportal.FractalClient.query_tasks>` method returns information on active tasks.

.. code-block:: python

    client = qcportal.FractalClient(...)
    client.query_tasks(status='WAITING')  # return tasks in queue
    client.query_tasks(status='RUNNING')  # return running tasks

The ``tag`` and ``manager`` fields may be used to find only your tasks.


Find Errors and Restart Jobs
****************************

Some jobs may fail, and will end up in the ``ERROR`` state.

.. code-block:: python

    myerrs = client.query_tasks(status='ERROR')  # return errored tasks

Errored jobs may be inspected:

.. code-block:: python

    record = client.query_results(mye[0].base_result.id)[0]
    print(record.stdout)  # Standard output
    print(record.stderr)  # Standard error
    print(client.query_kvstore(record.error)[0])  # Error message

and restarted:

.. code-block:: python

    res = client.modify_tasks("restart", [e.base_result.id for e in myerrs])
    print(res.n_updated)

Delete a column
+++++++++++++++

You may wish to remove a model from a :class:`Dataset <qcportal.collections.Dataset>`
or :class:`ReactionDataset <qcportal.collections.ReactionDataset>`.
Models are stored in ``Dataset.data.history``, and can be removed from there:

.. code-block:: python

    print(ds.data.history)
    # Output e.g.
    # {('energy', 'psi4', 'b3lyp', 'def2-svp', 'scf_default'),
    # ('energy', 'psi4', 'hf', 'sto-3g', 'scf_default'),
    # ('energy', 'psi4', 'lda', 'sto-3g', 'scf_default')}

    ds.data.history.remove(('energy', 'psi4', 'lda', 'sto-3g', 'scf_default'))
    ds.save()

    ds = client.get_collection(...)
    print(ds.data.history)
    # Output e.g.
    # {('energy', 'psi4', 'b3lyp', 'def2-svp', 'scf_default'),
    # ('energy', 'psi4', 'hf', 'sto-3g', 'scf_default')}

See also
++++++++

Many examples of interacting with collections hosted on QCArchive are provided on the `QCArchive Examples <https://qcarchive.molssi.org/examples/>`_ page.