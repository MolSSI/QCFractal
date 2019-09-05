Common Tasks
============

Check computation progress
++++++++++++++++++++++++++

.. note:: After PR #385

Find Errors
***********

Restart Jobs
************


Delete a column
+++++++++++++++

You may wish to remove a model from a ``Dataset`` or ``ReactionDataset``.
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

Many examples of interacting with collections hosted on QCArchive are provided on the `QCArchvive Examples <https://qcarchive.molssi.org/examples/>`_ page.