Dataset
=======

Querying
--------

.. todo:: write this after get_results, get_values PR

Visualizing
-----------

Creating
--------

Construct an empty Dataset:

.. code-block:: python

    import qcportal as ptl
    client = plt.FractalClient()  # add server and login information as needed
    ds = ptl.collections.Dataset("name", client=client)

The primary index of a Dataset is a list of molecules. Molecules can be added to a Dataset with ``add_entry``:

.. code-block:: python

    ds.add_entry(name, molecule)

Once all molecules have been added, commit the changes on the server with ``save``.
Note that this requires `write permissions <http://docs.qcarchive.molssi.org/projects/qcfractal/en/stable/server_user.html#user-permissions>`_.

.. code-block:: python

   ds.save()


Computing
---------

Methods can be computed added to the Dataset using the ``compute`` command.
This command causes a calculation to be requested for every molecule in the Dataset.
Any calculations that have previously been done will be automatically added without recomputation.
Note that this requires `compute permissions <http://docs.qcarchive.molssi.org/projects/qcfractal/en/stable/server_user.html#user-permissions>`_.

.. code-block:: python

    models = {('b3lyp', 'def2-svp'), ('mp2', 'cc-pVDZ')}

    for method, basis in models:
        print(method, basis)
        spec = {"program": "psi4",
            "method": method,
            "basis": basis,
            "keywords": "my_keywords",
            "tag": "mgwtfm"}
        ds.compute(**spec)


.. note::

    You can set a default program and keyword set for a Dataset.
    These defaults will be used in compute and query calls.

    .. code-block:: python

        ds.set_default_program("psi4")

        keywords = ptl.models.KeywordSet(values={'maxiter': 1000,
                                                 'e_convergence': 8,
                                                 'guess': 'sad',
                                                 'scf_type': 'df'})
        ds.add_keywords("my_keywords", "psi4", keywords, default=True)

        ds.save()


API
---

.. autoclass:: qcfractal.interface.collections.Dataset
    :members:
