Exploring the Datasets
======================

The :class:`Dataset <qcportal.collections.Dataset>` collection represents a table whose rows correspond to
:class:`Molecules <qcportal.models.Molecule>` and whose columns correspond to properties.
Columns may either result from QCFractal-based calculations or be contributed from outside sources.
For example, the QM9 dataset in QCArchive contains small organic molecules with up to 9 heavy atoms, and includes
the original reported PBE0 energies, as well as energies calculated with a variety of other density functionals and basis sets.

The existing :class:`Datasets <qcportal.collections.Dataset>` can be listed with
:meth:`FractalClient.list_collections("Dataset") <qcportal.FractalClient.list_collections>`
and obtained using :meth:`FractalClient.get_collection("Dataset", name) <qcportal.FractalClient.get_collection>`.

Querying the Data
-----------------

Available result specifications (method, basis set, program, keyword, driver combinations) in a
:class:`Dataset <qcportal.collections.Dataset>` may be listed with the :meth:`list_values <qcportal.collections.Dataset.list_values>`
method. Values are queried with the :meth:`get_values <qcportal.collections.Dataset.get_values>` method. For results computed
using QCFractal, the underlying :class:`Records <qcportal.models.ResultRecord>`
are retrieved with :meth:`get_records <qcportal.collections.Dataset.get_records>`.

For further details about how to query :class:`Datasets <qcportal.collections.Dataset>`
see the `QCArchive examples <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/getting_started.html>`_.

Statistics and Visualization
----------------------------

Statistical operations on :class:`Datasets <qcportal.collections.Dataset>` may be performed using
:meth:`statistics <qcportal.collections.Dataset.statistics>` command
and plotted using the :meth:`visualize <qcportal.collections.Dataset.visualize>` command.

For examples of visualizing :class:`Datasets <qcportal.collections.Dataset>`,
see the `QCArchive examples <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/getting_started.html>`_.

Creating the Datasets
---------------------

Construct an empty :class:`Dataset <qcportal.collections.Dataset>`:

.. code-block:: python

    >>> import qcportal as ptl
    >>> client = plt.FractalClient()  # add server and login information as needed
    >>> ds = ptl.collections.Dataset("name", client=client)

The primary index of a :class:`Dataset <qcportal.collections.Dataset>` is a list of :class:`Molecules <qcportal.models.Molecule>`.
:class:`Molecules <qcportal.models.Molecule>` can be added to a :class:`Dataset <qcportal.collections.Dataset>` with
:meth:`add_entry <qcportal.collections.Dataset.add_entry>`:

.. code-block:: python

    >>> ds.add_entry(name, molecule)

Once all :class:`Molecules <qcportal.models.Molecule>` are added,
the changes can be committed to the server with :meth:`save <qcportal.collections.Dataset.save>` method.
Note that this requires `write permissions <http://docs.qcarchive.molssi.org/projects/qcfractal/en/stable/server_user.html#user-permissions>`_.

.. code-block:: python

   >>> ds.save()


.. _dataset-computing:

Computational Tasks
-------------------

Computations on the molecules within the :class:`Datasets <qcportal.collections.Dataset>` can be performed using the
:meth:`compute <qcportal.collections.Dataset.compute>` command.
If the results of the requested computation already exist in the :class:`Dataset <qcportal.collections.Dataset>`, 
they will be reused to avoid recomputation. Note that for perfoming computations,
`compute permissions <http://docs.qcarchive.molssi.org/projects/qcfractal/en/stable/server_user.html#user-permissions>`_ is required.

.. code-block:: python

   >>> models = {('b3lyp', 'def2-svp'), ('mp2', 'cc-pVDZ')}

   >>> for method, basis in models:
   >>>     print(method, basis)
   >>>     spec = {"program": "psi4",
   >>>         "method": method,
   >>>         "basis": basis,
   >>>         "keywords": "my_keywords",
   >>>         "tag": "mgwtfm"}
   >>>     ds.compute(**spec)


.. note::

    A default quantum chemical program and a set of computational 
    keywords can be specified for a :class:`Dataset <qcportal.collections.Dataset>`.
    These default values will be used in the :meth:`compute <qcportal.collections.Dataset.compute>`,
    :meth:`get_values <qcportal.collections.Dataset.get_values>`, and
    :meth:`get_records <qcportal.collections.Dataset.get_records>` methods.

    .. code-block:: python

        >>> ds.set_default_program("psi4")

        >>> keywords = ptl.models.KeywordSet(values={'maxiter': 1000,
        >>>                                         'e_convergence': 8,
        >>>                                         'guess': 'sad',
        >>>                                         'scf_type': 'df'})

        >>> ds.add_keywords("my_keywords", "psi4", keywords, default=True)

        >>> ds.save()


API
---

.. autoclass:: qcportal.collections.Dataset
    :members:
