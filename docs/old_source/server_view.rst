Server-side Dataset Views
=========================

.. note::
    This is an experimental feature.

HDF5 views of Datasets may be stored on the server to improve query performance.
To use views, first specify a path to store views in the :class:`qcfractal.config.ViewSettings`.

Next, generate a view for the collection(s) of interest:

.. code-block:: python

    import qcfractal.interface as ptl
    ds = ptl.get_collection("ReactionDataset", "S22")

    # Note the server will look for views in the directory specified above,
    # named {collection_id}.hdf5
    view = ptl.collections.HDF5View(viewpath / f"{ds.data.id}.hdf5")
    view.write(ds)

Finally, mark the collection as supporting views:

.. code-block:: python

    # Update the dataset to indicate a view is available
    ds.__dict__["view_available"] = True
    ds.save()

    # Optionally, you may add a download URL for the view
    ds.__dict__["view_url"] = "https://someserver.com/view.hdf5"
    ds.save()
