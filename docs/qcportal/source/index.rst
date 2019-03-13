========
QCPortal
========

*QCPortal is a distributed compute and archival platform for quantum chemistry.*


Scaling
-------

QCPortal is aimed at a single user on a laptop up to large multi-PI groups on
dozens of different supercomputers. QCPortal provides a central location to
marshal and distribute data or computation. QCPortal can switch between a
variety of computational queue backends such as:

- `Dask <http://dask.pydata.org/en/latest/docs.html>`_ - A graph-based workflow engine for laptops and small clusters.
- `Fireworks <https://materialsproject.github.io/fireworks/>`_ - A asynchronous Mongo-based distributed queuing system.
- `Parsl <http://parsl-project.org>`_ - High-performance workflows.

Additional backends such as BOINC, Radical Pilot, and Balsam are under active
consideration.

========

Index
-----


**Getting Started**

* :doc:`install`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   install.rst

**Client**

Portal is the primary user interface to a Fractal server instance.

* :doc:`client`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Portal Documentation

   client.rst

**Collections**

Portal is the primary user interface to a Fractal server instance.

* :doc:`collections`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Portal Documentation

   collections.rst

