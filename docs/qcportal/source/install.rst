Install QCPortal
=================

You can install ``qcportal`` with ``conda`` or with ``pip``.

Conda
-----

You can install qcportal using `conda <https://www.anaconda.com/download/>`_:

.. code-block:: console

    >>> conda install qcportal -c conda-forge

This installs QCPortal and its dependencies. The qcportal package is maintained on the
`conda-forge channel <https://conda-forge.github.io/>`_.


Pip
---

you can also install QCPortal using ``pip``:

.. code-block:: console

   >>> pip install qcportal


Test the Installation
---------------------

You can test to make sure that QCPortal is installed correctly by first installing ``pytest``.

From ``conda``:

.. code-block:: console

   >>> conda install pytest -c conda-forge

From ``pip``:

.. code-block:: console

   >>> pip install pytest

Then, run the following command:

.. code-block::

   >>> pytest --pyargs qcportal


Developing from Source
----------------------

The QCPortal package is part of the QCFractal package and is the ``qcfractal.interface`` folder. If you are a developer
and want to make contributions Portal, you can access the source code from
`github <https://github.com/molssi/qcfractal>`_ and the aforementioned folder.
