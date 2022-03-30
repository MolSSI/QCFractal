Detailed Manager Information
============================

This page documents all the internals of the :term:`Managers<Manager>` in depth and is **not** intended for the general
user nor should be required for setting up and running them. This page is for those who are interested in the inner
workings and detailed flow of the how the :term:`Manager` interacts with the :term:`Server`, the :term:`Adapter`,
the :term:`Scheduler`, and what it is abstracting away from the user.

Since this is intended to be a deep dive into mechanics, please let us know if something is missing or unclear and we
can expand this page as needed.

Manager Flowchart
-----------------

The Queue Manager's interactions with the Fractal Server, the Distributed Compute Engine, the physical Compute
Hardware, and the user are shown in the following diagram.

.. image:: media/QCFractalQueueManager.png
   :width: 800px
   :alt: Flowchart of what happens when a user starts a Queue Manager
   :align: center
