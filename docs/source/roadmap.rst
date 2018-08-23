QCArchive Roadmap
=================

Mission Statement
-----------------

The QCArchive project sets out to answer the fundamental question of "How do
we compile, aggregate, query, and share quantum chemistry data to accelerate
the understanding of new method performance, fitting of novel force fields, and
supporting the incredible data needs of machine learning for computational
molecular science?"

The QCArchive project is made up of three primary tools:
 - `QCSchema <https://github.com/MolSSI/QC_JSON_Schema>`_ - A key/value schema for quantum chemistry.
 - `QCEngine <https://github.com/MolSSI/QCEngine>`_ - A computational middleware to provide IO to a variety of quantum chemistry programs.
 - `QCFractal <https://github.com/MolSSI/QCFractal>`_ - A computational middleware to provide IO to a variety of quantum chemistry programs.

The tools aim to provide an environment to facilitate the following goals:
- Assist the computational molecular sciences (CMS) community in becoming more data-driven and statistical in nature.
- Provide community-driven, multi-use quantum chemistry databases that the CMS community can access free of charge.
- Support QC data requirements of the Open Force Field Consortium.
- Support the upcoming machine learning initiatives.
- Provide all current major QC databases in a universal format (S22, GMTMKN55).
- Provide container-based programs whereby community members can contribute computational time toward chosen portions of the database.
- Work directly with popular database manipulation front ends to provide their data backend requirements.
- To the extent possible, build tools in reusable pieces that can be reappropriated for general use.

An initial design document can be found `here <https://docs.google.com/document/d/1jG9BGIaDswkm03kiNdAGUE4FUDFGtYp_axV5J-Dg8OM/edit?usp=sharing>`_.

===========


Timeline: 2019-01-15 - Future
-----------------------------

The roadmap is in progress.

===========


Timeline: 2018-08-23 to 2019-01-15
----------------------------------

This roadmap lays out the first six months of development and initial targets
at the end of 2018 and likely continuing into early 2019. The initial targets
primarily focus on the initial capabilities of the project and building out a
core that can be expanded upon.


Use Case: OpenFF Torsion Scans
++++++++++++++++++++++++++++++

Collaborators: `Open Force Field Consortium <http://openforcefield.org>`_


.. image:: media/openff_torsion_workflow.jpg
   :width: 800px
   :alt: OpenFF Torsion Workflow example
   :align: center

Document: `UC: Torsion Scans <https://docs.google.com/document/d/1OmIeMISfrxBVyVXYYj5jn2eVzaPRbuZbNtRPgenFOrQ/edit?usp=sharing>`_.

Use Case: Reference Databases
+++++++++++++++++++++++++++++

Collaborators: `Sherrill Group (Georgia Tech) <http://vergil.chemistry.gatech.edu>`_

Document: `UC: Reference Databases <https://docs.google.com/document/d/12_X60PFPZmnj-Ak9AEGW_VytzZ0LtJrAMxZnSFu0aJo/edit?usp=sharing>`_.

Reference datasets form the core of understanding the performance of more
approximate methods in quantum chemistry.  These reference datasets are
appearing with increased frequency and are of an ever-increasing size to match
the corresponding needs of the plethora of new theories and ideas applied to
increasing chemical diversity requirements.

Reference datasets are often found in an assortment of CSV, PDF, and raw ASCII
files. The diverse formats make aggregating (or even using) these benchmark sets quite the
onerous task when they are core to our fundamental understanding of more
popular and approximate methods. To our knowledge, aggregating these quantum
chemistry references has only been tried rarely and without to significant
effect. Please examine the `BegDB <http://www.begdb.com>`_ and the `BFDb
<http://vergil.chemistry.gatech.edu/active_bfdb/bfdb/cgi-bin/bfdb.py>`_
projects for current examples.

The QCArchive project will facilitate the creation of these datasets by
constructing a framework to make th computation and storage of datasets as
effortless of an experience as possible.  These tools can also be used to
distribute reference datasets. Finally, these tools will contain many data best
practices such as computational provenance and reference tracking to enhance
the reproducibility of these datasets.

Release Schedule
++++++++++++++++
To facilitate the rapid evolution of features and release will be created on
the first of every months.

**Release 2018-09-01**

- *Primary focus*: Rapid evolution of feature set and overall structure
- Initial service portal-side interface handlers.
- Server logging overhaul.
- Initial PyPi and Conda alpha release.
- Handles long-term queues in the database layer with additional hooks to trigger upon job completion.


**Release 2018-10-01**

- *Primary focus*: Working examples that facilitate use cases.
- Enhancement of the Database class to correclty track citations, reference data, subsets, etc.
- Enhancement of the service classes to include better searching, data handling, and error messages.

**Release 2018-11-01**

- *Primary focus*: Backend optimization and cleanup.
- PyPi and Conda beta release.

**Release 2018-12-01**

- *Primary focus*: Security, database optimization, and documentation.

**Release 2019-01-15**

 - *Primary focus*: Remaining issues for a first release.
