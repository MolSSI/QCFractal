========
REST API
========

The items in this list document the REST API calls which can be made against the server,
this includes both the Body and the Responses for the various GET, POST, and PUT calls.

The entries are organized such that the API is presented first, separated by objects.
The last group of entries are common models which are *parts* of the API Bodies and
Responses (like Metadata), but occur many times in the normal calls.


--------
KV Store
--------

.. autoclass:: qcportal.models.rest_models.KVStoreGETBody

.. autoclass:: qcportal.models.rest_models.KVStoreGETResponse

--------
Molecule
--------

.. autoclass:: qcportal.models.rest_models.MoleculeGETBody

.. autoclass:: qcportal.models.rest_models.MoleculeGETResponse

.. autoclass:: qcportal.models.rest_models.MoleculePOSTBody

.. autoclass:: qcportal.models.rest_models.MoleculePOSTResponse

--------
Keywords
--------

.. autoclass:: qcportal.models.rest_models.KeywordGETBody

.. autoclass:: qcportal.models.rest_models.KeywordGETResponse

.. autoclass:: qcportal.models.rest_models.KeywordPOSTBody

.. autoclass:: qcportal.models.rest_models.KeywordPOSTResponse

-----------
Collections
-----------

.. autoclass:: qcportal.models.rest_models.CollectionGETBody

.. autoclass:: qcportal.models.rest_models.CollectionGETResponse

.. autoclass:: qcportal.models.rest_models.CollectionPOSTBody

.. autoclass:: qcportal.models.rest_models.CollectionPOSTResponse

------
Result
------

.. autoclass:: qcportal.models.rest_models.ResultGETBody

.. autoclass:: qcportal.models.rest_models.ResultGETResponse

----------
Procedures
----------

.. autoclass:: qcportal.models.rest_models.ProcedureGETBody

.. autoclass:: qcportal.models.rest_models.ProcedureGETResponse

----------
Task Queue
----------

.. autoclass:: qcportal.models.rest_models.TaskQueueGETBody

.. autoclass:: qcportal.models.rest_models.TaskQueueGETResponse

.. autoclass:: qcportal.models.rest_models.TaskQueuePOSTBody

.. autoclass:: qcportal.models.rest_models.TaskQueuePOSTResponse

.. autoclass:: qcportal.models.rest_models.TaskQueuePUTBody

.. autoclass:: qcportal.models.rest_models.TaskQueuePUTResponse

-------------
Service Queue
-------------

.. autoclass:: qcportal.models.rest_models.ServiceQueueGETBody

.. autoclass:: qcportal.models.rest_models.ServiceQueueGETResponse

.. autoclass:: qcportal.models.rest_models.ServiceQueuePOSTBody

.. autoclass:: qcportal.models.rest_models.ServiceQueuePOSTResponse

.. autoclass:: qcportal.models.rest_models.ServiceQueuePUTBody

.. autoclass:: qcportal.models.rest_models.ServiceQueuePUTResponse

-------------
Queue Manager
-------------

.. autoclass:: qcportal.models.rest_models.QueueManagerGETBody

.. autoclass:: qcportal.models.rest_models.QueueManagerGETResponse

.. autoclass:: qcportal.models.rest_models.QueueManagerPOSTBody

.. autoclass:: qcportal.models.rest_models.QueueManagerPOSTResponse

.. autoclass:: qcportal.models.rest_models.QueueManagerPUTBody

.. autoclass:: qcportal.models.rest_models.QueueManagerPUTResponse

----------------------
Common REST Components
----------------------

These are NOT complete Body or Responses to the REST API, but common fragments which
make up things like the Metadata or the Data fields.

.. autoclass:: qcportal.models.rest_models.EmptyMeta

.. autoclass:: qcportal.models.rest_models.ResponseMeta

.. autoclass:: qcportal.models.rest_models.ResponseGETMeta

.. autoclass:: qcportal.models.rest_models.ResponsePOSTMeta

.. autoclass:: qcportal.models.rest_models.QueryMeta

.. autoclass:: qcportal.models.rest_models.QueryMetaProjection

.. autoclass:: qcportal.models.rest_models.QueueManagerMeta