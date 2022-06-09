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

.. autopydantic_model:: qcportal.models.rest_models.KVStoreGETBody

.. autopydantic_model:: qcportal.models.rest_models.KVStoreGETResponse

--------
Molecule
--------

.. autopydantic_model:: qcportal.models.rest_models.MoleculeGETBody

.. autopydantic_model:: qcportal.models.rest_models.MoleculeGETResponse

.. autopydantic_model:: qcportal.models.rest_models.MoleculePOSTBody

.. autopydantic_model:: qcportal.models.rest_models.MoleculePOSTResponse

--------
Keywords
--------

.. autopydantic_model:: qcportal.models.rest_models.KeywordGETBody

.. autopydantic_model:: qcportal.models.rest_models.KeywordGETResponse

.. autopydantic_model:: qcportal.models.rest_models.KeywordPOSTBody

.. autopydantic_model:: qcportal.models.rest_models.KeywordPOSTResponse

-----------
Collections
-----------

.. autoclass:: qcportal.models.rest_models.CollectionGETBody

.. autopydantic_model:: qcportal.models.rest_models.CollectionGETResponse

.. autoclass:: qcportal.models.rest_models.CollectionPOSTBody

.. autopydantic_model:: qcportal.models.rest_models.CollectionPOSTResponse

------
Result
------

.. autoclass:: qcportal.models.rest_models.ResultGETBody

.. autopydantic_model:: qcportal.models.rest_models.ResultGETResponse

----------
Procedures
----------

.. autoclass:: qcportal.models.rest_models.ProcedureGETBody

.. autopydantic_model:: qcportal.models.rest_models.ProcedureGETResponse

----------
Task Queue
----------

.. autoclass:: qcportal.models.rest_models.TaskQueueGETBody

.. autopydantic_model:: qcportal.models.rest_models.TaskQueueGETResponse

.. autoclass:: qcportal.models.rest_models.TaskQueuePOSTBody

.. autopydantic_model:: qcportal.models.rest_models.TaskQueuePOSTResponse

.. autoclass:: qcportal.models.rest_models.TaskQueuePUTBody

.. autopydantic_model:: qcportal.models.rest_models.TaskQueuePUTResponse

-------------
Service Queue
-------------

.. autoclass:: qcportal.models.rest_models.ServiceQueueGETBody

.. autopydantic_model:: qcportal.models.rest_models.ServiceQueueGETResponse

.. autoclass:: qcportal.models.rest_models.ServiceQueuePOSTBody

.. autopydantic_model:: qcportal.models.rest_models.ServiceQueuePOSTResponse

.. autoclass:: qcportal.models.rest_models.ServiceQueuePUTBody

.. autopydantic_model:: qcportal.models.rest_models.ServiceQueuePUTResponse

-------------
Queue Manager
-------------

.. autopydantic_model:: qcportal.models.rest_models.QueueManagerGETBody

.. autopydantic_model:: qcportal.models.rest_models.QueueManagerGETResponse

.. autopydantic_model:: qcportal.models.rest_models.QueueManagerPOSTBody

.. autopydantic_model:: qcportal.models.rest_models.QueueManagerPOSTResponse

.. autopydantic_model:: qcportal.models.rest_models.QueueManagerPUTBody

.. autopydantic_model:: qcportal.models.rest_models.QueueManagerPUTResponse

----------------------
Common REST Components
----------------------

These are NOT complete Body or Responses to the REST API, but common fragments which
make up things like the Metadata or the Data fields.

.. autopydantic_model:: qcportal.models.rest_models.EmptyMeta

.. autopydantic_model:: qcportal.models.rest_models.ResponseMeta

.. autopydantic_model:: qcportal.models.rest_models.ResponseGETMeta

.. autopydantic_model:: qcportal.models.rest_models.ResponsePOSTMeta

.. autopydantic_model:: qcportal.models.rest_models.QueryMeta

.. does not exist in qcf .. autoclass:: qcportal.models.rest_models.QueryMetaProjection

.. autopydantic_model:: qcportal.models.rest_models.QueueManagerMeta
