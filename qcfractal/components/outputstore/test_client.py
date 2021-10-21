"""
Tests the keywords subsocket
"""

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.portal.components.outputstore import CompressionEnum, OutputStore
from qcfractal.portal import PortalClient
from qcfractal.portal.client import PortalRequestError


@pytest.mark.parametrize("compression", CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_outputs_client_basic_str(
    storage_socket: SQLAlchemySocket, snowflake_client: PortalClient, compression, compression_level
):

    # Add via the storage socket - the client doesn't allow for adding outputs
    input_str = "This is some input " * 20
    kv = OutputStore.compress(input_str, compression, compression_level)

    # Add both as the OutputStore and as the plain str
    added_ids = storage_socket.outputstore.add([kv, input_str])

    # Now get via the client
    r = snowflake_client._get_outputs(added_ids, missing_ok=False)
    assert len(r) == 2

    assert r[0].id == added_ids[0]
    assert r[0].compression == compression
    assert r[0].get_string() == input_str

    assert r[1].id == added_ids[1]
    assert r[1].get_string() == input_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not CompressionEnum.none:
        assert r[0].compression_level == compression_level

    # get a single id
    r = snowflake_client._get_outputs(added_ids[0])
    assert isinstance(r, OutputStore)


def test_outputs_client_get_nonexist(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
    input_dict = {str(k): "This is some input " * k for k in range(5)}
    added_ids = storage_socket.outputstore.add([input_dict, input_dict])

    out = snowflake_client._get_outputs([added_ids[0], 999, added_ids[1]], missing_ok=True)
    assert out[0].id == added_ids[0]
    assert out[1] is None
    assert out[2].id == added_ids[1]

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(PortalRequestError, match=r"Could not find all requested records"):
        snowflake_client._get_outputs([added_ids[0], 999], missing_ok=False)


def test_outputs_client_get_empty(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
    input_dict = {str(k): "This is some input " * k for k in range(5)}
    storage_socket.outputstore.add([input_dict, input_dict])

    out = snowflake_client._get_outputs([])
    assert out == []
