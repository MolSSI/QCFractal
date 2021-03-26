"""
Tests the output_store subsocket
"""

import pytest
import qcfractal.interface as ptl
from qcfractal.interface.models import KVStore, ObjectId


@pytest.mark.parametrize("compression", ptl.models.CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_kvstore_basic_str(storage_socket, compression, compression_level):
    """
    Tests retrieving old data from KVStore
    """

    input_str = "This is some input " * 20
    kv = KVStore.compress(input_str, compression, compression_level)
    added_id = storage_socket.output_store.add([kv])[0]

    r = storage_socket.output_store.get([added_id], missing_ok=False)
    assert len(r) == 1
    kv = KVStore(**r[0])

    assert ObjectId(kv.id) == added_id
    assert kv.compression == compression
    assert kv.get_string() == input_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not ptl.models.CompressionEnum.none:
        assert kv.compression_level == compression_level


@pytest.mark.parametrize("compression", ptl.models.CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_kvstore_basic_json(storage_socket, compression, compression_level):
    """
    Tests retrieving old data from KVStore
    """

    input_dict = {str(k): "This is some input " * k for k in range(5)}
    kv = KVStore.compress(input_dict, compression, compression_level)
    added_id = storage_socket.output_store.add([kv])[0]

    r = storage_socket.output_store.get([added_id], missing_ok=False)
    assert len(r) == 1
    kv = KVStore(**r[0])

    assert ObjectId(kv.id) == added_id
    assert kv.compression == compression
    assert kv.get_json() == input_dict

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not ptl.models.CompressionEnum.none:
        assert kv.compression_level == compression_level
