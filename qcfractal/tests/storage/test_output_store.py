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
    Tests storing/retrieving plain string data in KVStore
    """

    input_str = "This is some input " * 20
    kv = KVStore.compress(input_str, compression, compression_level)

    # Add both as the KVStore and as the plain str
    added_ids = storage_socket.output_store.add([kv, input_str])

    r = storage_socket.output_store.get(added_ids, missing_ok=False)
    assert len(r) == 2
    kv1 = KVStore(**r[0])
    kv2 = KVStore(**r[1])

    assert ObjectId(kv1.id) == added_ids[0]
    assert kv1.compression == compression
    assert kv1.get_string() == input_str

    assert ObjectId(kv2.id) == added_ids[1]
    assert kv2.get_string() == input_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not ptl.models.CompressionEnum.none:
        assert kv1.compression_level == compression_level


@pytest.mark.parametrize("compression", ptl.models.CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_kvstore_basic_json(storage_socket, compression, compression_level):
    """
    Tests storing/retrieving dict/json data in KVStore
    """

    input_dict = {str(k): "This is some input " * k for k in range(5)}
    kv = KVStore.compress(input_dict, compression, compression_level)

    # Add both as the KVStore and as the plain dict
    added_ids = storage_socket.output_store.add([kv, input_dict])

    r = storage_socket.output_store.get(added_ids, missing_ok=False)
    assert len(r) == 2
    kv1 = KVStore(**r[0])
    kv2 = KVStore(**r[1])

    assert ObjectId(kv1.id) == added_ids[0]
    assert kv1.compression == compression
    assert kv1.get_json() == input_dict

    assert ObjectId(kv2.id) == added_ids[1]
    assert kv2.get_json() == input_dict

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not ptl.models.CompressionEnum.none:
        assert kv1.compression_level == compression_level


@pytest.mark.parametrize("compression", ptl.models.CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_kvstore_replace(storage_socket, compression, compression_level):
    """
    Tests replacing data in KVStore
    """

    input_str = "This is some input " * 20

    # Add twice
    added_ids = storage_socket.output_store.add([input_str, input_str])

    new_str = "Some new stuff just came in" * 10
    new_kv = KVStore.compress(new_str, compression, compression_level)

    new_id_1 = storage_socket.output_store.replace(added_ids[0], new_kv)
    new_id_2 = storage_socket.output_store.replace(added_ids[1], new_str)
    r = storage_socket.output_store.get([new_id_1, new_id_2], missing_ok=False)

    kv1 = KVStore(**r[0])
    kv2 = KVStore(**r[1])

    assert ObjectId(kv1.id) == new_id_1
    assert kv1.compression == compression
    assert kv1.get_string() == new_str

    assert ObjectId(kv2.id) == new_id_2
    assert kv2.get_string() == new_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not ptl.models.CompressionEnum.none:
        assert kv1.compression_level == compression_level

    # Old ones should have been deleted
    r = storage_socket.output_store.get(added_ids, missing_ok=True)
    assert all(x is None for x in r)


def test_kvstore_replace_new(storage_socket):
    """
    Tests replacing data in KVStore when the given ID is None
    """

    input_str = "This is some input " * 20

    added_id = storage_socket.output_store.replace(None, input_str)

    r = storage_socket.output_store.get([added_id], missing_ok=False)

    kv1 = KVStore(**r[0])

    assert ObjectId(kv1.id) == added_id
    assert kv1.get_string() == input_str


def test_kvstore_replace_none(storage_socket):
    """
    Tests replacing data in KVStore when the new output is None
    """

    input_str = "This is some input " * 20

    added_id = storage_socket.output_store.add([input_str])[0]

    # Replace with nothing
    new_id_1 = storage_socket.output_store.replace(added_id, None)

    assert new_id_1 is None

    # Old ones should have been deleted
    r = storage_socket.output_store.get([added_id], missing_ok=True)
    assert all(x is None for x in r)


def test_kvstore_replace_badid(storage_socket):
    """
    Tests replacing data in KVStore when the given ID does not exist

    This is allowed
    """

    input_str = "This is some input " * 20

    added_id = storage_socket.output_store.replace(1234, input_str)
    r = storage_socket.output_store.get([added_id], missing_ok=False)
    kv1 = KVStore(**r[0])

    assert ObjectId(kv1.id) == added_id
    assert kv1.get_string() == input_str


@pytest.mark.parametrize("compression", ptl.models.CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_kvstore_append(storage_socket, compression, compression_level):
    """
    Tests appending data in KVStore
    """

    input_str = "This is some input " * 20
    kv = KVStore.compress(input_str, compression, compression_level)

    added_id = storage_socket.output_store.add([kv])[0]

    app_str = "This needs to be appended" * 10
    app_id_1 = storage_socket.output_store.append(added_id, app_str)

    assert app_id_1 == added_id
    r = storage_socket.output_store.get([app_id_1], missing_ok=False)

    kv1 = KVStore(**r[0])

    # Should not have changed compression
    assert ObjectId(kv1.id) == added_id
    assert kv1.compression == compression
    assert kv1.get_string() == input_str + app_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not ptl.models.CompressionEnum.none:
        assert kv1.compression_level == compression_level


def test_kvstore_append_new(storage_socket):
    """
    Tests appending data in KVStore when the original is given as None
    """

    input_str = "This is some input " * 20

    added_id = storage_socket.output_store.append(None, input_str)

    r = storage_socket.output_store.get([added_id], missing_ok=False)
    kv1 = KVStore(**r[0])

    assert ObjectId(kv1.id) == added_id
    assert kv1.get_string() == input_str


def test_kvstore_append_badid(storage_socket):
    """
    Tests appending data in KVStore when the original id does not exist

    This should raise an exception
    """

    input_str = "This is some input " * 20

    with pytest.raises(RuntimeError, match="Cannot append to output"):
        storage_socket.output_store.append(1234, input_str)
