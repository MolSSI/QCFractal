"""
Tests the output_store subsocket
"""

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.exceptions import MissingDataError
from qcfractal.interface.models import CompressionEnum, OutputStore


@pytest.mark.parametrize("compression", CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_outputs_socket_basic_str(storage_socket: SQLAlchemySocket, compression, compression_level):
    """
    Tests storing/retrieving plain string data in OutputStore
    """

    input_str = "This is some input " * 20
    kv = OutputStore.compress(input_str, compression, compression_level)

    # Add both as the OutputStore and as the plain str
    added_ids = storage_socket.outputstore.add([kv, input_str])

    r = storage_socket.outputstore.get(added_ids, missing_ok=False)
    assert len(r) == 2
    kv1 = OutputStore(**r[0])
    kv2 = OutputStore(**r[1])

    assert kv1.id == added_ids[0]
    assert kv1.compression == compression
    assert kv1.get_string() == input_str

    assert kv2.id == added_ids[1]
    assert kv2.get_string() == input_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not CompressionEnum.none:
        assert kv1.compression_level == compression_level


@pytest.mark.parametrize("compression", CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_outputs_socket_basic_json(storage_socket: SQLAlchemySocket, compression, compression_level):
    """
    Tests storing/retrieving dict/json data in OutputStore
    """

    input_dict = {str(k): "This is some input " * k for k in range(5)}
    kv = OutputStore.compress(input_dict, compression, compression_level)

    # Add both as the OutputStore and as the plain dict
    added_ids = storage_socket.outputstore.add([kv, input_dict])

    r = storage_socket.outputstore.get(added_ids, missing_ok=False)
    assert len(r) == 2
    kv1 = OutputStore(**r[0])
    kv2 = OutputStore(**r[1])

    assert kv1.id == added_ids[0]
    assert kv1.compression == compression
    assert kv1.get_json() == input_dict

    assert kv2.id == added_ids[1]
    assert kv2.get_json() == input_dict

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not CompressionEnum.none:
        assert kv1.compression_level == compression_level

    # Test reverse order
    r = storage_socket.outputstore.get(list(reversed(added_ids)), missing_ok=False)
    assert len(r) == 2
    assert r[0]["id"] == added_ids[1]
    assert r[1]["id"] == added_ids[0]


def test_outputs_socket_get_nonexist(storage_socket: SQLAlchemySocket):
    input_dict = {str(k): "This is some input " * k for k in range(5)}
    added_ids = storage_socket.outputstore.add([input_dict, input_dict])

    out = storage_socket.outputstore.get([999, added_ids[0], 123, added_ids[1]], missing_ok=True)
    assert out[0] is None
    assert out[1]["id"] == added_ids[0]
    assert out[2] is None
    assert out[3]["id"] == added_ids[1]

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(MissingDataError, match=r"Could not find all requested records"):
        storage_socket.outputstore.get([999, added_ids[0], 123], missing_ok=False)


def test_outputs_socket_get_empty(storage_socket: SQLAlchemySocket):
    input_dict = {str(k): "This is some input " * k for k in range(5)}
    added_ids = storage_socket.outputstore.add([input_dict, input_dict])

    out = storage_socket.outputstore.get([])
    assert out == []


@pytest.mark.parametrize("compression", CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_outputs_socket_append(storage_socket: SQLAlchemySocket, compression, compression_level):
    """
    Tests appending data in OutputStore
    """

    input_str = "This is some input " * 20
    kv = OutputStore.compress(input_str, compression, compression_level)

    added_id = storage_socket.outputstore.add([kv])[0]

    app_str = "This needs to be appended" * 10
    app_id_1 = storage_socket.outputstore.append(added_id, app_str)

    assert app_id_1 == added_id
    r = storage_socket.outputstore.get([app_id_1], missing_ok=False)

    kv1 = OutputStore(**r[0])

    # Should not have changed compression
    assert kv1.id == added_id
    assert kv1.compression == compression
    assert kv1.get_string() == input_str + app_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not CompressionEnum.none:
        assert kv1.compression_level == compression_level


def test_outputs_socket_append_new(storage_socket):
    """
    Tests appending data in an output when the original is given as None
    """

    input_str = "This is some input " * 20

    added_id = storage_socket.outputstore.append(None, input_str)

    r = storage_socket.outputstore.get([added_id], missing_ok=False)
    kv1 = OutputStore(**r[0])

    assert kv1.id == added_id
    assert kv1.get_string() == input_str


def test_outputs_socket_append_badid(storage_socket):
    """
    Tests appending data in an output when the original id does not exist

    This should raise an exception
    """

    input_str = "This is some input " * 20

    with pytest.raises(MissingDataError, match="Cannot append to output"):
        storage_socket.outputstore.append(1234, input_str)
