import pytest

from . import portal


def test_kvstore_model_fail():
    # data is a string, but compression is not none
    with pytest.raises(ValueError, match=r"Compression is set, but input is a"):
        portal.models.KVStore(**{"data": "123", "compression": "bzip2"})

    # data is a dict, but compression is not none
    with pytest.raises(ValueError, match=r"Compression is set, but input is a"):
        portal.models.KVStore(**{"data": {"123": 123}, "compression": "bzip2"})

    # data is a string, but compression level is not 0
    with pytest.raises(ValueError, match=r"Compression level is set, but input is a"):
        portal.models.KVStore(**{"data": "123", "compression_level": 1})

    # data is a string, but compression level is not 0
    with pytest.raises(ValueError, match=r"Compression level is set, but input is a"):
        portal.models.KVStore(**{"data": {"123": 123}, "compression_level": 1})
