import json

import pytest

from .models import KeywordSet


@pytest.mark.parametrize(
    "data, hash_index",
    [
        # Simple checks
        ({"values": {"hi": 5}}, "53042da4ac1af059816008631e4589a0cd0c98c6"),
        ({"values": {"HI": 5}}, "53042da4ac1af059816008631e4589a0cd0c98c6"),
        # Check options combinations
        ({"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}}, "888837dec981e2f9a1ef2ef2d33db6d239df65da"),
        (
            {"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}, "lowercase": False},
            "08f6f37fec721b753096d71f7dbbccb734e8a696",
        ),
        (
            {"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}, "exact_floats": True},
            "691c86202af20868bba457514e63c83d0444142e",
        ),
        (
            {"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}, "exact_floats": True, "lowercase": False},
            "a3a6eea9edec3c2901a76fa8a41d4f9db9b3405f",
        ),
        # Check recursive
        ({"values": {"d1": {"D2": [1.0e-17, 5]}}}, "55c6f1c7e610ce379feaa8a97854ea61974e0d92"),
        (
            {"values": {"d1": {"D2": [1.0e-17, 5]}}, "exact_floats": True, "lowercase": False},
            "639d207f6a58ed2974f9737c3240ebffdf459857",
        ),
        # Check hash_index build
        ({"values": {}, "hash_index": "waffles"}, "waffles"),
        ({"values": {}, "hash_index": "waffles", "build_index": True}, "bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"),
    ],
)
def test_keywords_canary_hash(data, hash_index):
    """
    Canary test aginst possible changes in the options hash_index
    """

    opt = KeywordSet(**data)

    assert hash_index == opt.hash_index, opt.values


@pytest.mark.parametrize(
    "data1, data2",
    [
        # Test case sensitivity
        ({"values": {}}, {"values": {}}),
        # Test float creation
        ({"values": {"CAPS": 5, "other": 4.0e-3}}, {"values": {"caps": 5, "other": 0.004}}),
        ({"values": {"other": 4.123e-5}}, {"values": {"OTHER": 0.00004123}}),
        # Test list of floats
        ({"values": {"other": [1.11e-2, 2.22e-3]}}, {"values": {"other": [0.0111, 0.00222]}}),
        # Test small floats
        ({"values": {"other": 1 + 1.0e-17}}, {"values": {"other": 1.0 - 1.0e-17}}),
        # Test dict order
        ({"values": {"a": 5, "b": 6, "c": None}}, {"values": {"b": 6, "a": 5, "c": None}}),
        # Check recusive
        (
            {"values": {"d1": {"D2": [0.0, 5], "d3": (3, 1.0e-17)}}},
            {"values": {"d1": {"d2": [1.0e-17, 5], "d3": (3, 0)}}},
        ),
    ],
)
def test_keywords_comparison_hash(data1, data2):
    """
    Ensure the hash_index finds collisions correctly before and after serialization.
    """
    opt1 = KeywordSet(**data1)
    opt2 = KeywordSet(**data2)

    # Check after serialization and rebuild index
    opt1s = KeywordSet(**json.loads(opt1.json()), build_index=True)
    opt2s = KeywordSet(**json.loads(opt2.json()), build_index=True)

    # Paranoid, try all combinations
    assert opt1.hash_index == opt2.hash_index
    assert opt1.hash_index == opt2s.hash_index
    assert opt1s.hash_index == opt2.hash_index
    assert opt1s.hash_index == opt2s.hash_index
