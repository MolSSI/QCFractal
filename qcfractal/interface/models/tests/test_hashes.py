import json

import pytest

from ..common_models import KeywordSet


@pytest.mark.parametrize("data, hash_index", [

    # Simple checks
    ({"program": "psi4", "values": {"hi": 5}},
     "c9ae3acb782cad09c6867d8433a2963fe7b724fd"),

    ({"program": "PSI4", "values": {"HI": 5}},
     "c9ae3acb782cad09c6867d8433a2963fe7b724fd"),

    # Check options combinations
    ({"program": "PSI4", "values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}},
     "2d93c6c81466a2c51ec2e1a4f9e53a2c6bd9e3ed"),

    ({"program": "PSI4", "values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}, "lowercase": False},
     "3a6a8e2fb0449ec3db81191daf5de66f9a63c088"),

    ({"program": "PSI4", "values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}, "exact_floats": True},
     "7860429a00fe05f4e3a829c2e5680cd5a0fa94b6"),

    ({"program": "PSI4", "values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}, "exact_floats": True, "lowercase": False},
     "f0c58a301466f29c43cc303260e044e94551bc0a"),

    # Check recursive
    ({"program": "PSI4", "values": {"d1": {"D2": [1.e-17, 5]}}},
     "e50f3c0a580ca846b484a21a5a28058785fb8d00"),

    ({"program": "PSI4", "values": {"d1": {"D2": [1.e-17, 5]}}, "exact_floats": True, "lowercase": False},
     "ab6ce6dade7454730d2872c95e8f84449e00b321"),

    # Check hash_index build
    ({"program": "PSI4", "values": {}, "hash_index": "waffles"},
     "waffles"),

    ({"program": "PSI4", "values": {}, "hash_index": "waffles", "build_index": True},
     "10bdc0b99338761032d9d0f999ede01295185e5d"),
]) # yapf: disable
def test_option_canary_hash(data, hash_index):
    """
    Canary test aginst possible changes in the options hash_index
    """

    opt = KeywordSet(**data)

    assert hash_index == opt.hash_index, opt.keywords


@pytest.mark.parametrize("data1, data2", [
    # Test case sensitivity
    ({"program": "Psi4", "values": {}},
     {"program": "psi4", "values": {}}),

    # Test float creation
    ({"program": "Psi4", "values": {"CAPS": 5, "other": 4.e-3}},
     {"program": "psi4", "values": {"caps": 5, "other": 0.004}}),

    ({"program": "Psi4", "values": {"other": 4.123e-5}},
     {"program": "psi4", "values": {"OTHER": 0.00004123}}),

    # Test list of floats
    ({"program": "S", "values": {"other": [1.11e-2, 2.22e-3]}},
     {"program": "s", "values": {"other": [0.0111, 0.00222]}}),

    # Test small floats
    ({"program": "S", "values": {"other": 1 + 1.e-17}},
     {"program": "s", "values": {"other": 1.0 - 1.e-17}}),

    # Test dict order
    ({"program": "S", "values": {"a": 5, "b": 6, "c": None}},
     {"program": "s", "values": {"b": 6, "a": 5, "c": None}}),

    # Check recusive
    ({"program": "PSI4", "values": {"d1": {"D2": [0.0, 5], "d3": (3, 1.e-17)}}},
     {"program": "PSI4", "values": {"d1": {"d2": [1.e-17, 5], "d3": (3, 0)}}}),

]) # yapf: disable
def test_option_comparison_hash(data1, data2):
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