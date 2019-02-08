import pytest

from ..common_models import Option


@pytest.mark.parametrize("opt, hash_index", [
    (Option(**{"program": "psi4", "options": {"hi": 5}}),
     "c9ae3acb782cad09c6867d8433a2963fe7b724fd"),

    (Option(**{"program": "PSI4", "options": {"HI": 5}}),
     "c9ae3acb782cad09c6867d8433a2963fe7b724fd"),

    (Option(**{"program": "PSI4", "options": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}}),
     "7860429a00fe05f4e3a829c2e5680cd5a0fa94b6"),

    (Option(**{"program": "PSI4", "options": {}, "hash_index": "waffles"}),
     "waffles"),

    (Option(**{"program": "PSI4", "options": {}, "hash_index": "waffles", "build_index": True}),
     "10bdc0b99338761032d9d0f999ede01295185e5d"),
]) # yapf: disable
def test_option_canary_hash(opt, hash_index):
    """
    Canary test aginst possible changes in the options hash_index
    """

    assert hash_index == opt.hash_index


@pytest.mark.parametrize("opt1, opt2", [
    # Test case sensitivity
    (Option(**{"program": "Psi4", "options": {}}),
     Option(**{"program": "psi4", "options": {}})),

    # Test float creation
    (Option(**{"program": "Psi4", "options": {"CAPS": 5, "other": 4.e-3}}),
     Option(**{"program": "psi4", "options": {"caps": 5, "other": 0.004}})),

    (Option(**{"program": "Psi4", "options": {"other": 4.123e-5}}),
     Option(**{"program": "psi4", "options": {"OTHER": 0.00004123}})),

    # Test list of floats
    (Option(**{"program": "S", "options": {"other": [1.11e-2, 2.22e-3]}}),
     Option(**{"program": "s", "options": {"other": [0.0111, 0.00222]}})),

    # Test small floats
    (Option(**{"program": "S", "options": {"other": 1 + 1.e-17}}),
     Option(**{"program": "s", "options": {"other": 1 - 1.e-17}})),

    # Test dict order
    (Option(**{"program": "S", "options": {"a": 5, "b": 6}}),
     Option(**{"program": "s", "options": {"b": 6, "a": 5}})),

]) # yapf: disable
def test_option_comparison_hash(opt1, opt2):
    """
    Ensure the index finds collisions
    """

    assert opt1.hash_index == opt2.hash_index

    # Assert after serialization
    opt2 = Option.parse_raw(opt2.json())
    opt1 = Option.parse_raw(opt1.json())
    assert opt1.hash_index == opt2.hash_index