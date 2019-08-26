"""
Tests for the interface utility functions.
"""

from . import portal


def test_replace_dict_keys():

    ret = portal.util.replace_dict_keys({5: 5}, {5: 10})
    assert ret == {10: 5}

    ret = portal.util.replace_dict_keys({5: 5}, {10: 5})
    assert ret == {5: 5}

    ret = portal.util.replace_dict_keys([{5: 5}], {10: 5})
    assert ret == [{5: 5}]

    ret = portal.util.replace_dict_keys({5: {5: 10}}, {5: 10})
    assert ret == {10: {10: 10}}
