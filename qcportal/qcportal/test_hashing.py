import random

from qcarchivetesting import load_hash_test_data
from qcportal.utils import hash_dict


def shuffle_dict(d):
    def _shuffle_inner(x):
        a, b = x
        if isinstance(b, dict):
            b = shuffle_dict(b)
        return a, b

    d2 = [_shuffle_inner(x) for x in d.items()]
    random.shuffle(d2)
    return dict(d2)


def test_hash_stable():
    test_data = load_hash_test_data("dict_hash_test_data")

    for hash, data in test_data.items():
        assert hash == hash_dict(data)

        # Shuffle and test again
        for i in range(20):
            d2 = shuffle_dict(data)
            assert hash_dict(d2) == hash
