import numpy as np
import pytest

from ..model_utils import hash_dictionary, recursive_normalizer


@pytest.mark.parametrize(
    "unormalized, normalized",
    [
        (5.0 + 1.0e-12, 5.0),
        (0.0 + 1.0e-12, 0.0),
        (0.0 - 1.0e-12, 0.0),
        ("HeLLo", "hello"),
        ([0.0 - 1.0e-12, 0.0 + 1.0e-12], [0, 0]),
        ((0.0 - 1.0e-12, 0.0 + 1.0e-12), (0, 0)),
        ({"Hi": 1.0e-16}, {"hi": 0}),
        ({"Hi": {"Lo": 1.0e-13}}, {"hi": {"lo": 0}}),
    ],
)  # yapf: disable
def test_recursive_normalizer_exacts(unormalized, normalized):

    converted = recursive_normalizer(unormalized)
    assert converted == normalized
    if isinstance(normalized, dict):
        assert hash_dictionary(converted) == hash_dictionary(normalized)


def test_recursive_normalizer_array():
    bench = {"hello": np.arange(3)}
    norm1 = recursive_normalizer({"HeLLo": np.arange(3) + 1.0e-12})
    norm2 = recursive_normalizer({"HeLLo": np.arange(3) - 1.0e-12})

    assert np.array_equal(bench["hello"], norm1["hello"])
    assert np.array_equal(bench["hello"], norm2["hello"])
