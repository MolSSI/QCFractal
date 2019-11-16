import pydantic
import pytest

from ..rest_models import QueryFilter


def test_include_exclude_exclusive():
    QueryFilter()
    QueryFilter(include=["foo"])
    QueryFilter(exclude=["goo"])
    with pytest.raises(pydantic.ValidationError):
        QueryFilter(include=["foo"], exclude=["goo"])
