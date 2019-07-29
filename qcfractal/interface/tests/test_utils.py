"""
Tests for the interface utility functions.
"""

from . import portal
from pydantic import BaseModel, Schema
from typing import Optional, List, Union, Tuple, Dict, Any
from pytest import fixture


@fixture(scope="function")
def doc_fixture():

    class Nest(BaseModel):
        """A nested model"""
        n: float = 56

    class X(BaseModel):
        """A Pydantic model made up of many, many different combinations of ways of mapping types in Pydantic"""
        x: int
        y: str = Schema(...)
        n: Nest
        n2: Nest = Schema(
            Nest(),
            description="A detailed description"
        )
        z: float = 5
        z2: float = None
        z3: Optional[float]
        z4: Optional[float] = Schema(
            5,
            description="Some number I just made up"
        )
        z5: Optional[Union[float, int]]
        z6: Optional[List[int]]
        l: List[int]
        l2: List[Union[int, str]]
        t: Tuple[str, int]
        t2: Tuple[List[int]]
        t3: Tuple[Any]
        d: Dict[str, Any]
        dlu: Dict[Union[int, str], List[Union[int, str, float]]] = Schema(..., description="this is complicated")
        dlu2: Dict[Any, List[Union[int, str, float]]]
        dlu3: Dict[str, Any]
        si: int = Schema(
            ...,
            description="A level of constraint",
            gt=0
        )
        sf: float = Schema(
            None,
            description="Optional Constrained Number",
            le=100.3
        )
    yield X


def test_replace_dict_keys():

    ret = portal.util.replace_dict_keys({5: 5}, {5: 10})
    assert ret == {10: 5}

    ret = portal.util.replace_dict_keys({5: 5}, {10: 5})
    assert ret == {5: 5}

    ret = portal.util.replace_dict_keys([{5: 5}], {10: 5})
    assert ret == [{5: 5}]

    ret = portal.util.replace_dict_keys({5: {5: 10}}, {5: 10})
    assert ret == {10: {10: 10}}


def test_doc_formatter(doc_fixture):
    portal.util.doc_formatter(doc_fixture, allow_failure=False)
    assert "this is complicated" in doc_fixture.__doc__
    assert "z3 : float, Optional" in doc_fixture.__doc__
    print(doc_fixture.__doc__)


def test_doc_formatter_exiting(doc_fixture):
    doc_fixture.__doc__ = "Parameters\n"
    portal.util.doc_formatter(doc_fixture, allow_failure=False)
    assert "this is complicated" not in doc_fixture.__doc__
