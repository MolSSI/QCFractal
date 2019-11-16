import pytest
from pydantic import BaseModel, ValidationError

from ..common_models import ObjectId


class OIdModel(BaseModel):
    id: ObjectId


@pytest.mark.parametrize(
    "oid", ["5c754f049642c7c861d67de5", "000000000000000000000000", "0123456789abcdef01234567", "123", 123]
)
def test_objid_check(oid):
    OIdModel(id=oid)


@pytest.mark.parametrize("oid", ["", "0123456789abcdef0123456z"])
def test_objid_wrong(oid):
    with pytest.raises(ValidationError):
        OIdModel(id=oid)
