import pytest
from pydantic import BaseModel, ValidationError

from ..records import OptimizationRecord
import qcelemental as qcel

oid1 = "000000000000000000000000"
oid2 = "000000000000000000000001"
