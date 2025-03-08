from __future__ import annotations

from enum import Enum
from typing import (
    TYPE_CHECKING,
)

try:
    import pydantic.v1 as pydantic
    from pydantic.v1 import BaseModel, Extra, validator, PrivateAttr, Field
except ImportError:
    import pydantic
    from pydantic import BaseModel, Extra, validator, PrivateAttr, Field

if TYPE_CHECKING:
    pass


class ProjectAttachmentType(str, Enum):
    """
    The type of attachment a file is for a dataset
    """

    other = "other"
