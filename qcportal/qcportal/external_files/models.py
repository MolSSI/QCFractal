from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional

try:
    from pydantic.v1 import BaseModel, Extra, validator, PrivateAttr, Field
except ImportError:
    from pydantic import BaseModel, Extra, validator, PrivateAttr, Field


class ExternalFileStatusEnum(str, Enum):
    """
    The state of an external file
    """

    available = "available"
    processing = "processing"


class ExternalFileTypeEnum(str, Enum):
    """
    The state of an external file
    """

    dataset_attachment = "dataset_attachment"


class ExternalFile(BaseModel):
    id: int
    file_type: ExternalFileTypeEnum

    created_on: datetime
    status: ExternalFileStatusEnum

    file_name: str
    description: Optional[str]
    provenance: Dict[str, Any]

    sha256sum: str
    file_size: int
