from enum import Enum


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
