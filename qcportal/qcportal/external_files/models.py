from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, PrivateAttr


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
    project_attachment = "project_attachment"


class ExternalFileUploadBase(BaseModel):
    file_name: str
    description: str
    provenance: dict[str, Any]



class ExternalFile(BaseModel):
    id: int
    file_type: ExternalFileTypeEnum

    created_on: datetime
    status: ExternalFileStatusEnum

    file_name: str
    description: str | None
    provenance: dict[str, Any]

    sha256sum: str
    file_size: int

    _client: Any = PrivateAttr(None)

    def propagate_client(self, client):
        self._client = client

    def get_direct_url(self):
        if self._client is None:
            raise RuntimeError("No client to use with this ExternalFile")

        return self._client.get_external_file_direct_link(self.id)

    def download(self, destination_path: str, overwrite: bool = False) -> None:
        """
        Downloads an external file to the given path

        The file size and checksum will be checked against the metadata stored on the server

        Parameters
        ----------
        destination_path
            Full path to the destination file (including filename)
        overwrite
            If True, allow for overwriting an existing file. If False, and a file already exists at the given
            destination path, an exception will be raised.
        """

        if self._client is None:
            raise RuntimeError("No client to use with this ExternalFile")

        # Swallow return value - no one cares
        self._client.download_external_file(self.id, destination_path, overwrite)
