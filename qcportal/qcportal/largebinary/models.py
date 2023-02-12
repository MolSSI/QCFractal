from typing import Optional, Any, Tuple

from pydantic import BaseModel, Extra, PrivateAttr

from qcportal.compression import (
    CompressionEnum,
    decompress,
)


class LargeBinary(BaseModel):
    """
    Stores information about a large binary blob
    """

    class Config:
        allow_mutation = True
        extra = Extra.forbid

    _client: Any = PrivateAttr(None)

    id: int
    size: int
    checksum: str
    compression_type: CompressionEnum

    _compressed_data: Optional[bytes] = PrivateAttr(None)
    _decompressed_data: Optional[Any] = PrivateAttr(None)

    def _fetch_from_url(self, url: str):
        if self._compressed_data is None and self._decompressed_data is None:
            cdata, ctype = self._client._auto_request(
                "get",
                url,
                None,
                None,
                Tuple[bytes, CompressionEnum],
                None,
                None,
            )

            assert self.compression_type == ctype
            self._compressed_data = cdata

    def fetch(self):
        raise NotImplementedError("fetch() must be implemented by derived classes")

    @property
    def data(self) -> Any:
        self.fetch()

        # Decompress, then remove compressed form
        if self._decompressed_data is None:
            self._decompressed_data = decompress(self._compressed_data, self.compression_type)
            self._compressed_data = None

        return self._decompressed_data
