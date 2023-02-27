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

    data_url_: Optional[str] = None  # Typically set in derived classes
    compressed_data_: Optional[bytes] = None
    decompressed_data_: Optional[Any] = None

    def _fetch_raw_data(self):
        if self.compressed_data_ is None and self.decompressed_data_ is None:
            cdata, ctype = self._client.make_request(
                "get",
                self.data_url_,
                Tuple[bytes, CompressionEnum],
            )

            assert self.compression_type == ctype
            self.compressed_data_ = cdata

    @property
    def data(self) -> Any:
        self._fetch_raw_data()

        # Decompress, then remove compressed form
        if self.decompressed_data_ is None:
            self.decompressed_data_ = decompress(self.compressed_data_, self.compression_type)
            self.compressed_data_ = None

        return self.decompressed_data_
