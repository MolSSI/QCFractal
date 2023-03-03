from enum import Enum
from typing import Tuple, Optional, Any

from pydantic import PrivateAttr, Field, BaseModel, Extra

from qcportal.compression import decompress, CompressionEnum


class OutputTypeEnum(str, Enum):
    """
    What type of data is stored
    """

    stdout = "stdout"
    stderr = "stderr"
    error = "error"


class OutputStore(BaseModel):
    """
    Storage of outputs and error messages, with optional compression
    """

    class Config:
        extra = Extra.forbid

    output_type: OutputTypeEnum = Field(..., description="The type of output this is (stdout, error, etc)")
    compression_type: CompressionEnum = Field(CompressionEnum.none, description="Compression method (such as lzma)")

    data_url_: Optional[str] = None
    compressed_data_: Optional[bytes] = None
    decompressed_data_: Optional[Any] = None

    _client: Any = PrivateAttr(None)

    def propagate_client(self, client, history_base_url):
        self._client = client
        self.data_url_ = f"{history_base_url}/outputs/{self.output_type}/data"

    def _fetch_raw_data(self):
        if self.compressed_data_ is None and self.decompressed_data_ is None:
            cdata, ctype = self._client.make_request(
                "get",
                self.data_url_,
                Tuple[bytes, CompressionEnum],
            )

            self.compression_type = ctype
            self.compressed_data_ = cdata

    @property
    def data(self) -> Any:
        self._fetch_raw_data()

        # Decompress, then remove compressed form
        if self.decompressed_data_ is None:
            self.decompressed_data_ = decompress(self.compressed_data_, self.compression_type)
            self.compressed_data_ = None

        return self.decompressed_data_
