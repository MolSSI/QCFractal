from typing import Tuple, Optional, Any

from pydantic import Extra, BaseModel, PrivateAttr
from qcelemental.models.results import WavefunctionProperties

from qcportal.compression import CompressionEnum, decompress


class Wavefunction(BaseModel):
    """
    Storage of native files, with compression
    """

    class Config:
        extra = Extra.forbid

    compression_type: CompressionEnum

    data_url_: Optional[str] = None
    compressed_data_: Optional[bytes] = None
    decompressed_data_: Optional[WavefunctionProperties] = None

    _client: Any = PrivateAttr(None)

    def propagate_client(self, client, record_base_url):
        self._client = client
        self.data_url_ = f"{record_base_url}/wavefunction/data"

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
    def data(self) -> WavefunctionProperties:
        self._fetch_raw_data()

        # Decompress, then remove compressed form
        if self.decompressed_data_ is None:
            wfn_dict = decompress(self.compressed_data_, self.compression_type)
            self.decompressed_data_ = WavefunctionProperties(**wfn_dict)
            self.compressed_data_ = None

        return self.decompressed_data_
