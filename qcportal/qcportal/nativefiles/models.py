import os
from typing import Optional, Any, Tuple

from pydantic import Field, BaseModel, Extra, PrivateAttr

from qcportal.compression import (
    CompressionEnum,
    decompress,
    get_compressed_ext,
)


class NativeFile(BaseModel):
    """
    Storage of native files, with compression
    """

    class Config:
        extra = Extra.forbid

    name: str = Field(..., description="Name of the file")
    compression_type: CompressionEnum = Field(..., description="Compression method (such as lzma)")

    data_url_: Optional[str] = None
    compressed_data_: Optional[bytes] = None
    decompressed_data_: Optional[Any] = None

    _client: Any = PrivateAttr(None)

    def propagate_client(self, client, record_base_url):
        self._client = client
        self.data_url_ = f"{record_base_url}/native_files/{self.name}/data"

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

    def save_file(
        self, directory: str, new_name: Optional[str] = None, keep_compressed: bool = False, overwrite: bool = False
    ):
        """
        Saves the file to the given directory
        """

        if new_name is None:
            name = self.name
        else:
            name = new_name

        if keep_compressed:
            name += get_compressed_ext(self.compression_type)

        full_path = os.path.join(directory, name)
        if os.path.exists(full_path) and not overwrite:
            raise RuntimeError(f"File {full_path} already exists. Not overwriting")

        if keep_compressed:
            with open(full_path, "wb") as f:
                f.write(self.data)
        else:
            d = self.data

            # TODO - streaming decompression?
            if isinstance(d, str):
                with open(full_path, "wt") as f:
                    f.write(self.data)
            elif isinstance(d, bytes):
                with open(full_path, "wb") as f:
                    f.write(self.data)
            else:
                raise RuntimeError(f"Cannot write data of type {type(d)} to a file")
