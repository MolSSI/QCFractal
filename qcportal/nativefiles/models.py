import os
from typing import Union, Optional

from pydantic import Field, validator, BaseModel, Extra

from qcportal.compression import CompressionEnum, compress, decompress_bytes, decompress_string, get_compressed_ext


class NativeFile(BaseModel):
    """
    Storage of native files, with compression
    """

    class Config:
        allow_mutation = False
        extra = Extra.forbid

    id: Optional[int] = Field(
        None, description="ID of the object on the database. This is assigned automatically by the database."
    )
    record_id: Optional[int] = Field(None, description="The id of the record this file is attached to")

    name: str = Field(..., description="Name of the file")
    compression: CompressionEnum = Field(..., description="Compression method (such as gzip)")
    compression_level: int = Field(..., description="Level of compression (typically 0-9)")
    is_text: bool = Field(..., description="True if this is a plain text file. False if binary")
    uncompressed_size: int = Field(..., description="The uncompressed size of the file")
    data: bytes = Field(..., description="Compressed raw data of output/errors, etc")

    @validator("data", pre=True)
    def _set_data(cls, data, values):
        """Handles special data types

        Strings are converted to byte arrays, and dicts are converted via json.dumps. If a string or
        dictionary is given, then compression & compression level must be none/0 (the defaults)

        Will chack that compression and compression level are None/0. Since this validator
        runs after all the others, that is safe.

        (According to pydantic docs, validators are run in the order of field definition)
        """
        if isinstance(data, str):
            if values["compression"] != CompressionEnum.none:
                raise ValueError("Compression is set, but input is a string")
            if values["compression_level"] != 0:
                raise ValueError("Compression level is set, but input is a string")
            return data.encode()
        else:
            return data

    @classmethod
    def compress(
        cls,
        name: str,
        input_data: Union[str, bytes],
        compression_type: CompressionEnum = CompressionEnum.none,
        compression_level: Optional[int] = None,
    ):
        """Compresses a string or bytes given a compression scheme and level

        Returns an object of type `cls`

        If compression_level is None, but a compression_type is specified, an appropriate default level is chosen
        """

        is_text = False

        if isinstance(input_data, str):
            input_data = input_data.encode()
            is_text = True

        uncompressed_size = len(input_data)
        compressed_data, compression_type, compression_level = compress(input_data, compression_type, compression_level)

        return cls(
            name=name,
            data=compressed_data,
            is_text=is_text,
            compression=compression_type,
            compression_level=compression_level,
            uncompressed_size=uncompressed_size,
        )

    @property
    def as_string(self) -> str:
        if not self.is_text:
            raise RuntimeError("Cannot print as string - is not text!")

        return decompress_string(self.data, self.compression)

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
            name += get_compressed_ext(self.compression)

        full_path = os.path.join(directory, name)
        if os.path.exists(full_path) and not overwrite:
            raise RuntimeError(f"File {full_path} already exists. Not overwriting")

        with open(full_path, "wb") as f:
            if keep_compressed:
                f.write(self.data)
            else:
                # Ok if text. We won't decode into a string
                f.write(decompress_bytes(self.data, self.compression))
