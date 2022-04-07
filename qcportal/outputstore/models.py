import json
from enum import Enum
from typing import Union, Dict, Optional, Any

from pydantic import Field, validator, BaseModel, Extra

from qcportal.compression import CompressionEnum, compress, decompress_string, decompress_json


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
        allow_mutation = False
        extra = Extra.forbid

    output_type: OutputTypeEnum = Field(..., description="The type of output this is (stdout, error, etc)")
    compression: CompressionEnum = Field(CompressionEnum.none, description="Compression method (such as gzip)")
    compression_level: int = Field(0, description="Level of compression (typically 0-9)")
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
        if isinstance(data, dict):
            if values["compression"] != CompressionEnum.none:
                raise ValueError("Compression is set, but input is a dictionary")
            if values["compression_level"] != 0:
                raise ValueError("Compression level is set, but input is a dictionary")
            return json.dumps(data).encode()
        elif isinstance(data, str):
            if values["compression"] != CompressionEnum.none:
                raise ValueError("Compression is set, but input is a string")
            if values["compression_level"] != 0:
                raise ValueError("Compression level is set, but input is a string")
            return data.encode()
        else:
            return data

    @validator("compression", pre=True)
    def _set_compression(cls, compression):
        """Sets the compression type to CompressionEnum.none if compression is None

        Needed as older entries in the database have null for compression/compression_level
        """
        if compression is None:
            return CompressionEnum.none
        else:
            return compression

    @validator("compression_level", pre=True)
    def _set_compression_level(cls, compression_level):
        """Sets the compression_level to zero if compression is None

        Needed as older entries in the database have null for compression/compression_level
        """
        if compression_level is None:
            return 0
        else:
            return compression_level

    @classmethod
    def compress(
        cls,
        output_type: OutputTypeEnum,
        input_data: Union[Dict[str, str], str],
        compression_type: CompressionEnum = CompressionEnum.none,
        compression_level: Optional[int] = None,
    ):
        """Compresses a string or dictionary given a compression scheme and level

        Returns an object of type `cls`

        If compression_level is None, but a compression_type is specified, an appropriate default level is chosen
        """

        compressed_data, compression_type, compression_level = compress(input_data, compression_type, compression_level)

        return cls(
            output_type=output_type,
            data=compressed_data,
            compression=compression_type,
            compression_level=compression_level,
        )

    def get_string(self) -> str:
        """
        Returns the string representing the output
        """
        return decompress_string(self.data, self.compression)

    def get_json(self) -> Dict[Any, Any]:
        """
        Returns a dict if the data stored is a JSON string

        (errors are stored as JSON. stdout/stderr are just strings)
        """

        return decompress_json(self.data, self.compression)

    @property
    def as_string(self) -> str:
        return self.get_string()

    @property
    def as_json(self) -> Dict[Any, Any]:
        return self.get_json()
