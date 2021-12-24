import bz2
import gzip
import json
import lzma
from enum import Enum
from typing import Union, Dict, Optional

from pydantic import Field, validator, BaseModel, Extra


class CompressionEnum(str, Enum):
    """
    How data is compressed (compression method only, ie gzip, bzip2)
    """

    none = "none"
    gzip = "gzip"
    bzip2 = "bzip2"
    lzma = "lzma"


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

    id: Optional[int] = Field(
        None, description="ID of the object on the database. This is assigned automatically by the database."
    )
    history_id: Optional[int] = Field(None, description="The history ID this output is attached to")

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
        """Compresses a string given a compression scheme and level

        Returns an object of type `cls`

        If compression_level is None, but a compression_type is specified, an appropriate default level is chosen
        """

        if isinstance(input_data, dict):
            input_data = json.dumps(input_data)

        data = input_data.encode()

        # No compression
        if compression_type is CompressionEnum.none:
            compression_level = 0

        # gzip compression
        elif compression_type is CompressionEnum.gzip:
            if compression_level is None:
                compression_level = 6
            data = gzip.compress(data, compresslevel=compression_level)

        # bzip2 compression
        elif compression_type is CompressionEnum.bzip2:
            if compression_level is None:
                compression_level = 6
            data = bz2.compress(data, compresslevel=compression_level)

        # LZMA compression
        # By default, use level = 1 for larger files (>15MB or so)
        elif compression_type is CompressionEnum.lzma:
            if compression_level is None:
                if len(data) > 15 * 1048576:
                    compression_level = 1
                else:
                    compression_level = 6
            data = lzma.compress(data, preset=compression_level)
        else:
            # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
            raise TypeError("Unknown compression type??")

        return cls(
            output_type=output_type, data=data, compression=compression_type, compression_level=compression_level
        )

    def get_string(self):
        """
        Returns the string representing the output
        """
        if self.compression is CompressionEnum.none:
            return self.data.decode()
        elif self.compression is CompressionEnum.gzip:
            return gzip.decompress(self.data).decode()
        elif self.compression is CompressionEnum.bzip2:
            return bz2.decompress(self.data).decode()
        elif self.compression is CompressionEnum.lzma:
            return lzma.decompress(self.data).decode()
        else:
            # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
            raise TypeError("Unknown compression type??")

    def get_json(self):
        """
        Returns a dict if the data stored is a JSON string

        (errors are stored as JSON. stdout/stderr are just strings)
        """
        s = self.get_string()
        return json.loads(s)

    @property
    def as_string(self):
        return self.get_string()

    @property
    def as_json(self):
        return self.get_json()
