from __future__ import annotations

import json
from enum import Enum
from typing import Optional, Union, Dict, Tuple, Any
import gzip
import bz2
import lzma


class CompressionEnum(str, Enum):
    """
    How data is compressed (compression method only, ie gzip, bzip2)
    """

    none = "none"
    gzip = "gzip"
    bzip2 = "bzip2"
    lzma = "lzma"


def compress(
    input_data: Union[str, bytes, Dict[str, str]],
    compression_type: CompressionEnum = CompressionEnum.none,
    compression_level: Optional[int] = None,
) -> Tuple[bytes, CompressionEnum, int]:
    """Compresses data given a compression scheme and level

    If compression_level is None, but a compression_type is specified, an appropriate default level is chosen

    Returns a tuple containing the compressed data, applied compression type, and compression level (which may
    be different from the provided arguments)
    """

    if isinstance(input_data, dict):
        data = json.dumps(input_data).encode()
    elif isinstance(input_data, str):
        data = input_data.encode()
    elif isinstance(input_data, bytes):
        data = input_data
    else:
        raise RuntimeError(f"Unknown input data type: {type(input_data)}")

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
    # By default, use level = 1 for larger data (>15MB or so)
    elif compression_type is CompressionEnum.lzma:
        if compression_level is None:
            if len(data) > 15 * 1048576:
                compression_level = 1
            else:
                compression_level = 6
        data = lzma.compress(data, preset=compression_level)
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")

    return (data, compression_type, compression_level)


def decompress_bytes(compressed_data: bytes, compression_type: CompressionEnum) -> bytes:
    """
    Decompresses bytes to a byte array
    """
    if compression_type is CompressionEnum.none:
        return compressed_data
    elif compression_type is CompressionEnum.gzip:
        return gzip.decompress(compressed_data)
    elif compression_type is CompressionEnum.bzip2:
        return bz2.decompress(compressed_data)
    elif compression_type is CompressionEnum.lzma:
        return lzma.decompress(compressed_data)
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")


def decompress_string(compressed_data: bytes, compression_type: CompressionEnum) -> str:

    """
    Returns the string that was compressed
    """
    return decompress_bytes(compressed_data, compression_type).decode()


def decompress_json(compressed_data: bytes, compression_type: CompressionEnum) -> Dict[Any, Any]:
    """
    Returns a dictionary that was stored compressed
    """
    s = decompress_string(compressed_data, compression_type)
    return json.loads(s)
