from __future__ import annotations

import json
import lzma
from enum import Enum
from typing import Optional, Union, Dict, Tuple, List, Any

import msgpack
import zstandard


class CompressionEnum(str, Enum):
    """
    How data is compressed (compression method only, ie lzma, zstd)
    """

    none = "none"
    lzma = "lzma"
    zstd = "zstd"


def get_compressed_ext(compression_type: str) -> str:
    if compression_type == CompressionEnum.none:
        return ""
    elif compression_type == CompressionEnum.zstd:
        return ".zstd"
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")


def compress(
    input_data: Any,
    compression_type: CompressionEnum = CompressionEnum.zstd,
    compression_level: Optional[int] = None,
) -> Tuple[bytes, CompressionEnum, int]:
    """Serializes and compresses data given a compression scheme and level

    If compression_level is None, but a compression_type is specified, an appropriate default level is chosen

    Returns a tuple containing the compressed data, applied compression type, and compression level (which may
    be different from the provided arguments)
    """

    data = msgpack.packb(input_data, use_bin_type=True)

    # No compression
    if compression_type is CompressionEnum.none:
        compression_level = 0

    # LZMA compression
    # By default, use level = 1 for larger data (>15MB or so)
    elif compression_type is CompressionEnum.lzma:
        if compression_level is None:
            if len(data) > 15 * 1048576:
                compression_level = 1
            else:
                compression_level = 6
        data = lzma.compress(data, preset=compression_level)

    # ZStandard compression
    # By default, use level = 6 for larger data (>15MB or so)
    elif compression_type is CompressionEnum.zstd:
        if compression_level is None:
            if len(data) > 15 * 1048576:
                compression_level = 6
            else:
                compression_level = 16
        data = zstandard.compress(data, level=compression_level)
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")

    return (data, compression_type, compression_level)


def decompress(compressed_data: bytes, compression_type: CompressionEnum) -> Any:
    """
    Decompresses and deserializes data into python objects
    """
    if compression_type == CompressionEnum.none:
        decompressed_data = compressed_data
    elif compression_type == CompressionEnum.lzma:
        decompressed_data = lzma.decompress(compressed_data)
    elif compression_type == CompressionEnum.zstd:
        decompressed_data = zstandard.decompress(compressed_data)
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")

    return msgpack.unpackb(decompressed_data, raw=False)


def compress_old(
    input_data: Union[str, bytes, Dict[str, str]],
    compression_type: CompressionEnum = CompressionEnum.none,
    compression_level: Optional[int] = None,
) -> Tuple[bytes, CompressionEnum, int]:
    """Compresses data given a compression scheme and level

    If compression_level is None, but a compression_type is specified, an appropriate default level is chosen

    Returns a tuple containing the compressed data, applied compression type, and compression level (which may
    be different from the provided arguments)
    """

    if isinstance(input_data, (dict, list, tuple)):
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

    # LZMA compression
    # By default, use level = 1 for larger data (>15MB or so)
    elif compression_type is CompressionEnum.lzma:
        if compression_level is None:
            if len(data) > 15 * 1048576:
                compression_level = 1
            else:
                compression_level = 6
        data = lzma.compress(data, preset=compression_level)

    # ZStandard compression
    # By default, use level = 6 for larger data (>15MB or so)
    elif compression_type is CompressionEnum.zstd:
        if compression_level is None:
            if len(data) > 15 * 1048576:
                compression_level = 6
            else:
                compression_level = 16
        data = zstandard.compress(data, level=compression_level)
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")

    return (data, compression_type, compression_level)


def decompress_old_bytes(compressed_data: bytes, compression_type: CompressionEnum) -> bytes:
    """
    Decompresses bytes to a byte array
    """
    if compression_type == CompressionEnum.none:
        return compressed_data
    elif compression_type == CompressionEnum.lzma:
        return lzma.decompress(compressed_data)
    elif compression_type == CompressionEnum.zstd:
        return zstandard.decompress(compressed_data)
    else:
        # Shouldn't ever happen, unless we change CompressionEnum but not the rest of this function
        raise TypeError(f"Unknown compression type: {compression_type}")


def decompress_old_string(compressed_data: bytes, compression_type: CompressionEnum) -> str:

    """
    Returns the string that was compressed
    """
    return decompress_old_bytes(compressed_data, compression_type).decode()


def decompress_old_json(compressed_data: bytes, compression_type: CompressionEnum) -> Union[List[Any], Dict[Any, Any]]:
    """
    Returns a dictionary that was stored compressed
    """
    s = decompress_old_string(compressed_data, compression_type)
    return json.loads(s)
