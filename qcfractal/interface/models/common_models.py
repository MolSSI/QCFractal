"""
Common models for QCPortal/Fractal
"""
import json

# For compression
import lzma
import bz2
import gzip

from enum import Enum
from typing import Any, Dict, Optional, Union

from pydantic import Field, validator
from qcelemental.models import AutodocBaseSettings, Molecule, ProtoModel, Provenance
from qcelemental.models.procedures import OptimizationProtocols
from qcelemental.models.results import ResultProtocols

from .model_utils import hash_dictionary, prepare_basis, recursive_normalizer

__all__ = ["QCSpecification", "OptimizationSpecification", "KeywordSet", "ObjectId", "DriverEnum", "Citation"]

# Add in QCElemental models
__all__.extend(["Molecule", "Provenance", "ProtoModel"])

# Autodoc
__all__.extend(["OptimizationProtocols", "ResultProtocols"])


class ObjectId(str):
    """
    The Id of the object in the data.
    """

    _valid_hex = set("0123456789abcdef")

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if isinstance(v, str) and (len(v) == 24) and (set(v) <= cls._valid_hex):
            return v
        elif isinstance(v, int):
            return str(v)
        elif isinstance(v, str) and v.isdigit():
            return v
        else:
            raise TypeError("The string {} is not a valid 24-character hexadecimal or integer ObjectId!".format(v))


class DriverEnum(str, Enum):
    """
    The type of calculation that is being performed (e.g., energy, gradient, Hessian, ...).
    """

    energy = "energy"
    gradient = "gradient"
    hessian = "hessian"
    properties = "properties"


class CompressionEnum(str, Enum):
    """
    How data is compressed (compression method only, ie gzip, bzip2)
    """

    none = "none"
    gzip = "gzip"
    bzip2 = "bzip2"
    lzma = "lzma"


class KVStore(ProtoModel):
    """
    Storage of outputs and error messages, with optional compression
    """

    id: int = Field(
        None, description="Id of the object on the database. This is assigned automatically by the database."
    )

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

        return cls(data=data, compression=compression_type, compression_level=compression_level)

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


class QCSpecification(ProtoModel):
    """
    The quantum chemistry metadata specification for individual computations such as energy, gradient, and Hessians.
    """

    driver: DriverEnum = Field(..., description=str(DriverEnum.__doc__))
    method: str = Field(..., description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...).")
    basis: Optional[str] = Field(
        None,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
        "methods without basis sets.",
    )
    keywords: Optional[ObjectId] = Field(
        None,
        description="The Id of the :class:`KeywordSet` registered in the database to run this calculation with. This "
        "Id must exist in the database.",
    )
    protocols: Optional[ResultProtocols] = Field(ResultProtocols(), description=str(ResultProtocols.__base_doc__))
    program: str = Field(
        ...,
        description="The quantum chemistry program to evaluate the computation with. Not all quantum chemistry programs"
        " support all combinations of driver/method/basis.",
    )

    def dict(self, *args, **kwargs):
        ret = super().dict(*args, **kwargs)

        # Maintain hash compatability
        if len(ret["protocols"]) == 0:
            ret.pop("protocols", None)

        return ret

    @validator("basis")
    def _check_basis(cls, v):
        return prepare_basis(v)

    @validator("program")
    def _check_program(cls, v):
        return v.lower()

    @validator("method")
    def _check_method(cls, v):
        return v.lower()


class OptimizationSpecification(ProtoModel):
    """
    Metadata describing a geometry optimization.
    """

    program: str = Field(..., description="Optimization program to run the optimization with")
    keywords: Optional[Dict[str, Any]] = Field(
        None,
        description="Dictionary of keyword arguments to pass into the ``program`` when the program runs. "
        "Note that unlike :class:`QCSpecification` this is a dictionary of keywords, not the Id for a "
        ":class:`KeywordSet`. ",
    )
    protocols: Optional[OptimizationProtocols] = Field(
        OptimizationProtocols(), description=str(OptimizationProtocols.__base_doc__)
    )

    def dict(self, *args, **kwargs):
        ret = super().dict(*args, **kwargs)

        # Maintain hash compatability
        if len(ret["protocols"]) == 0:
            ret.pop("protocols", None)

        return ret

    @validator("program")
    def _check_program(cls, v):
        return v.lower()

    @validator("keywords")
    def _check_keywords(cls, v):
        if v is not None:
            v = recursive_normalizer(v)
        return v


class KeywordSet(ProtoModel):
    """
    A key:value storage object for Keywords.
    """

    id: Optional[ObjectId] = Field(
        None, description="The Id of this object, will be automatically assigned when added to the database."
    )
    hash_index: str = Field(
        ...,
        description="The hash of this keyword set to store and check for collisions. This string is automatically "
        "computed.",
    )
    values: Dict[str, Optional[Any]] = Field(
        ...,
        description="The key-value pairs which make up this KeywordSet. There is no direct relation between this "
        "dictionary and applicable program/spec it can be used on.",
    )
    lowercase: bool = Field(
        True,
        description="String keys are in the ``values`` dict are normalized to lowercase if this is True. Assists in "
        "matching against other :class:`KeywordSet` objects in the database.",
    )
    exact_floats: bool = Field(
        False,
        description="All floating point numbers are rounded to 1.e-10 if this is False."
        "Assists in matching against other :class:`KeywordSet` objects in the database.",
    )
    comments: Optional[str] = Field(
        None,
        description="Additional comments for this KeywordSet. Intended for pure human/user consumption " "and clarity.",
    )

    def __init__(self, **data):

        build_index = False
        if ("hash_index" not in data) or data.pop("build_index", False):
            build_index = True
            data["hash_index"] = "placeholder"

        ProtoModel.__init__(self, **data)

        # Overwrite options with massaged values
        kwargs = {"lowercase": self.lowercase}
        if self.exact_floats:
            kwargs["digits"] = False

        self.__dict__["values"] = recursive_normalizer(self.values, **kwargs)

        # Build a hash index if we need it
        if build_index:
            self.__dict__["hash_index"] = self.get_hash_index()

    def get_hash_index(self):
        return hash_dictionary(self.values.copy())


class Citation(ProtoModel):
    """ A literature citation.  """

    acs_citation: Optional[
        str
    ] = None  # hand-formatted citation in ACS style. In the future, this could be bibtex, rendered to different formats.
    bibtex: Optional[str] = None  # bibtex blob for later use with bibtex-renderer
    doi: Optional[str] = None
    url: Optional[str] = None

    def to_acs(self) -> str:
        """ Returns an ACS-formatted citation """
        return self.acs_citation
