from typing import Optional, Dict, Any

from pydantic import Field, BaseModel

from ..utils import recursive_normalizer, hash_dictionary


class KeywordSet(BaseModel):
    """
    A key:value storage object for Keywords.
    """

    class Config:
        allow_mutation = False

    id: Optional[int] = Field(
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

        BaseModel.__init__(self, **data)

        # Overwrite options with massaged values
        kwargs = {"lowercase": self.lowercase}
        if self.exact_floats:
            kwargs["digits"] = False

        self.__dict__["values"] = recursive_normalizer(self.values, **kwargs)

        # Build a hash index if we need it
        if build_index:
            self.build_index()

    def build_index(self):
        self.__dict__["hash_index"] = hash_dictionary(self.values.copy())
