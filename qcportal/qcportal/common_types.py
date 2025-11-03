from typing import Annotated, TypeAlias

import numpy as np
from numpy.typing import NDArray
from pydantic import StringConstraints, GetPydanticSchema, InstanceOf

LowerStr = Annotated[str, StringConstraints(to_lower=True)]
Max128Str = Annotated[str, StringConstraints(max_length=128)]

# From https://github.com/pydantic/pydantic/issues/6477#issuecomment-3066697766
PydanticNDArray: TypeAlias = Annotated[
    NDArray,
    GetPydanticSchema(lambda _s, h: h(InstanceOf[np.ndarray]), lambda _s, h: h(InstanceOf[np.ndarray])),
]
