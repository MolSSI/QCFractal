import json
from enum import Enum
from typing import List, Union, Optional, Dict, Iterable, Tuple, Sequence, Any

try:
    from pydantic.v1 import BaseModel, Extra, Field, constr, validator
except ImportError:
    from pydantic import BaseModel, Extra, Field, constr, validator
from typing_extensions import Literal

from qcportal.molecules import Molecule
from qcportal.optimization.record_models import OptimizationSpecification, OptimizationRecord
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.utils import recursive_normalizer


def serialize_key(key: Union[str, Sequence[int]]) -> str:
    """
    Serializes the key used to map to optimization calculations

    A string `key` is used for preoptimization

    Parameters
    ----------
    key
        A string or sequence of integers denoting the position in the grid

    Returns
    -------
    :
        A string representation of the key
    """

    if key == "preoptimization":
        return key

    if isinstance(key, str):
        return key
    else:
        return json.dumps(key)


def deserialize_key(key: str) -> Union[str, Tuple[int, ...]]:
    """
    Deserializes the key used to map to optimization calculations

    This turns the key back into a form usable for creating constraints
    """

    if key == "preoptimization":
        return key

    r = json.loads(key)
    return tuple(r)


class ScanTypeEnum(str, Enum):
    """
    The type of scan to perform. This choices is limited to the scan types allowed by the scan dimensions.
    """

    distance = "distance"
    angle = "angle"
    dihedral = "dihedral"


class StepTypeEnum(str, Enum):
    """
    The types of steps to take in a scan dimension: either in absolute or relative terms. ``relative`` indicates that
    the values are relative to the starting value (e.g., a bond starts as 2.1 Bohr, relative steps of [-0.1, 0, 1.0]
    indicate grid points of [2.0, 2.1, 3.1] Bohr. An ``absolute`` ``step_type`` will be exactly those values instead."
    """

    absolute = "absolute"
    relative = "relative"


class ScanDimension(BaseModel):
    """
    A full description of a dimension to scan over.
    """

    class Config:
        extra = Extra.forbid

    type: ScanTypeEnum = Field(..., description=str(ScanTypeEnum.__doc__))
    indices: List[int] = Field(
        ...,
        description="The indices of atoms to select for the scan. The size of this is a function of the type. e.g., "
        "distances, angles and dihedrals require 2, 3, and 4 atoms, respectively.",
    )
    steps: List[float] = Field(
        ...,
        description="Step sizes to scan in relative to your current location in the scan. This must be a strictly "
        "monotonic series.",
        units=["Bohr", "degrees"],
    )
    step_type: StepTypeEnum = Field(..., description=str(StepTypeEnum.__doc__))

    @validator("type", "step_type", pre=True)
    def check_lower_type_step_type(cls, v):
        return v.lower()

    @validator("indices")
    def check_indices(cls, v, values, **kwargs):
        sizes = {ScanTypeEnum.distance: 2, ScanTypeEnum.angle: 3, ScanTypeEnum.dihedral: 4}
        if sizes[values["type"]] != len(v):
            raise ValueError(
                "ScanDimension of type {} must have {} values, found {}.".format(
                    values["type"], sizes[values["type"]], len(v)
                )
            )

        return v

    @validator("steps")
    def check_steps(cls, v):
        if not (all(x < y for x, y in zip(v, v[1:])) or all(x > y for x, y in zip(v, v[1:]))):
            raise ValueError("Steps are not strictly monotonically increasing or decreasing.")

        v = recursive_normalizer(v)

        return v


class GridoptimizationKeywords(BaseModel):
    """
    Keywords for grid optimizations
    """

    class Config:
        extra = Extra.forbid

    scans: List[ScanDimension] = Field(
        [], description="The dimensions to scan along (along with their options) for the Gridoptimization."
    )
    preoptimization: bool = Field(
        True,
        description="If ``True``, first runs an unrestricted optimization before starting the grid computations. "
        "This is especially useful when combined with ``relative`` ``step_types``.",
    )


class GridoptimizationSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "gridoptimization"
    optimization_specification: OptimizationSpecification
    keywords: GridoptimizationKeywords


class GridoptimizationAddBody(RecordAddBodyBase):
    specification: GridoptimizationSpecification
    initial_molecules: List[Union[int, Molecule]]


class GridoptimizationQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = None
    optimization_program: Optional[List[str]]
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    initial_molecule_id: Optional[List[int]] = None

    @validator("qc_basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


class GridoptimizationOptimization(BaseModel):
    class Config:
        extra = Extra.forbid

    optimization_id: int
    key: str
    energy: Optional[float] = None


class GridoptimizationRecord(BaseRecord):
    record_type: Literal["gridoptimization"] = "gridoptimization"
    specification: GridoptimizationSpecification
    starting_grid: Optional[List[int]]
    initial_molecule_id: int
    starting_molecule_id: Optional[int]

    ######################################################
    # Fields not included when fetching the record
    ######################################################
    initial_molecule_: Optional[Molecule] = None
    starting_molecule_: Optional[Molecule] = None
    optimizations_: Optional[List[GridoptimizationOptimization]] = None

    ########################################
    # Caches
    ########################################
    optimizations_cache_: Optional[Dict[Any, OptimizationRecord]] = None

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self.optimizations_cache_ is not None:
            for opt in self.optimizations_cache_.values():
                opt.propagate_client(client)

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_starting_molecule(self):
        self._assert_online()
        self.starting_molecule_ = self._client.get_molecules([self.starting_molecule_id])[0]

    def _fetch_optimizations(self):
        self._assert_online()

        self.optimizations_ = self._client.make_request(
            "get",
            f"api/v1/records/gridoptimization/{self.id}/optimizations",
            List[GridoptimizationOptimization],
        )

        # Fetch optimization records from the server
        opt_ids = [x.optimization_id for x in self.optimizations_]
        opt_records = self._client.get_optimizations(opt_ids)

        self.optimizations_cache_ = {deserialize_key(x.key): y for x, y in zip(self.optimizations_, opt_records)}

        self.propagate_client(self._client)

    def _handle_includes(self, includes: Optional[Iterable[str]]):
        if includes is None:
            return

        BaseRecord._handle_includes(self, includes)

        if "initial_molecule" in includes:
            self._fetch_initial_molecule()
        if "starting_molecule" in includes:
            self._fetch_starting_molecule()
        if "optimizations" in includes:
            self._fetch_optimizations()

    @property
    def initial_molecule(self) -> Molecule:
        if self.initial_molecule_ is None:
            self._fetch_initial_molecule()
        return self.initial_molecule_

    @property
    def starting_molecule(self) -> Optional[Molecule]:
        if self.starting_molecule_ is None:
            self._fetch_starting_molecule()
        return self.starting_molecule_

    @property
    def optimizations(self) -> Dict[Any, OptimizationRecord]:
        if self.optimizations_cache_ is None:
            self._fetch_optimizations()
        return self.optimizations_cache_

    @property
    def preoptimization(self) -> Optional[OptimizationRecord]:
        if self.optimizations_cache_ is None:
            self._fetch_optimizations()
        return self.optimizations_cache_.get("preoptimization", None)

    @property
    def final_energies(self) -> Dict[Tuple[int, ...], float]:
        return {k: v.energies[-1] for k, v in self.optimizations.items() if v.energies}
