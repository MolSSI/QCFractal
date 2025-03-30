from __future__ import annotations

import json
from enum import Enum
from typing import List, Union, Optional, Dict, Iterable, Tuple, Sequence, Any

try:
    from pydantic.v1 import BaseModel, Extra, Field, constr, validator, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Extra, Field, constr, validator, PrivateAttr
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.optimization.record_models import OptimizationSpecification, OptimizationRecord
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.utils import recursive_normalizer, is_included
from qcportal.cache import get_records_with_cache


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


class GridoptimizationInput(RestModelBase):
    record_type: Literal["gridoptimization"] = "gridoptimization"
    specification: GridoptimizationSpecification
    initial_molecule: Union[int, Molecule]


class GridoptimizationMultiInput(RestModelBase):
    specification: GridoptimizationSpecification
    initial_molecules: List[Union[int, Molecule]]


class GridoptimizationAddBody(RecordAddBodyBase, GridoptimizationMultiInput):
    pass


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
    # Fields not always included when fetching the record
    ######################################################
    initial_molecule_: Optional[Molecule] = Field(None, alias="initial_molecule")
    starting_molecule_: Optional[Molecule] = Field(None, alias="starting_molecule")
    optimizations_: Optional[List[GridoptimizationOptimization]] = Field(None, alias="optimizations")

    ########################################
    # Caches
    ########################################
    _optimizations_cache: Optional[Dict[Any, OptimizationRecord]] = PrivateAttr(None)

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self._optimizations_cache is not None:
            for opt in self._optimizations_cache.values():
                opt.propagate_client(client)

    @classmethod
    def _fetch_children_multi(
        cls,
        client,
        record_cache,
        records: Iterable[GridoptimizationRecord],
        include: Iterable[str],
        force_fetch: bool = False,
    ):
        # Should be checked by the calling function
        assert records
        assert all(isinstance(x, GridoptimizationRecord) for x in records)

        # Collect optimization id for all grid optimizations
        if is_included("optimizations", include, None, False):
            opt_ids = set()
            for r in records:
                if r.optimizations_:
                    opt_ids.update(x.optimization_id for x in r.optimizations_)

            opt_ids = list(opt_ids)
            opt_records = get_records_with_cache(
                client, record_cache, OptimizationRecord, opt_ids, include=include, force_fetch=force_fetch
            )
            opt_map = {x.id: x for x in opt_records}

            for r in records:
                if r.optimizations_ is None:
                    r._optimizations_cache = None
                else:
                    r._optimizations_cache = {}
                    for go_opt in r.optimizations_:
                        key = deserialize_key(go_opt.key)
                        r._optimizations_cache[key] = opt_map[go_opt.optimization_id]

                r.propagate_client(r._client)

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_starting_molecule(self):
        self._assert_online()
        self.starting_molecule_ = self._client.get_molecules([self.starting_molecule_id])[0]

    def _fetch_optimizations(self):
        if self.optimizations_ is None:
            self._assert_online()
            self.optimizations_ = self._client.make_request(
                "get",
                f"api/v1/records/gridoptimization/{self.id}/optimizations",
                List[GridoptimizationOptimization],
            )

        self.fetch_children(["optimizations"])

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
        if self._optimizations_cache is None:
            self._fetch_optimizations()
        return self._optimizations_cache

    @property
    def preoptimization(self) -> Optional[OptimizationRecord]:
        if self._optimizations_cache is None:
            self._fetch_optimizations()
        return self._optimizations_cache.get("preoptimization", None)

    @property
    def final_energies(self) -> Dict[Tuple[int, ...], float]:
        return {k: v.energies[-1] for k, v in self.optimizations.items() if v.energies}
