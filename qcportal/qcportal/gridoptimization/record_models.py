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
from qcportal.optimization.record_models import (
    OptimizationSpecification,
    OptimizationRecord,
    compare_optimization_records,
)
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters, compare_base_records
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

    ##############################################
    # Fields with child records
    # (generally not received from the server)
    ##############################################
    optimization_records_: Optional[Dict[Any, OptimizationRecord]] = Field(None, alias="optimizations_records")

    # Actual mapping, with tuples as keys. These will point to the same lists & records as above
    _optimization_map: Optional[Dict[Any, OptimizationRecord]] = PrivateAttr(None)

    def propagate_client(self, client, base_url_prefix: Optional[str]):
        BaseRecord.propagate_client(self, client, base_url_prefix)

        if self.optimization_records_ is not None:
            for opt in self.optimization_records_.values():
                opt.propagate_client(client, base_url_prefix)

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

        base_url_prefix = next(iter(records))._base_url_prefix
        assert all(r._base_url_prefix == base_url_prefix for r in records)

        # Collect optimization id for all grid optimizations
        if is_included("optimizations", include, None, False):
            opt_ids = set()
            for r in records:
                if r.optimizations_:
                    opt_ids.update(x.optimization_id for x in r.optimizations_)

            opt_ids = list(opt_ids)
            opt_records = get_records_with_cache(
                client,
                base_url_prefix,
                record_cache,
                OptimizationRecord,
                opt_ids,
                include=include,
                force_fetch=force_fetch,
            )
            opt_map = {x.id: x for x in opt_records}

            for r in records:
                if r.optimizations_ is None:
                    r._optimization_map = None
                else:
                    r.optimization_records_ = {
                        go_opt.key: opt_map[go_opt.optimization_id] for go_opt in r.optimizations_
                    }
                    r._optimization_map = {deserialize_key(k): v for k, v in r.optimization_records_.items()}

                r.propagate_client(r._client, base_url_prefix)

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

    def get_cache_dict(self, **kwargs) -> Dict[str, Any]:
        return self.dict(exclude={"optimization_records_"}, **kwargs)

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
        if self.optimization_records_ is None:
            self._fetch_optimizations()

        if self._optimization_map is None:
            self._optimization_map = {deserialize_key(k): v for k, v in self.optimization_records_.items()}

        return self._optimization_map

    @property
    def preoptimization(self) -> Optional[OptimizationRecord]:
        if self.optimization_records_ is None:
            self._fetch_optimizations()
        return self.optimization_records_.get("preoptimization", None)

    @property
    def final_energies(self) -> Dict[Tuple[int, ...], float]:
        return {k: v.energies[-1] for k, v in self.optimizations.items() if v.energies}


def compare_gridoptimization_records(record_1: GridoptimizationRecord, record_2: GridoptimizationRecord):
    compare_base_records(record_1, record_2)

    assert record_1.initial_molecule.get_hash() == record_2.initial_molecule.get_hash()
    assert record_1.starting_molecule.get_hash() == record_2.starting_molecule.get_hash()
    assert record_1.starting_grid == record_2.starting_grid

    assert (record_1.optimization_records_ is None) == (record_2.optimizations_ is None)

    if record_1.optimization_records_ is not None:
        assert len(record_1.optimization_records_) == len(record_2.optimization_records_)
        for k, t1 in record_1.optimization_records_.items():
            t2 = record_2.optimization_records_[k]
            compare_optimization_records(t1, t2)
