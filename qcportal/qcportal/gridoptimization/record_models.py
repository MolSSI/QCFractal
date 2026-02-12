from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator, PrivateAttr

from qcportal.base_models import RestModelBase
from qcportal.cache import get_records_with_cache
from qcportal.common_types import LowerStr
from qcportal.molecules import Molecule
from qcportal.optimization.record_models import (
    OptimizationSpecification,
    OptimizationRecord,
    compare_optimization_records,
)
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters, compare_base_records
from qcportal.utils import recursive_normalizer, is_included


def serialize_key(key: str | Sequence[int]) -> str:
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


def deserialize_key(key: str) -> str | tuple[int, ...]:
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
    The type of scan to perform. This choice is limited to the scan types allowed by the scan dimensions.
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

    model_config = ConfigDict(extra="forbid")

    type: ScanTypeEnum = Field(..., description=str(ScanTypeEnum.__doc__))
    indices: list[int] = Field(
        ...,
        description="The indices of atoms to select for the scan. The size of this is a function of the type. e.g., "
        "distances, angles and dihedrals require 2, 3, and 4 atoms, respectively.",
    )
    steps: list[float] = Field(
        ...,
        description="Step sizes to scan in relative to your current location in the scan. This must be a strictly "
        "monotonic series.",
        json_schema_extra={"units": ["Bohr", "degrees"]},
    )
    step_type: StepTypeEnum = Field(..., description=str(StepTypeEnum.__doc__))

    @field_validator("type", "step_type", mode="before")
    @classmethod
    def check_lower_type_step_type(cls, v):
        return v.lower()

    @model_validator(mode="after")
    def check_indices(self):
        sizes = {ScanTypeEnum.distance: 2, ScanTypeEnum.angle: 3, ScanTypeEnum.dihedral: 4}
        if sizes[self.type] != len(self.indices):
            raise ValueError(
                "ScanDimension of type {} must have {} values, found {}.".format(
                    self.type, sizes[self.type], len(self.indices)
                )
            )

        if not (
            all(x < y for x, y in zip(self.steps, self.steps[1:]))
            or all(x > y for x, y in zip(self.steps, self.steps[1:]))
        ):
            raise ValueError("Steps are not strictly monotonically increasing or decreasing.")

        self.steps = recursive_normalizer(self.steps)

        return self


class GridoptimizationKeywords(BaseModel):
    """
    Keywords for grid optimizations
    """

    model_config = ConfigDict(extra="forbid")

    scans: list[ScanDimension] = Field(
        [], description="The dimensions to scan along (along with their options) for the Gridoptimization."
    )
    preoptimization: bool = Field(
        True,
        description="If ``True``, first runs an unrestricted optimization before starting the grid computations. "
        "This is especially useful when combined with ``relative`` ``step_types``.",
    )


class GridoptimizationSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    program: LowerStr = "gridoptimization"
    optimization_specification: OptimizationSpecification
    keywords: GridoptimizationKeywords


class GridoptimizationInput(RestModelBase):
    record_type: Literal["gridoptimization"] = "gridoptimization"
    specification: GridoptimizationSpecification
    initial_molecule: int | Molecule


class GridoptimizationMultiInput(RestModelBase):
    specification: GridoptimizationSpecification
    initial_molecules: list[int | Molecule]


class GridoptimizationAddBody(RecordAddBodyBase, GridoptimizationMultiInput):
    pass


class GridoptimizationQueryFilters(RecordQueryFilters):
    program: list[str] | None = None
    optimization_program: list[str] | None
    qc_program: list[LowerStr] | None = None
    qc_method: list[LowerStr] | None = None
    qc_basis: list[LowerStr | None] | None = None
    initial_molecule_id: list[int] | None = None

    @field_validator("qc_basis")
    @classmethod
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


class GridoptimizationOptimization(BaseModel):
    model_config = ConfigDict(extra="forbid")

    optimization_id: int
    key: str
    energy: float | None = None


class GridoptimizationRecord(BaseRecord):
    record_type: Literal["gridoptimization"] = "gridoptimization"
    specification: GridoptimizationSpecification
    starting_grid: list[int] | None
    initial_molecule_id: int
    starting_molecule_id: int | None

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    initial_molecule_: Molecule | None = Field(None, alias="initial_molecule")
    starting_molecule_: Molecule | None = Field(None, alias="starting_molecule")
    optimizations_: list[GridoptimizationOptimization] | None = Field(None, alias="optimizations")

    ##############################################
    # Fields with child records
    # (generally not received from the server)
    ##############################################
    optimization_records_: dict[Any, OptimizationRecord] | None = Field(None, alias="optimizations_records")

    # Actual mapping, with tuples as keys. These will point to the same lists & records as above
    _optimization_map: dict[Any, OptimizationRecord] | None = PrivateAttr(None)

    def propagate_client(self, client, base_url_prefix: str | None):
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
                list[GridoptimizationOptimization],
            )

        self.fetch_children(["optimizations"])

    def get_cache_dict(self, **kwargs) -> dict[str, Any]:
        return self.model_dump(exclude={"optimization_records_"}, **kwargs)

    @property
    def initial_molecule(self) -> Molecule:
        if self.initial_molecule_ is None:
            self._fetch_initial_molecule()
        return self.initial_molecule_

    @property
    def starting_molecule(self) -> Molecule | None:
        if self.starting_molecule_ is None:
            self._fetch_starting_molecule()
        return self.starting_molecule_

    @property
    def optimizations(self) -> dict[Any, OptimizationRecord]:
        if self.optimization_records_ is None:
            self._fetch_optimizations()

        if self._optimization_map is None:
            self._optimization_map = {deserialize_key(k): v for k, v in self.optimization_records_.items()}

        return self._optimization_map

    @property
    def preoptimization(self) -> OptimizationRecord | None:
        if self.optimization_records_ is None:
            self._fetch_optimizations()
        return self.optimization_records_.get("preoptimization", None)

    @property
    def final_energies(self) -> dict[tuple[int, ...], float]:
        return {k: v.energies[-1] for k, v in self.optimizations.items() if v.energies}


def compare_gridoptimization_records(record_1: GridoptimizationRecord, record_2: GridoptimizationRecord):
    compare_base_records(record_1, record_2)

    assert record_1.initial_molecule == record_2.initial_molecule
    assert record_1.starting_molecule == record_2.starting_molecule
    assert record_1.starting_grid == record_2.starting_grid

    assert (record_1.optimizations is None) == (record_2.optimizations is None)

    if record_1.optimizations is not None:
        assert len(record_1.optimizations) == len(record_2.optimizations)
        for k, t1 in record_1.optimization_records_.items():
            t2 = record_2.optimization_records_[k]
            compare_optimization_records(t1, t2)
