from __future__ import annotations

import json
from typing import List, Optional, Tuple, Union, Dict, Iterable, Sequence, Any

try:
    from pydantic.v1 import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.cache import get_records_with_cache
from qcportal.utils import recursive_normalizer, is_included
from ..optimization.record_models import OptimizationSpecification, OptimizationRecord


def serialize_key(key: Union[str, Sequence[int]]) -> str:
    """
    Serializes the key used to map to optimization calculations

    Parameters
    ----------
    key
        A string or sequence of integers denoting the position in the grid

    Returns
    -------
    :
        A string representation of the key
    """

    return json.dumps(key)


def deserialize_key(key: str) -> Union[str, Tuple[int, ...]]:
    """
    Deserializes the key used to map to optimization calculations

    This turns the key back into a form usable for creating constraints
    """

    r = json.loads(key)
    return tuple(r)


class TorsiondriveKeywords(BaseModel):
    """
    Options for torsiondrive calculations
    """

    class Config:
        extra = Extra.forbid

    dihedrals: List[Tuple[int, int, int, int]] = Field(
        [],
        description="The list of dihedrals to select for the TorsionDrive operation. Each entry is a tuple of integers "
        "of for particle indices.",
    )
    grid_spacing: List[int] = Field(
        [],
        description="List of grid spacing for dihedral scan in degrees. Multiple values will be mapped to each "
        "dihedral angle.",
    )
    dihedral_ranges: Optional[List[Tuple[int, int]]] = Field(
        None,
        description="A list of dihedral range limits as a pair (lower, upper). "
        "Each range corresponds to the dihedrals in input.",
    )
    energy_decrease_thresh: Optional[float] = Field(
        None,
        description="The threshold of the smallest energy decrease amount to trigger activating optimizations from "
        "grid point.",
    )
    energy_upper_limit: Optional[float] = Field(
        None,
        description="The threshold if the energy of a grid point that is higher than the current global minimum, to "
        "start new optimizations, in unit of a.u. I.e. if energy_upper_limit = 0.05, current global "
        "minimum energy is -9.9 , then a new task starting with energy -9.8 will be skipped.",
    )

    @root_validator
    def normalize(cls, values):
        return recursive_normalizer(values)


class TorsiondriveSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "torsiondrive"
    optimization_specification: OptimizationSpecification
    keywords: TorsiondriveKeywords


class TorsiondriveOptimization(BaseModel):
    class Config:
        extra = Extra.forbid

    optimization_id: int
    key: str
    position: int
    energy: Optional[float] = None


class TorsiondriveInput(RestModelBase):
    record_type: Literal["torsiondrive"] = "torsiondrive"
    specification: TorsiondriveSpecification
    initial_molecules: List[Union[int, Molecule]]
    as_service: bool


class TorsiondriveMultiInput(RestModelBase):
    specification: TorsiondriveSpecification
    initial_molecules: List[List[Union[int, Molecule]]]
    as_service: bool


class TorsiondriveAddBody(RecordAddBodyBase, TorsiondriveMultiInput):
    pass


class TorsiondriveQueryFilters(RecordQueryFilters):
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


class TorsiondriveRecord(BaseRecord):
    record_type: Literal["torsiondrive"] = "torsiondrive"
    specification: TorsiondriveSpecification

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    initial_molecules_ids_: Optional[List[int]] = Field(None, alias="initial_molecules_ids")
    initial_molecules_: Optional[List[Molecule]] = Field(None, alias="initial_molecules")

    optimizations_: Optional[List[TorsiondriveOptimization]] = Field(None, alias="optimizations")
    minimum_optimizations_: Optional[Dict[str, int]] = Field(None, alias="minimum_optimizations")

    ########################################
    # Caches
    ########################################
    _optimizations_cache: Optional[Dict[Any, List[OptimizationRecord]]] = PrivateAttr(None)
    _minimum_optimizations_cache: Optional[Dict[Any, OptimizationRecord]] = PrivateAttr(None)

    @classmethod
    def _fetch_children_multi(
        cls,
        client,
        record_cache,
        records: Iterable[TorsiondriveRecord],
        include: Iterable[str],
        force_fetch: bool = False,
    ):
        # Should be checked by the calling function
        assert records
        assert all(isinstance(x, TorsiondriveRecord) for x in records)

        do_opt = is_included("optimizations", include, None, False)
        do_minopt = is_included("minimum_optimizations", include, None, False)

        if not do_opt and not do_minopt:
            return

        # Collect optimization id for all torsiondrives
        opt_ids = set()
        for r in records:
            if r.optimizations_ and do_opt:
                opt_ids.update(x.optimization_id for x in r.optimizations_)
            if r.minimum_optimizations_ and do_minopt:
                opt_ids.update(r.minimum_optimizations_.values())

        opt_ids = list(opt_ids)
        opt_records = get_records_with_cache(
            client, record_cache, OptimizationRecord, opt_ids, include=include, force_fetch=force_fetch
        )
        opt_map = {x.id: x for x in opt_records}

        for r in records:
            if do_opt:
                r._optimizations_cache = None
            if do_minopt or do_opt:
                r._minimum_optimizations_cache = None

            if r.optimizations_ is None and r.minimum_optimizations_ is None:
                continue

            if do_opt and r.optimizations_ is not None:
                r._optimizations_cache = {}
                for td_opt in r.optimizations_:
                    key = deserialize_key(td_opt.key)
                    r._optimizations_cache.setdefault(key, list())
                    r._optimizations_cache[key].append(opt_map[td_opt.optimization_id])

            if r.minimum_optimizations_ is None and r.optimizations_ is not None and do_opt:
                # find the minimum optimizations for each key from what we have in the optimizations
                # chooses the lowest id if there are records with the same energy
                r.minimum_optimizations_ = {}
                for k, v in r._optimizations_cache.items():
                    # Remove any optimizations without energies
                    v2 = [x for x in v if x.energies]
                    if v2:
                        lowest_opt = min(v2, key=lambda x: (x.energies[-1], x.id))
                        r.minimum_optimizations_[serialize_key(k)] = lowest_opt.id

            if do_minopt or do_opt and r.minimum_optimizations_ is not None:  # either from the server or from above
                r._minimum_optimizations_cache = {
                    deserialize_key(k): opt_map[v] for k, v in r.minimum_optimizations_.items()
                }

            r.propagate_client(r._client)

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self._optimizations_cache is not None:
            for opts in self._optimizations_cache.values():
                for opt in opts:
                    opt.propagate_client(client)

        # But may need to do minimum_optimizations_cache_, since they may have been obtained separately
        if self._minimum_optimizations_cache is not None:
            for opt in self._minimum_optimizations_cache.values():
                opt.propagate_client(client)

    def _fetch_initial_molecules(self):
        self._assert_online()
        if self.initial_molecules_ids_ is None:
            self.initial_molecules_ids_ = self._client.make_request(
                "get",
                f"api/v1/records/torsiondrive/{self.id}/initial_molecules",
                List[int],
            )

        self.initial_molecules_ = self._client.get_molecules(self.initial_molecules_ids_)

    def _fetch_optimizations(self):
        if self.optimizations_ is None:
            self._assert_online()
            self.optimizations_ = self._client.make_request(
                "get",
                f"api/v1/records/torsiondrive/{self.id}/optimizations",
                List[TorsiondriveOptimization],
            )

        self.fetch_children(["optimizations"])

    def _fetch_minimum_optimizations(self):
        if self.minimum_optimizations_ is None:
            self._assert_online()
            self.minimum_optimizations_ = self._client.make_request(
                "get",
                f"api/v1/records/torsiondrive/{self.id}/minimum_optimizations",
                Dict[str, int],
            )

        self.fetch_children(["minimum_optimizations"])

    @property
    def initial_molecules(self) -> List[Molecule]:
        if self.initial_molecules_ is None:
            self._fetch_initial_molecules()
        return self.initial_molecules_

    @property
    def optimizations(self) -> Dict[str, List[OptimizationRecord]]:
        if self._optimizations_cache is None:
            self._fetch_optimizations()

        return self._optimizations_cache

    @property
    def minimum_optimizations(self) -> Dict[Tuple[float, ...], OptimizationRecord]:
        if self._minimum_optimizations_cache is None:
            self._fetch_minimum_optimizations()

        return self._minimum_optimizations_cache

    @property
    def final_energies(self) -> Dict[Tuple[float, ...], float]:
        return {k: v.energies[-1] for k, v in self.minimum_optimizations.items() if v.energies}
