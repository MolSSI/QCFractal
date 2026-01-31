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
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters, compare_base_records
from qcportal.cache import get_records_with_cache
from qcportal.utils import recursive_normalizer, is_included
from ..optimization.record_models import OptimizationSpecification, OptimizationRecord, compare_optimization_records


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

    ##############################################
    # Fields with child records
    # (generally not received from the server)
    ##############################################
    optimization_records_: Optional[Dict[str, List[OptimizationRecord]]] = Field(None, alias="optimization_records")

    # Actual mapping, with tuples as keys. These will point to the same lists & records as above
    _optimization_map: Optional[Dict[Any, List[OptimizationRecord]]] = PrivateAttr(None)
    _minimum_optimization_map: Optional[Dict[Any, OptimizationRecord]] = PrivateAttr(None)

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

        base_url_prefix = next(iter(records))._base_url_prefix
        assert all(r._base_url_prefix == base_url_prefix for r in records)

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
            client, base_url_prefix, record_cache, OptimizationRecord, opt_ids, include=include, force_fetch=force_fetch
        )
        opt_map = {x.id: x for x in opt_records}

        for r in records:
            if do_opt:
                r.optimization_records_ = None
                r._optimization_map = None
            if do_minopt or do_opt:
                r._minimum_optimization_map = None

            if r.optimizations_ is None and r.minimum_optimizations_ is None:
                # Bail out early, leaving those fields as None
                continue

            if do_opt and r.optimizations_ is not None:
                r.optimization_records_ = {}
                r._optimization_map = {}
                for td_opt in r.optimizations_:
                    # optimization_records_ uses the string key
                    r.optimization_records_.setdefault(td_opt.key, list())
                    r.optimization_records_[td_opt.key].append(opt_map[td_opt.optimization_id])

                    # maps use tuples or strings
                    key = deserialize_key(td_opt.key)
                    r._optimization_map.setdefault(key, list())
                    r._optimization_map[key].append(opt_map[td_opt.optimization_id])

            if r.minimum_optimizations_ is None and r.optimizations_ is not None and do_opt:
                # find the minimum optimizations for each key from what we have in the optimizations
                # chooses the lowest id if there are records with the same energy
                r.minimum_optimizations_ = {}
                for str_key, rec_list in r.optimization_records_.items():
                    # Remove any optimizations without energies
                    v2 = [x for x in rec_list if x.energies]
                    if v2:
                        lowest_opt = min(v2, key=lambda x: (x.energies[-1], x.id))
                        r.minimum_optimizations_[str_key] = lowest_opt.id

            if (do_minopt or do_opt) and r.minimum_optimizations_ is not None:  # either from the server or from above
                r._minimum_optimization_map = {
                    deserialize_key(str_key): opt_map[opt_id] for str_key, opt_id in r.minimum_optimizations_.items()
                }

            r.propagate_client(r._client, base_url_prefix)

    def propagate_client(self, client, base_url_prefix: Optional[str]):
        BaseRecord.propagate_client(self, client, base_url_prefix)

        if self.optimization_records_ is not None:
            for opts in self.optimization_records_.values():
                for opt in opts:
                    opt.propagate_client(client, base_url_prefix)

        # But may need to do minimum optimizations map, since they may have been obtained separately
        if self._minimum_optimization_map is not None:
            for opt in self._minimum_optimization_map.values():
                opt.propagate_client(client, base_url_prefix)

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

    def get_cache_dict(self, **kwargs) -> Dict[str, Any]:
        return self.dict(exclude={"optimization_records_"}, **kwargs)

    @property
    def initial_molecules(self) -> List[Molecule]:
        if self.initial_molecules_ is None:
            self._fetch_initial_molecules()
        return self.initial_molecules_

    @property
    def optimizations(self) -> Dict[str, List[OptimizationRecord]]:
        if self.optimization_records_ is None:
            self._fetch_optimizations()

        if self._optimization_map is None:
            self._optimization_map = {deserialize_key(k): v for k, v in self.optimization_records_.items()}

        return self._optimization_map

    @property
    def minimum_optimizations(self) -> Dict[Tuple[float, ...], OptimizationRecord]:
        if (
            self._minimum_optimization_map is None
            and self.minimum_optimizations_ is not None
            and self.optimization_records_ is not None
        ):
            opt_map = {}
            for opt_records in self.optimization_records_.values():
                opt_map.update({x.id: x for x in opt_records})

            self._minimum_optimization_map = {
                deserialize_key(k): opt_map[v] for k, v in self.minimum_optimizations_.items()
            }

        elif self._minimum_optimization_map is None:
            self._fetch_minimum_optimizations()

        return self._minimum_optimization_map

    @property
    def final_energies(self) -> Dict[Tuple[float, ...], float]:
        return {k: v.energies[-1] for k, v in self.minimum_optimizations.items() if v.energies}


def compare_torsiondrive_records(record_1: TorsiondriveRecord, record_2: TorsiondriveRecord):
    compare_base_records(record_1, record_2)

    assert len(record_1.initial_molecules) == len(record_2.initial_molecules)
    molecules_1 = sorted(record_1.initial_molecules, key=lambda x: x.get_hash())
    molecules_2 = sorted(record_2.initial_molecules, key=lambda x: x.get_hash())
    assert all(x == y for x, y in zip(molecules_1, molecules_2))

    if record_1.optimizations is not None:
        assert len(record_1.optimizations) == len(record_2.optimizations)
        for k, t1_lst in record_1.optimizations.items():
            t2_lst = record_2.optimizations[k]
            assert len(t1_lst) == len(t2_lst)
            for t1, t2 in zip(t1_lst, t2_lst):
                compare_optimization_records(t1, t2)

        if record_1.status == "complete":
            for k, t1 in record_1.minimum_optimizations.items():
                t2 = record_2.minimum_optimizations[k]
                compare_optimization_records(t1, t2)
