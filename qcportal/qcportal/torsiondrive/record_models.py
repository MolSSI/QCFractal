import json
from typing import List, Optional, Tuple, Union, Dict, Set, Iterable, Sequence, Any

from pydantic import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
from typing_extensions import Literal

from qcportal.base_models import ProjURLParameters
from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.utils import recursive_normalizer
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
    optimization_record: Optional[OptimizationRecord]


class TorsiondriveAddBody(RecordAddBodyBase):
    specification: TorsiondriveSpecification
    initial_molecules: List[List[Union[int, Molecule]]]
    as_service: bool


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
    initial_molecules_: Optional[List[Molecule]] = Field(None, alias="initial_molecules")
    optimizations_: Optional[List[TorsiondriveOptimization]] = Field(None, alias="optimizations")

    ########################################
    # Private caches
    ########################################
    _optimizations_cache: Optional[Dict[Any, List[OptimizationRecord]]] = PrivateAttr(None)
    _minimum_optimizations_cache: Optional[Dict[Any, OptimizationRecord]] = PrivateAttr(None)

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:

        if includes is None:
            return None

        ret = BaseRecord.transform_includes(includes)

        if "initial_molecules" in includes:
            ret.add("initial_molecules")
        if "optimizations" in includes:
            ret |= {"optimizations.*", "optimizations.optimization_record"}

        return ret

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        # Don't need to do _optimizations_cache. Those should point back to the records in optimizations_
        if self.optimizations_ is not None:
            for opt in self.optimizations_:
                if opt.optimization_record:
                    opt.optimization_record.propagate_client(client)

        # But may need to do _minimum_optimizations_cache, since they may have been obtained separately
        if self._minimum_optimizations_cache is not None:
            for opt in self._minimum_optimizations_cache.values():
                if opt:
                    opt.propagate_client(client)

    def make_caches(self):
        BaseRecord.make_caches(self)

        if self.optimizations_ is None:
            self._optimizations_cache = None
            self._minimum_optimizations_cache = None
            return

        self._optimizations_cache = {}
        self._minimum_optimizations_cache = {}

        # convert the raw optimization data to a dictionary of key -> List[OptimizationRecord]
        for opt in self.optimizations_:
            opt_key = deserialize_key(opt.key)
            self._optimizations_cache.setdefault(opt_key, list())
            self._optimizations_cache[opt_key].append(opt.optimization_record)

        # find the minimum optimizations for each key
        # chooses the lowest id if there are records with the same energy
        for k, v in self._optimizations_cache.items():
            # Remove any optimizations without energies
            v2 = [x for x in v if x.energies]
            if v2:
                self._minimum_optimizations_cache[k] = min(v2, key=lambda x: (x.energies[-1], x.id))

    def _fetch_initial_molecules(self):
        self._assert_online()

        self.initial_molecules_ = self._client._auto_request(
            "get",
            f"v1/records/torsiondrive/{self.id}/initial_molecules",
            None,
            None,
            List[Molecule],
            None,
            None,
        )

    def _fetch_optimizations(self):
        self._assert_online()

        url_params = {"include": ["*", "optimization_record"]}

        self.optimizations_ = self._client._auto_request(
            "get",
            f"v1/records/torsiondrive/{self.id}/optimizations",
            None,
            ProjURLParameters,
            List[TorsiondriveOptimization],
            None,
            url_params,
        )

        self.make_caches()
        self.propagate_client(self._client)

    def _fetch_minimum_optimizations(self):
        self._assert_online()

        url_params = {}

        min_opt = self._client._auto_request(
            "get",
            f"v1/records/torsiondrive/{self.id}/minimum_optimizations",
            None,
            ProjURLParameters,
            Dict[str, OptimizationRecord],
            None,
            url_params,
        )

        self._minimum_optimizations_cache = {}
        for key, opt in min_opt.items():
            self._minimum_optimizations_cache[deserialize_key(key)] = opt

        self.propagate_client(self._client)

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
