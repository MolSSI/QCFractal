import json
from typing import List, Optional, Tuple, Union, Dict, Iterable, Sequence, Any

try:
    from pydantic.v1 import BaseModel, Field, Extra, root_validator, constr, validator
except ImportError:
    from pydantic import BaseModel, Field, Extra, root_validator, constr, validator
from typing_extensions import Literal

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
    # Fields not included when fetching the record
    ######################################################
    initial_molecules_ids_: Optional[List[int]] = None
    optimizations_: Optional[List[TorsiondriveOptimization]] = None

    ########################################
    # Caches
    ########################################
    initial_molecules_: Optional[List[Molecule]] = None
    optimizations_cache_: Optional[Dict[Any, List[OptimizationRecord]]] = None
    minimum_optimizations_cache_: Optional[Dict[Any, OptimizationRecord]] = None

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self.optimizations_cache_ is not None:
            for opts in self.optimizations_cache_.values():
                for opt in opts:
                    opt.propagate_client(client)

        # But may need to do minimum_optimizations_cache_, since they may have been obtained separately
        if self.minimum_optimizations_cache_ is not None:
            for opt in self.minimum_optimizations_cache_.values():
                opt.propagate_client(client)

    def _fetch_initial_molecules(self):
        self._assert_online()

        self.initial_molecules_ids_ = self._client.make_request(
            "get",
            f"api/v1/records/torsiondrive/{self.id}/initial_molecules",
            List[int],
        )

        self.initial_molecules_ = self._client.get_molecules(self.initial_molecules_ids_)

    def _fetch_optimizations(self):
        self._assert_online()

        self.optimizations_ = self._client.make_request(
            "get",
            f"api/v1/records/torsiondrive/{self.id}/optimizations",
            List[TorsiondriveOptimization],
        )

        # Fetch optimization records from the server
        opt_ids = [x.optimization_id for x in self.optimizations_]
        opt_records = self._client.get_optimizations(opt_ids)

        self.optimizations_cache_ = {}
        for td_opt, opt_record in zip(self.optimizations_, opt_records):
            key = deserialize_key(td_opt.key)
            self.optimizations_cache_.setdefault(key, list())
            self.optimizations_cache_[key].append(opt_record)

        # find the minimum optimizations for each key
        # chooses the lowest id if there are records with the same energy
        self.minimum_optimizations_cache_ = {}
        for k, v in self.optimizations_cache_.items():
            # Remove any optimizations without energies
            v2 = [x for x in v if x.energies]
            if v2:
                self.minimum_optimizations_cache_[k] = min(v2, key=lambda x: (x.energies[-1], x.id))

        self.propagate_client(self._client)

    def _fetch_minimum_optimizations(self):
        self._assert_online()

        min_opt_ids = self._client.make_request(
            "get",
            f"api/v1/records/torsiondrive/{self.id}/minimum_optimizations",
            Dict[str, int],
        )

        # Fetch optimization records from the server
        opt_key_ids = list(min_opt_ids.items())
        opt_ids = [x[1] for x in opt_key_ids]
        opt_records = self._client.get_optimizations(opt_ids)

        self.minimum_optimizations_cache_ = {deserialize_key(x[0]): y for x, y in zip(opt_key_ids, opt_records)}

        self.propagate_client(self._client)

    def _handle_includes(self, includes: Optional[Iterable[str]]):
        if includes is None:
            return

        BaseRecord._handle_includes(self, includes)

        if "initial_molecules" in includes:
            self._fetch_initial_molecules()
        if "minimum_optimizations" in includes and "optimizations" not in includes:
            self._fetch_minimum_optimizations()
        if "optimizations" in includes:
            self._fetch_optimizations()

    @property
    def initial_molecules(self) -> List[Molecule]:
        if self.initial_molecules_ is None:
            self._fetch_initial_molecules()
        return self.initial_molecules_

    @property
    def optimizations(self) -> Dict[str, List[OptimizationRecord]]:
        if self.optimizations_cache_ is None:
            self._fetch_optimizations()

        return self.optimizations_cache_

    @property
    def minimum_optimizations(self) -> Dict[Tuple[float, ...], OptimizationRecord]:
        if self.minimum_optimizations_cache_ is None:
            self._fetch_minimum_optimizations()

        return self.minimum_optimizations_cache_

    @property
    def final_energies(self) -> Dict[Tuple[float, ...], float]:
        return {k: v.energies[-1] for k, v in self.minimum_optimizations.items() if v.energies}
