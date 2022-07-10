from typing import List, Optional, Tuple, Union, Dict, Set, Iterable

from pydantic import BaseModel, Field, Extra, root_validator, constr, validator
from typing_extensions import Literal

from ..models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from ..optimization.models import OptimizationSpecification, OptimizationRecord
from ...base_models import ProjURLParameters
from ...molecules import Molecule
from ...utils import recursive_normalizer


class TorsiondriveKeywords(BaseModel):
    """
    TorsionDriveRecord options
    """

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
    optimization_record: Optional[OptimizationRecord._DataModel]


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
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["torsiondrive"] = "torsiondrive"
        specification: TorsiondriveSpecification
        initial_molecules: Optional[List[Molecule]] = None
        optimizations: Optional[List[TorsiondriveOptimization]] = None

        # These hold actual optimization records
        optimizations_cache: Optional[Dict[str, List[OptimizationRecord]]] = None
        minimum_optimizations_cache: Optional[Dict[str, OptimizationRecord]] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["torsiondrive"] = "torsiondrive"
    raw_data: _DataModel

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

    def _make_caches(self):
        if self.raw_data.optimizations is None:
            return

        if self.raw_data.optimizations_cache is None:
            # convert the raw optimization data to a dictionary of key -> List[OptimizationRecord]
            opt_map = {}
            for opt in self.raw_data.optimizations:
                opt_map.setdefault(opt.key, list())
                opt_map[opt.key].append(OptimizationRecord.from_datamodel(opt.optimization_record, self.client))

            self.raw_data.optimizations_cache = opt_map

        # find the minimum optimizations for each key
        if self.raw_data.minimum_optimizations_cache is None:

            # chooses the lowest id if there are records with the same energy
            self.raw_data.minimum_optimizations_cache = {
                k: min(v, key=lambda x: (x.energies[-1], x.id)) for k, v in self.raw_data.optimizations_cache.items()
            }

    def _fetch_initial_molecules(self):
        self._assert_online()

        self.raw_data.initial_molecules = self.client._auto_request(
            "get",
            f"v1/records/torsiondrive/{self.raw_data.id}/initial_molecules",
            None,
            None,
            List[Molecule],
            None,
            None,
        )

    def _fetch_optimizations(self):
        self._assert_online()

        url_params = {"include": ["*", "optimization_record"]}

        self.raw_data.optimizations = self.client._auto_request(
            "get",
            f"v1/records/torsiondrive/{self.raw_data.id}/optimizations",
            None,
            ProjURLParameters,
            List[TorsiondriveOptimization],
            None,
            url_params,
        )

        self._make_caches()

    def _fetch_minimum_optimizations(self):
        self._assert_online()

        url_params = {}

        r = self.client._auto_request(
            "get",
            f"v1/records/torsiondrive/{self.raw_data.id}/minimum_optimizations",
            None,
            ProjURLParameters,
            Dict[str, OptimizationRecord._DataModel],
            None,
            url_params,
        )

        self.raw_data.minimum_optimizations_cache = {
            k: OptimizationRecord.from_datamodel(v, self.client) for k, v in r.items()
        }

    @property
    def specification(self) -> TorsiondriveSpecification:
        return self.raw_data.specification

    @property
    def initial_molecules(self) -> List[Molecule]:
        if self.raw_data.initial_molecules is None:
            self._fetch_initial_molecules()
        return self.raw_data.initial_molecules

    @property
    def optimizations(self) -> Dict[str, List[OptimizationRecord]]:
        self._make_caches()

        if self.raw_data.optimizations_cache is None:
            self._fetch_optimizations()

        return self.raw_data.optimizations_cache

    @property
    def minimum_optimizations(self) -> Dict[str, OptimizationRecord]:
        self._make_caches()

        if self.raw_data.minimum_optimizations_cache is None:
            self._fetch_minimum_optimizations()

        return self.raw_data.minimum_optimizations_cache
