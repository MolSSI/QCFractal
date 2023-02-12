from typing import List, Optional, Union, Dict, Set, Iterable

from pydantic import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
from typing_extensions import Literal

from qcportal.base_models import ProjURLParameters
from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.utils import recursive_normalizer
from ..optimization.record_models import OptimizationRecord
from ..singlepoint.record_models import QCSpecification, SinglepointRecord


class NEBKeywords(BaseModel):
    """
    NEBRecord options
    """

    class Config:
        extra = Extra.forbid

    images: int = Field(
        11,
        description="Number of images that will be used to locate a rough transition state structure.",
        gt=5,
    )

    spring_constant: float = Field(
        1.0,
        description="Spring constant in kcal/mol/Ang^2.",
    )

    spring_type: int = Field(
        0,
        description="0: Nudged Elastic Band (parallel spring force + perpendicular gradients)\n"
        "1: Hybrid Elastic Band (full spring force + perpendicular gradients)\n"
        "2: Plain Elastic Band (full spring force + full gradients)\n",
    )

    maximum_force: float = Field(
        0.05,
        description="Convergence criteria. Converge when maximum RMS-gradient (ev/Ang) of the chain fall below maximum_force.",
    )

    average_force: float = Field(
        0.025,
        description="Convergence criteria. Converge when average RMS-gradient (ev/Ang) of the chain fall below average_force.",
    )

    maximum_cycle: int = Field(100, description="Maximum iteration number for NEB calculation.")

    energy_weighted: Optional[int] = Field(
        None,
        description="Provide an integer value to vary the spring constant based on images' energy (range: spring_constant/energy_weighted - spring_constant).",
    )

    optimize_ts: bool = Field(
        False,
        description="Setting it equal to true will perform a transition sate optimization starting with the guessed transition state structure from the NEB calculation result.",
    )

    align_chain: bool = Field(False, description="Aligning the initial chain before optimization.")

    optimize_endpoints: bool = Field(
        False,
        description="Setting it equal to True will optimize two end points of the initial chain before starting NEB.",
    )

    coordinate_system: str = Field(
        "tric",
        description="Coordinate system for optimizations:\n"
        '"tric" for Translation-Rotation Internal Coordinates (default)\n'
        '"cart" = Cartesian coordinate system\n'
        '"prim" = Primitive (a.k.a redundant internal coordinates)\n '
        '"dlc" = Delocalized Internal Coordinates,\n'
        '"hdlc" = Hybrid Delocalized Internal Coordinates\n'
        '"tric-p" for primitive Translation-Rotation Internal Coordinates (no delocalization)\n ',
    )

    epsilon: float = Field(1e-5, description="Small eigenvalue threshold for resetting Hessian.")

    hessian_reset: bool = Field(
        True,
        description="Reset Hessian when eigenvalues are below the epsilon. If it is set to False, it will skip updating the hessian.",
    )

    @root_validator
    def normalize(cls, values):
        return recursive_normalizer(values)


class NEBSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "geometric"
    singlepoint_specification: QCSpecification
    keywords: NEBKeywords


class NEBOptimization(BaseModel):
    class config:
        extra = Extra.forbid

    optimization_id: int
    position: int
    ts: bool
    optimization_record: Optional[OptimizationRecord]


class NEBSinglepoint(BaseModel):
    class Config:
        extra = Extra.forbid

    singlepoint_id: int
    chain_iteration: int
    position: int
    singlepoint_record: Optional[SinglepointRecord]


# class NEBInitialchain(BaseModel):
#    class Config:
#        extra = Extra.forbid
#
#    id: int
#    molecule_id: int
#    position: int
#
#    molecule: Optional[Molecule]


class NEBAddBody(RecordAddBodyBase):
    specification: NEBSpecification
    initial_chains: List[List[Union[int, Molecule]]]


class NEBQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = "geometric"
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    initial_chain_id: Optional[List[int]] = None

    @validator("qc_basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


class NEBRecord(BaseRecord):

    record_type: Literal["neb"] = "neb"
    specification: NEBSpecification

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    initial_chain_: Optional[List[Molecule]] = Field(None, alias="initial_chain")
    singlepoints_: Optional[List[NEBSinglepoint]] = Field(None, alias="singlepoints")
    optimizations_: Optional[List[NEBOptimization]] = Field(None, alias="optimizations")

    ########################################
    # Private caches
    _optimizations_cache: Optional[Dict[str, OptimizationRecord]] = PrivateAttr(None)
    _singlepoints_cache: Optional[Dict[int, List[SinglepointRecord]]] = PrivateAttr(None)

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:
        if includes is None:
            return None

        ret = BaseRecord.transform_includes(includes)

        if "initial_chain" in includes:
            ret.add("initial_chain")
        if "singlepoints" in includes:
            ret |= {"singlepoints.*", "singlepoints.singlepoint_record"}
        if "optimizations" in includes:
            ret |= {"optimizations.*", "optimizations.optimization_record"}

        return ret

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        # Don't need to do _optimizations_cache. Those should point back to the records in optimizations_
        if self.optimizations_ is not None:
            for sp in self.optimizations_:
                if sp.optimization_record:
                    sp.optimization_record.propagate_client(client)

        # Don't need to do _singlepoints_cache. Those should point back to the records in singlepoints_
        if self.singlepoints_ is not None:
            for sp in self.singlepoints_:
                if sp.singlepoint_record:
                    sp.singlepoint_record.propagate_client(client)

    def make_caches(self):
        BaseRecord.make_caches(self)

        if self.optimizations_ is None:
            self._optimizations_cache = None
        if self.singlepoints_ is None:
            self._singlepoints_cache = None

        if self.optimizations_ is not None:
            self._optimizations_cache = {}

            # convert the raw optimization data to a dictionary of key -> Dict[str, OptimizationRecord]
            for opt in self.optimizations_:
                opt_rec = opt.optimization_record
                if opt.ts:
                    self._optimizations_cache["transition"] = opt_rec
                elif opt.position == 0:
                    self._optimizations_cache["initial"] = opt_rec
                else:
                    self._optimizations_cache["final"] = opt_rec

        if self.singlepoints_ is not None:
            self._singlepoints_cache = {}

            # Singlepoints should be in order of (iteration, position)
            for sp in self.singlepoints_:
                self._singlepoints_cache.setdefault(sp.chain_iteration, list())
                self._singlepoints_cache[sp.chain_iteration].append(sp.singlepoint_record)

    def _fetch_optimizations(self):
        self._assert_online()

        url_params = {"include": ["*", "optimization_record"]}

        self.optimizations_ = self._client._auto_request(
            "get",
            f"v1/records/neb/{self.id}/optimizations",
            None,
            ProjURLParameters,
            List[NEBOptimization],
            None,
            url_params,
        )

        self.make_caches()
        self.propagate_client(self._client)

    def _fetch_initial_chain(self):
        self._assert_online()

        self.initial_chain_ = self._client._auto_request(
            "get",
            f"v1/records/neb/{self.id}/initial_chain",
            None,
            None,
            List[Molecule],
            None,
            None,
        )

    def _fetch_singlepoints(self):
        self._assert_online()

        url_params = {"include": ["*", "singlepoint_record"]}

        self.singlepoints_ = self._client._auto_request(
            "get",
            f"v1/records/neb/{self.id}/singlepoints",
            None,
            ProjURLParameters,
            List[NEBSinglepoint],
            None,
            url_params,
        )

        self.make_caches()
        self.propagate_client(self._client)

    @property
    def initial_chain(self) -> List[Molecule]:
        if self.initial_chain_ is None:
            self._fetch_initial_chain()
        return self.initial_chain_

    @property
    def singlepoints(self) -> Dict[int, List[SinglepointRecord]]:
        if self._singlepoints_cache is None:
            self._fetch_singlepoints()
        return self._singlepoints_cache

    @property
    def neb_result(self):
        url_params = {}
        r = self._client._auto_request(
            "get",
            f"v1/records/neb/{self.id}/neb_result",
            None,
            ProjURLParameters,
            Molecule,
            None,
            url_params,
        )

        return r

    @property
    def optimizations(self) -> Optional[Dict[str, OptimizationRecord]]:
        if self._optimizations_cache is None:
            self._fetch_optimizations()
        return self._optimizations_cache

    @property
    def ts_optimization(self) -> Optional[OptimizationRecord]:
        if self._optimizations_cache is None:
            self._fetch_optimizations()
        return self._optimizations_cache.get("transition", None)
