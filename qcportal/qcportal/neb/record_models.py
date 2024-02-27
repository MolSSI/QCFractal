from typing import List, Optional, Union, Dict, Iterable

try:
    from pydantic.v1 import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
from typing_extensions import Literal

from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.utils import recursive_normalizer
from ..optimization.record_models import OptimizationRecord, OptimizationSpecification
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

    optimize_ts: bool = Field(
        False,
        description="Setting it equal to true will perform a transition sate optimization starting with the guessed transition state structure from the NEB calculation result.",
    )

    optimize_endpoints: bool = Field(
        False,
        description="Setting it equal to True will optimize two end points of the initial chain before starting NEB.",
    )

    epsilon: float = Field(1e-5, description="Small eigenvalue threshold for resetting Hessian.")

    hessian_reset: bool = Field(
        True,
        description="Reset Hessian when eigenvalues are below the epsilon.",
    )

    @root_validator
    def normalize(cls, values):
        return recursive_normalizer(values)


class NEBSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "geometric"
    singlepoint_specification: QCSpecification
    optimization_specification: Optional[OptimizationSpecification]
    keywords: NEBKeywords


class NEBOptimization(BaseModel):
    class config:
        extra = Extra.forbid

    optimization_id: int
    position: int
    ts: bool


class NEBSinglepoint(BaseModel):
    class Config:
        extra = Extra.forbid

    singlepoint_id: int
    chain_iteration: int
    position: int


class NEBAddBody(RecordAddBodyBase):
    specification: NEBSpecification
    initial_chains: List[List[Union[int, Molecule]]]


class NEBQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = "geometric"
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    molecule_id: Optional[List[int]] = None

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
    # Fields not included when fetching the record
    ######################################################
    initial_chain_molecule_ids_: Optional[List[int]] = None
    singlepoints_: Optional[List[NEBSinglepoint]] = None
    optimizations_: Optional[Dict[str, NEBOptimization]] = None
    neb_result_: Optional[Molecule] = None
    initial_chain_: Optional[List[Molecule]] = None

    ########################################
    # Caches
    ########################################
    _optimizations_cache: Optional[Dict[str, OptimizationRecord]] = PrivateAttr(None)
    _singlepoints_cache: Optional[Dict[int, List[SinglepointRecord]]] = PrivateAttr(None)

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self._optimizations_cache is not None:
            for opt in self._optimizations_cache.values():
                opt.propagate_client(client)

        if self._singlepoints_cache is not None:
            for splist in self._singlepoints_cache.values():
                for sp2 in splist:
                    sp2.propagate_client(client)

    def fetch_all(self):
        BaseRecord.fetch_all(self)

        self._fetch_initial_chain()
        self._fetch_singlepoints()
        self._fetch_optimizations()
        self._fetch_neb_result()

        for opt in self._optimizations_cache.values():
            opt.fetch_all()

        for splist in self._singlepoints_cache.values():
            for sp2 in splist:
                sp2.fetch_all()

    def _fetch_optimizations(self):
        self._assert_online()

        if not self.offline or self.optimizations_ is None:
            self._assert_online()
            self.optimizations_ = self._client.make_request(
                "get",
                f"api/v1/records/neb/{self.id}/optimizations",
                Dict[str, NEBOptimization],
            )

        # Fetch optimization records from server
        opt_ids = [opt.optimization_id for opt in self.optimizations_.values()]
        opt_recs = self._get_child_records(opt_ids, OptimizationRecord)
        opt_map = {opt.id: opt for opt in opt_recs}

        self._optimizations_cache = {}

        for opt_key, opt_info in self.optimizations_.items():
            self._optimizations_cache[opt_key] = opt_map[opt_info.optimization_id]

        self.propagate_client(self._client)

    def _fetch_singlepoints(self):
        self._assert_online()

        if not self.offline or self.singlepoints_ is None:
            self._assert_online()
            self.singlepoints_ = self._client.make_request(
                "get",
                f"api/v1/records/neb/{self.id}/singlepoints",
                List[NEBSinglepoint],
            )

        # Fetch singlepoint records from server or the cache
        sp_ids = [sp.singlepoint_id for sp in self.singlepoints_]
        sp_recs = self._get_child_records(sp_ids, SinglepointRecord)

        self._singlepoints_cache = {}

        # Singlepoints should be in order of (iteration, position)
        for sp_info, sp_rec in zip(self.singlepoints_, sp_recs):
            self._singlepoints_cache.setdefault(sp_info.chain_iteration, list())
            self._singlepoints_cache[sp_info.chain_iteration].append(sp_rec)

        self.propagate_client(self._client)

    def _fetch_initial_chain(self):
        self._assert_online()

        self.initial_chain_molecule_ids_ = self._client.make_request(
            "get",
            f"api/v1/records/neb/{self.id}/initial_chain",
            List[int],
        )

        self.initial_chain_ = self._client.get_molecules(self.initial_chain_molecule_ids_)

    def _fetch_neb_result(self):
        self._assert_online()

        self.neb_result_ = self._client.make_request(
            "get",
            f"api/v1/records/neb/{self.id}/neb_result",
            Optional[Molecule],
        )

    def _handle_includes(self, includes: Optional[Iterable[str]]):
        if includes is None:
            return

        BaseRecord._handle_includes(self, includes)

        if "initial_chain" in includes:
            self._fetch_initial_chain()
        if "singlepoints" in includes:
            self._fetch_singlepoints()
        if "optimizations" in includes:
            self._fetch_optimizations()
        if "result" in includes:
            self._fetch_neb_result()

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
        if self.neb_result_ is None and "neb_result_" not in self.__fields_set__:
            self._fetch_neb_result()
        return self.neb_result_

    @property
    def optimizations(self) -> Optional[Dict[str, OptimizationRecord]]:
        if self._optimizations_cache is None:
            self._fetch_optimizations()
        return self._optimizations_cache

    @property
    def ts_optimization(self) -> Optional[OptimizationRecord]:
        return self.optimizations.get("transition", None)
