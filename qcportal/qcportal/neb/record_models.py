from __future__ import annotations

from typing import List, Optional, Union, Dict, Iterable

try:
    from pydantic.v1 import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Field, Extra, root_validator, constr, validator, PrivateAttr
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.cache import get_records_with_cache
from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.utils import recursive_normalizer, is_included
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

    align: bool = Field(True, description="Align the images before starting the NEB calculation.")

    epsilon: float = Field(1e-5, description="Small eigenvalue threshold for resetting Hessian.")

    @root_validator
    def normalize(cls, values):
        return recursive_normalizer(values)


class NEBSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "geometric"
    singlepoint_specification: QCSpecification
    optimization_specification: Optional[OptimizationSpecification] = None
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


class NEBInput(RestModelBase):
    record_type: Literal["neb"] = "neb"
    specification: NEBSpecification
    initial_chain: List[Union[int, Molecule]]


class NEBMultiInput(RestModelBase):
    specification: NEBSpecification
    initial_chains: List[List[Union[int, Molecule]]]


class NEBAddBody(RecordAddBodyBase, NEBMultiInput):
    pass


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
    # Fields not always included when fetching the record
    ######################################################
    initial_chain_molecule_ids_: Optional[List[int]] = Field(None, alias="initial_chain_molecule_ids")
    singlepoints_: Optional[List[NEBSinglepoint]] = Field(None, alias="singlepoints")
    optimizations_: Optional[Dict[str, NEBOptimization]] = Field(None, alias="optimizations")
    neb_result_: Optional[Molecule] = Field(None, alias="neb_result")
    initial_chain_: Optional[List[Molecule]] = Field(None, alias="initial_chain")

    ########################################
    # Caches
    ########################################
    _optimizations_cache: Optional[Dict[str, OptimizationRecord]] = PrivateAttr(None)
    _singlepoints_cache: Optional[Dict[int, List[SinglepointRecord]]] = PrivateAttr(None)
    _ts_hessian: Optional[SinglepointRecord] = PrivateAttr(None)

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self._optimizations_cache is not None:
            for opt in self._optimizations_cache.values():
                opt.propagate_client(client)

        if self._singlepoints_cache is not None:
            for splist in self._singlepoints_cache.values():
                for sp2 in splist:
                    sp2.propagate_client(client)

    @classmethod
    def _fetch_children_multi(
        cls, client, record_cache, records: Iterable[NEBRecord], include: Iterable[str], force_fetch: bool = False
    ):
        # Should be checked by the calling function
        assert records
        assert all(isinstance(x, NEBRecord) for x in records)

        do_sp = is_included("singlepoints", include, None, False)
        do_opt = is_included("optimizations", include, None, False)

        if not do_sp and not do_opt:
            return

        # Collect optimization and singlepoint ids for all NEB
        opt_ids = set()
        sp_ids = set()

        for r in records:
            if r.optimizations_ is not None:
                opt_ids.update(x.optimization_id for x in r.optimizations_.values())
            if r.singlepoints_ is not None:
                sp_ids.update(x.singlepoint_id for x in r.singlepoints_)

        sp_ids = list(sp_ids)
        opt_ids = list(opt_ids)

        if do_sp:
            sp_records = get_records_with_cache(
                client, record_cache, SinglepointRecord, sp_ids, include=include, force_fetch=force_fetch
            )
            sp_map = {r.id: r for r in sp_records}
        if do_opt:
            opt_records = get_records_with_cache(
                client, record_cache, OptimizationRecord, opt_ids, include=include, force_fetch=force_fetch
            )
            opt_map = {r.id: r for r in opt_records}

        for r in records:
            if r.optimizations_ is None:
                r._optimizations_cache = None
            elif do_opt:
                r._optimizations_cache = dict()
                for opt_key, opt_info in r.optimizations_.items():
                    r._optimizations_cache[opt_key] = opt_map[opt_info.optimization_id]

            if r.singlepoints_ is None:
                r._singlepoints_cache = None
            elif do_sp:
                r._singlepoints_cache = dict()
                for sp_info in r.singlepoints_:
                    r._singlepoints_cache.setdefault(sp_info.chain_iteration, list())
                    r._singlepoints_cache[sp_info.chain_iteration].append(sp_map[sp_info.singlepoint_id])

                if len(r._singlepoints_cache) > 0:
                    if len(r._singlepoints_cache[max(r._singlepoints_cache)]) == 1:
                        _, temp_list = r._singlepoints_cache.popitem()
                        r._ts_hessian = temp_list[0]
                        assert r._ts_hessian.specification.driver == "hessian"

            r.propagate_client(r._client)

    def _fetch_optimizations(self):
        if self.optimizations_ is None:
            self._assert_online()
            self.optimizations_ = self._client.make_request(
                "get",
                f"api/v1/records/neb/{self.id}/optimizations",
                Dict[str, NEBOptimization],
            )

        self.fetch_children(["optimizations"])

    def _fetch_singlepoints(self):
        if self.singlepoints_ is None:
            self._assert_online()
            self.singlepoints_ = self._client.make_request(
                "get",
                f"api/v1/records/neb/{self.id}/singlepoints",
                List[NEBSinglepoint],
            )

        self.fetch_children(["singlepoints"])

    def _fetch_initial_chain(self):
        if self.initial_chain_molecule_ids_ is None:
            self._assert_online()
            self.initial_chain_molecule_ids_ = self._client.make_request(
                "get",
                f"api/v1/records/neb/{self.id}/initial_chain",
                List[int],
            )

        self.initial_chain_ = self._client.get_molecules(self.initial_chain_molecule_ids_)

    def _fetch_neb_result(self):
        if self.neb_result_ is None:
            self._assert_online()

            self.neb_result_ = self._client.make_request(
                "get",
                f"api/v1/records/neb/{self.id}/neb_result",
                Optional[Molecule],
            )

    @property
    def initial_chain(self) -> List[Molecule]:
        if self.initial_chain_ is None:
            self._fetch_initial_chain()
        return self.initial_chain_

    @property
    def final_chain(self) -> List[SinglepointRecord]:
        return self.singlepoints[max(self.singlepoints.keys())]

    @property
    def singlepoints(self) -> Dict[int, List[SinglepointRecord]]:
        if self._singlepoints_cache is None:
            self._fetch_singlepoints()
        return self._singlepoints_cache

    @property
    def result(self):
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

    @property
    def ts_hessian(self) -> Optional[SinglepointRecord]:
        if self._singlepoints_cache is None:
            self._fetch_singlepoints()
        return self._ts_hessian
