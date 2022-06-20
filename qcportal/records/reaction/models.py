from typing import List, Union, Optional, Tuple, Set, Iterable

from pydantic import BaseModel, Extra, root_validator, constr
from typing_extensions import Literal

from ..models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from ..optimization.models import OptimizationRecord, OptimizationSpecification
from ..singlepoint.models import (
    QCSpecification,
    SinglepointRecord,
)
from ...base_models import ProjURLParameters
from ...molecules import Molecule


class ReactionKeywords(BaseModel):
    class Config:
        extra = Extra.forbid

    max_running: Optional[int] = None


class ReactionSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "reaction"

    singlepoint_specification: Optional[QCSpecification]
    optimization_specification: Optional[OptimizationSpecification]

    keywords: ReactionKeywords

    @root_validator
    def required_spec(cls, v):
        qc_spec = v.get("singlepoint_specification", None)
        opt_spec = v.get("optimization_specification", None)
        if qc_spec is None and opt_spec is None:
            raise ValueError("singlepoint_specification or optimization_specification must be specified")
        return v


class ReactionAddBody(RecordAddBodyBase):
    specification: ReactionSpecification
    stoichiometries: List[List[Tuple[float, Union[int, Molecule]]]]


class ReactionQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = None
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    optimization_program: Optional[List[constr(to_lower=True)]] = None
    molecule_id: Optional[List[int]] = None


class ReactionComponent_(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    coefficient: float
    singlepoint_id: Optional[int]
    optimization_id: Optional[int]

    molecule: Optional[Molecule]
    singlepoint_record: Optional[SinglepointRecord._DataModel]
    optimization_record: Optional[OptimizationRecord._DataModel]


class ReactionComponent(ReactionComponent_):
    singlepoint_record: Optional[SinglepointRecord]
    optimization_record: Optional[OptimizationRecord]


class ReactionRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["reaction"] = "reaction"
        specification: ReactionSpecification

        total_energy: Optional[float]

        components: Optional[List[ReactionComponent_]] = None
        components_cache: Optional[List[ReactionComponent]] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["reaction"] = "reaction"
    raw_data: _DataModel

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:

        if includes is None:
            return None

        ret = BaseRecord.transform_includes(includes)

        if "components" in includes:
            ret |= {
                "components.*",
                "components.molecule",
                "components.singlepoint_record",
                "components.optimization_record",
            }

        return ret

    def _make_caches(self):
        if self.raw_data.components is None:
            return

        if self.raw_data.components_cache is None:
            self.raw_data.components_cache = []

            for com in self.raw_data.components:
                update = {}
                if com.singlepoint_record is not None:
                    sp = SinglepointRecord.from_datamodel(com.singlepoint_record, self.client)
                    update["singlepoint_record"] = sp
                if com.singlepoint_record is not None:
                    opt = OptimizationRecord.from_datamodel(com.optimization_record, self.client)
                    update["optimization_record"] = opt

                com2 = ReactionComponent(**com.dict(exclude={"singlepoint_record", "optimization_record"}), **update)
                self.raw_data.components_cache.append(com2)

    def _fetch_components(self):
        self._assert_online()
        url_params = {"include": ["*", "singlepoint_record", "optimization_record"]}

        self.raw_data.components = self.client._auto_request(
            "get",
            f"v1/records/reaction/{self.raw_data.id}/components",
            None,
            ProjURLParameters,
            List[ReactionComponent_],
            None,
            url_params,
        )

        self._make_caches()

    @property
    def specification(self) -> ReactionSpecification:
        return self.raw_data.specification

    @property
    def components(self) -> List[ReactionComponent]:
        self._make_caches()

        if self.raw_data.components_cache is None:
            self._fetch_components()

        return self.raw_data.components_cache

    @property
    def total_energy(self) -> Optional[float]:
        return self.raw_data.total_energy
