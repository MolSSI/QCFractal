from typing import List, Union, Optional, Tuple, Set, Iterable

from pydantic import BaseModel, Extra, root_validator, constr, Field
from typing_extensions import Literal

from qcportal.base_models import ProjURLParameters
from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from ..optimization.record_models import OptimizationRecord, OptimizationSpecification
from ..singlepoint.record_models import (
    QCSpecification,
    SinglepointRecord,
)


class ReactionKeywords(BaseModel):
    # NOTE: If we add keywords, update the dataset additional_keywords tests and add extra = Extra.forbid.
    # The current setup is needed for those tests (to allow for testing additional_keywords)
    # is needed
    class Config:
        pass
        # extra = Extra.forbid

    pass


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


class ReactionComponent(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    coefficient: float
    singlepoint_id: Optional[int]
    optimization_id: Optional[int]

    molecule: Molecule
    singlepoint_record: Optional[SinglepointRecord]
    optimization_record: Optional[OptimizationRecord]


class ReactionRecord(BaseRecord):

    record_type: Literal["reaction"] = "reaction"
    specification: ReactionSpecification

    total_energy: Optional[float]

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    components_: Optional[List[ReactionComponent]] = Field(None, alias="components")

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

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self.components_ is not None:
            for comp in self.components_:
                if comp.singlepoint_record:
                    comp.singlepoint_record.propagate_client(self._client)
                if comp.optimization_record:
                    comp.optimization_record.propagate_client(self._client)

    def _fetch_components(self):
        self._assert_online()
        url_params = {"include": ["*", "singlepoint_record", "optimization_record", "molecule"]}

        self.components_ = self._client._auto_request(
            "get",
            f"v1/records/reaction/{self.id}/components",
            None,
            ProjURLParameters,
            List[ReactionComponent],
            None,
            url_params,
        )

        self.propagate_client(self._client)

    @property
    def components(self) -> List[ReactionComponent]:
        if self.components_ is None:
            self._fetch_components()
        return self.components_
