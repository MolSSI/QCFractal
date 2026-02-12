from __future__ import annotations

from typing import List, Union, Optional, Tuple, Iterable, Dict, Any

try:
    from pydantic.v1 import BaseModel, Extra, root_validator, constr, Field
except ImportError:
    from pydantic import BaseModel, Extra, root_validator, constr, Field
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.utils import is_included
from qcportal.cache import get_records_with_cache
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters, compare_base_records
from ..optimization.record_models import OptimizationRecord, OptimizationSpecification, compare_optimization_records
from ..singlepoint.record_models import QCSpecification, SinglepointRecord, compare_singlepoint_records


class ReactionKeywords(BaseModel):
    # NOTE: If we add keywords, update the dataset additional_keywords tests and add extra = Extra.forbid.
    # The current setup is needed for those tests (to allow for testing additional_keywords)
    # is needed
    class Config:
        pass
        # extra = Extra.forbid


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


class ReactionInput(RestModelBase):
    record_type: Literal["reaction"] = "reaction"
    specification: ReactionSpecification
    stoichiometries: List[Tuple[float, Union[int, Molecule]]]


class ReactionMultiInput(RestModelBase):
    specification: ReactionSpecification
    stoichiometries: List[List[Tuple[float, Union[int, Molecule]]]]


class ReactionAddBody(RecordAddBodyBase, ReactionMultiInput):
    pass


class ReactionQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = None
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    optimization_program: Optional[List[constr(to_lower=True)]] = None
    molecule_id: Optional[List[int]] = None


class ReactionComponentMeta(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    coefficient: float
    singlepoint_id: Optional[int]
    optimization_id: Optional[int]

    molecule: Optional[Molecule]


class ReactionComponent(ReactionComponentMeta):
    singlepoint_record: Optional[SinglepointRecord] = None
    optimization_record: Optional[OptimizationRecord] = None


class ReactionRecord(BaseRecord):
    record_type: Literal["reaction"] = "reaction"
    specification: ReactionSpecification

    total_energy: Optional[float]

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    components_meta_: Optional[List[ReactionComponentMeta]] = Field(None, alias="components")

    ##############################################
    # Fields with child records
    # (generally not received from the server)
    ##############################################
    component_records_: Optional[List[ReactionComponent]] = Field(None, alias="component_records")

    def propagate_client(self, client, base_url_prefix: Optional[str]):
        BaseRecord.propagate_client(self, client, base_url_prefix)

        if self.component_records_ is not None:
            for comp in self.component_records_:
                if comp.singlepoint_record:
                    comp.singlepoint_record.propagate_client(self._client, base_url_prefix)
                if comp.optimization_record:
                    comp.optimization_record.propagate_client(self._client, base_url_prefix)

    @classmethod
    def _fetch_children_multi(
        cls, client, record_cache, records: Iterable[ReactionRecord], include: Iterable[str], force_fetch: bool = False
    ):
        # Should be checked by the calling function
        assert records
        assert all(isinstance(x, ReactionRecord) for x in records)

        base_url_prefix = next(iter(records))._base_url_prefix
        assert all(r._base_url_prefix == base_url_prefix for r in records)

        if is_included("components", include, None, False):
            # collect all singlepoint * optimization ids for all optimization
            sp_ids = set()
            opt_ids = set()

            for r in records:
                if r.components_meta_:
                    for cm in r.components_meta_:
                        if cm.singlepoint_id is not None:
                            sp_ids.add(cm.singlepoint_id)
                        if cm.optimization_id is not None:
                            opt_ids.add(cm.optimization_id)

            sp_ids = list(sp_ids)
            opt_ids = list(opt_ids)

            sp_records = get_records_with_cache(
                client,
                base_url_prefix,
                record_cache,
                SinglepointRecord,
                sp_ids,
                include=include,
                force_fetch=force_fetch,
            )
            opt_records = get_records_with_cache(
                client,
                base_url_prefix,
                record_cache,
                OptimizationRecord,
                opt_ids,
                include=include,
                force_fetch=force_fetch,
            )

            sp_map = {r.id: r for r in sp_records}
            opt_map = {r.id: r for r in opt_records}

            for r in records:
                if r.components_meta_ is None:
                    r.component_records_ = None
                else:
                    r.component_records_ = []
                    for cm in r.components_meta_:
                        rc = ReactionComponent(**cm.dict())

                        if rc.singlepoint_id is not None:
                            rc.singlepoint_record = sp_map[rc.singlepoint_id]
                        if rc.optimization_id is not None:
                            rc.optimization_record = opt_map[rc.optimization_id]

                        r.component_records_.append(rc)

                r.propagate_client(r._client, base_url_prefix)

    def _fetch_components(self):
        if self.components_meta_ is None:
            self._assert_online()

            # Will include molecules
            self.components_meta_ = self._client.make_request(
                "get",
                f"api/v1/records/reaction/{self.id}/components",
                List[ReactionComponentMeta],
            )

        self.fetch_children(["components"])

    def get_cache_dict(self, **kwargs) -> Dict[str, Any]:
        return self.dict(exclude={"component_records_"}, **kwargs)

    @property
    def components(self) -> List[ReactionComponent]:
        if self.component_records_ is None:
            self._fetch_components()
        return self.component_records_


def compare_reaction_records(record_1: ReactionRecord, record_2: ReactionRecord):
    compare_base_records(record_1, record_2)

    assert record_1.total_energy == record_2.total_energy

    assert (record_1.components is None) == (record_2.components is None)

    if record_1.components is not None:
        assert len(record_1.components) == len(record_2.components)

        # Sort by molecule hash
        components_1 = sorted(record_1.component_records_, key=lambda x: x.molecule.get_hash())
        components_2 = sorted(record_2.component_records_, key=lambda x: x.molecule.get_hash())

        for c1, c2 in zip(components_1, components_2):
            assert c1.molecule == c2.molecule
            assert c1.coefficient == c2.coefficient
            assert (c1.singlepoint_record is None) == (c2.singlepoint_record is None)
            assert (c1.optimization_record is None) == (c2.optimization_record is None)

            if c1.singlepoint_record is not None:
                compare_singlepoint_records(c1.singlepoint_record, c2.singlepoint_record)
            if c1.optimization_record is not None:
                compare_optimization_records(c1.optimization_record, c2.optimization_record)
