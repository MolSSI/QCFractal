from typing import List, Union, Optional, Tuple, Iterable

try:
    from pydantic.v1 import BaseModel, Extra, root_validator, constr, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Extra, root_validator, constr, PrivateAttr
from typing_extensions import Literal

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

    molecule: Optional[Molecule]
    singlepoint_record: Optional[SinglepointRecord]
    optimization_record: Optional[OptimizationRecord]


class ReactionRecord(BaseRecord):
    record_type: Literal["reaction"] = "reaction"
    specification: ReactionSpecification

    total_energy: Optional[float]

    ######################################################
    # Fields not included when fetching the record
    ######################################################
    ########################################
    # Caches
    ########################################
    _components: Optional[List[ReactionComponent]] = PrivateAttr(None)

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self._components is not None:
            for comp in self._components:
                if comp.singlepoint_record:
                    comp.singlepoint_record.propagate_client(self._client)
                if comp.optimization_record:
                    comp.optimization_record.propagate_client(self._client)

    def fetch_all(self):
        BaseRecord.fetch_all(self)
        self._fetch_components()

        for comp in self._components:
            if comp.singlepoint_record:
                comp.singlepoint_record.fetch_all()
            if comp.optimization_record:
                comp.optimization_record.fetch_all()

    def _fetch_components(self):
        self._assert_online()

        if not self.offline or self._components is None:
            self._assert_online()
            self._components = self._client.make_request(
                "get",
                f"api/v1/records/reaction/{self.id}/components",
                List[ReactionComponent],
            )

            mol_ids = [c.molecule_id for c in self._components]
            mols = self._client.get_molecules(mol_ids)
            for c, mol in zip(self._components, mols):
                assert mol.id == c.molecule_id
                c.molecule = mol

        # Fetch records from server or cache
        sp_comp = [c for c in self._components if c.singlepoint_id is not None]
        sp_ids = [c.singlepoint_id for c in sp_comp]
        sp_recs = self._get_child_records(sp_ids, SinglepointRecord)
        for c, rec in zip(sp_comp, sp_recs):
            c.singlepoint_record = rec

        opt_comp = [c for c in self._components if c.optimization_id is not None]
        opt_ids = [c.optimization_id for c in opt_comp]
        opt_recs = self._get_child_records(opt_ids, OptimizationRecord)
        for c, rec in zip(opt_comp, opt_recs):
            assert rec.initial_molecule_id == c.molecule_id
            c.optimization_record = rec

        self.propagate_client(self._client)

    def _handle_includes(self, includes: Optional[Iterable[str]]):
        if includes is None:
            return

        BaseRecord._handle_includes(self, includes)

        if "components" in includes:
            self._fetch_components()

    @property
    def components(self) -> List[ReactionComponent]:
        if self._components is None:
            self._fetch_components()
        return self._components
