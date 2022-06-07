from enum import Enum
from typing import List, Union, Optional, Dict, Any, Set, Iterable

from pydantic import BaseModel, Extra, validator, constr
from typing_extensions import Literal

from ..models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from ..singlepoint.models import (
    QCSpecification,
    SinglepointRecord,
)
from ...base_models import ProjURLParameters
from ...molecules import Molecule


class BSSECorrectionEnum(str, Enum):
    none = "none"
    cp = "cp"


class ManybodyKeywords(BaseModel):
    class Config:
        extra = Extra.forbid

    max_nbody: Optional[int]
    bsse_correction: BSSECorrectionEnum

    @validator("max_nbody")
    def check_max_nbody(cls, v):
        if v is not None and v <= 0:
            raise ValueError("max_nbody must be None or > 0")
        return v


class ManybodySpecification(BaseModel):
    program: constr(to_lower=True) = "manybody"
    singlepoint_specification: QCSpecification
    keywords: ManybodyKeywords


class ManybodyAddBody(RecordAddBodyBase):
    specification: ManybodySpecification
    initial_molecules: List[Union[int, Molecule]]


class ManybodyQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = None
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    initial_molecule_id: Optional[List[int]] = None


# Used internally - stores datamodel for SinglepointRecord
class ManybodyCluster_(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    fragments: List[int]
    basis: List[int]
    degeneracy: int
    singlepoint_id: Optional[int]

    molecule: Optional[Molecule] = None
    singlepoint_record: Optional[SinglepointRecord._DataModel]


# User facing - stores SinglepointRecord itself
class ManybodyCluster(ManybodyCluster_):
    singlepoint_record: Optional[SinglepointRecord]


class ManybodyRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["manybody"] = "manybody"
        specification: ManybodySpecification
        results: Optional[Dict[str, Any]]

        initial_molecule_id: int
        initial_molecule: Optional[Molecule] = None

        clusters: Optional[List[ManybodyCluster_]] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["manybody"] = "manybody"
    raw_data: _DataModel

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:

        if includes is None:
            return None

        ret = BaseRecord.transform_includes(includes)

        if "initial_molecule" in includes:
            ret.add("initial_molecule")
        if "clusters" in includes:
            ret.add("clusters")

        return ret

    def _fetch_initial_molecule(self):
        self.raw_data.initial_molecule = self.client.get_molecules([self.raw_data.initial_molecule_id])[0]

    def _fetch_clusters(self):
        url_params = {"include": ["*", "molecule", "singlepoint_record"]}

        self.raw_data.clusters = self.client._auto_request(
            "get",
            f"v1/records/manybody/{self.raw_data.id}/clusters",
            None,
            ProjURLParameters,
            List[ManybodyCluster_],
            None,
            url_params,
        )

    @property
    def initial_molecule(self) -> Molecule:
        if self.raw_data.initial_molecule is None:
            self._fetch_initial_molecule()
        return self.raw_data.initial_molecule

    @property
    def initial_molecule_id(self) -> int:
        return self.raw_data.initial_molecule_id

    @property
    def specification(self) -> ManybodySpecification:
        return self.raw_data.specification

    @property
    def results(self):
        return self.raw_data.results

    @property
    def clusters(self) -> List[ManybodyCluster]:
        if self.raw_data.clusters is None:
            self._fetch_clusters()

        if self.raw_data.clusters is None:
            return []

        ret = []
        for x in self.raw_data.clusters:
            sp = None
            if x.singlepoint_record is not None:
                sp = SinglepointRecord.from_datamodel(x.singlepoint_record, self.client)

            ret.append(ManybodyCluster(**x.dict(exclude={"singlepoint_record"}), singlepoint_record=sp))

        return ret
