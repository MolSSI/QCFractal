from enum import Enum
from typing import List, Union, Optional, Dict, Any

try:
    from pydantic.v1 import BaseModel, Extra, validator, constr, PrivateAttr, Field
except ImportError:
    from pydantic import BaseModel, Extra, validator, constr, PrivateAttr, Field
from typing_extensions import Literal

from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.singlepoint.record_models import (
    QCSpecification,
    SinglepointRecord,
)


class BSSECorrectionEnum(str, Enum):
    nocp = "nocp"
    cp = "cp"
    vmfc = "vmfc"


class ManybodyKeywords(BaseModel):
    class Config:
        extra = Extra.forbid

    max_nbody: Optional[int] = None
    bsse_correction: BSSECorrectionEnum

    @validator("max_nbody")
    def check_max_nbody(cls, v):
        if v is not None and v <= 0:
            raise ValueError("max_nbody must be None or > 0")
        return v


class ManybodySpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "manybody"
    levels: Dict[Union[int, Literal["supersystem"]], QCSpecification]
    return_total_data: bool
    bsse_correction: List[BSSECorrectionEnum]


class ManybodyAddBody(RecordAddBodyBase):
    specification: ManybodySpecification
    initial_molecules: List[Union[int, Molecule]]


class ManybodyQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = None
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    initial_molecule_id: Optional[List[int]] = None


class ManybodyClusterMeta(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    mc_level: str
    fragments: List[int]
    basis: List[int]

    singlepoint_id: Optional[int]
    molecule: Optional[Molecule] = None


class ManybodyCluster(ManybodyClusterMeta):
    singlepoint_record: Optional[SinglepointRecord] = None


class ManybodyRecord(BaseRecord):
    record_type: Literal["manybody"] = "manybody"
    specification: ManybodySpecification
    results: Optional[Dict[str, Any]]

    initial_molecule_id: int

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    initial_molecule_: Optional[Molecule] = Field(None, alias="initial_molecule")
    clusters_meta_: Optional[List[ManybodyClusterMeta]] = Field(None, alias="clusters")

    ########################################
    # Caches
    ########################################
    _clusters: Optional[List[ManybodyCluster]] = PrivateAttr(None)

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self._clusters is not None:
            for cluster in self._clusters:
                if cluster.singlepoint_record:
                    cluster.singlepoint_record.propagate_client(client)

    def _fetch_all(self, recursive: bool = False) -> Dict[str, Any]:
        extra_data = BaseRecord._fetch_all(self, recursive=recursive)
        if self.initial_molecule_ is None:
            self.initial_molecule_ = extra_data.get("initial_molecule", None)

        if self.clusters_meta_ is None:
            self.clusters_meta_ = extra_data.get("clusters", None)

        if recursive and self.clusters_meta_:
            self._fetch_clusters()

            # Fetch everything about the optimizations
            if self._clusters:
                for c in self._clusters:
                    if c.singlepoint_record:
                        c.singlepoint_record.fetch_all(True)

        self.propagate_client(self._client)
        return extra_data

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_clusters(self):
        self._assert_online()

        if self.clusters_meta_ is None:
            # Will include molecules
            self.clusters_meta_ = self._client.make_request(
                "get",
                f"api/v1/records/manybody/{self.id}/clusters",
                List[ManybodyClusterMeta],
            )

        self._clusters = [ManybodyCluster(**x.dict()) for x in self.clusters_meta_]

        # Fetch singlepoint records and molecules
        sp_ids = [x.singlepoint_id for x in self._clusters if x.singlepoint_id is not None]
        sp_recs = self._get_child_records(sp_ids, SinglepointRecord)
        sp_rec_map = {x.id: x for x in sp_recs}

        for cluster in self._clusters:
            if cluster.singlepoint_id is not None:
                sp = sp_rec_map[cluster.singlepoint_id]
                assert sp.id == cluster.singlepoint_id
                assert sp.molecule_id == cluster.molecule_id
                cluster.singlepoint_record = sp

        self.propagate_client(self._client)

    @property
    def initial_molecule(self) -> Molecule:
        if self.initial_molecule_ is None:
            self._fetch_initial_molecule()
        return self.initial_molecule_

    @property
    def clusters(self) -> List[ManybodyCluster]:
        if self.clusters_meta_ is None or self._clusters is None:
            self._fetch_clusters()
        return self._clusters
