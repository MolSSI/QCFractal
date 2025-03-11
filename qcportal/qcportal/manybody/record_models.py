from __future__ import annotations

from enum import Enum
from typing import List, Union, Optional, Dict, Any, Iterable

try:
    from pydantic.v1 import BaseModel, Extra, validator, constr, PrivateAttr, Field
except ImportError:
    from pydantic import BaseModel, Extra, validator, constr, PrivateAttr, Field
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.cache import get_records_with_cache
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

    return_total_data: bool = False


class ManybodySpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "qcmanybody"
    levels: Dict[Union[int, Literal["supersystem"]], QCSpecification]
    bsse_correction: List[BSSECorrectionEnum]
    keywords: ManybodyKeywords = Field(ManybodyKeywords())
    protocols: Dict[str, Any] = Field(default_factory=dict)


class ManybodyInput(RestModelBase):
    record_type: Literal["manybody"] = "manybody"
    specification: ManybodySpecification
    initial_molecule: Union[int, Molecule]


class ManybodyMultiInput(RestModelBase):
    specification: ManybodySpecification
    initial_molecules: List[Union[int, Molecule]]


class ManybodyAddBody(RecordAddBodyBase, ManybodyMultiInput):
    pass


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

    @classmethod
    def _fetch_children_multi(
        cls, client, record_cache, records: Iterable[ManybodyRecord], include: Iterable[str], force_fetch: bool = False
    ):
        # Should be checked by the calling function
        assert records
        assert all(isinstance(x, ManybodyRecord) for x in records)

        if "clusters" in include or "**" in include:
            # collect all singlepoint ids for all manybody records
            sp_ids = set()

            for r in records:
                if r.clusters_meta_:
                    sp_ids.update(x.singlepoint_id for x in r.clusters_meta_ if x.singlepoint_id is not None)

            sp_ids = list(sp_ids)
            sp_recs = get_records_with_cache(
                client, record_cache, SinglepointRecord, sp_ids, include=include, force_fetch=force_fetch
            )
            sp_map = {x.id: x for x in sp_recs}

            for r in records:
                if r.clusters_meta_ is None:
                    r._clusters = None
                else:
                    r._clusters = []
                    for cm in r.clusters_meta_:
                        cluster = ManybodyCluster(**cm.dict())

                        if cluster.singlepoint_id is not None:
                            cluster.singlepoint_record = sp_map[cluster.singlepoint_id]

                        r._clusters.append(cluster)

                r.propagate_client(client)

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_clusters(self):
        if self.clusters_meta_ is None:
            self._assert_online()
            self.clusters_meta_ = self._client.make_request(
                "get",
                f"api/v1/records/manybody/{self.id}/clusters",
                List[ManybodyClusterMeta],
            )

        self.fetch_children(["clusters"])

    @property
    def initial_molecule(self) -> Molecule:
        if self.initial_molecule_ is None:
            self._fetch_initial_molecule()
        return self.initial_molecule_

    @property
    def clusters(self) -> List[ManybodyCluster]:
        if self._clusters is None:
            self._fetch_clusters()
        return self._clusters
