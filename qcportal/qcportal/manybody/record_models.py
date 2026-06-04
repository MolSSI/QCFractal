from __future__ import annotations

from collections.abc import Iterable
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from qcportal.base_models import RestModelBase
from qcportal.cache import get_records_with_cache
from qcportal.common_types import LowerStr
from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters, compare_base_records
from qcportal.singlepoint.record_models import (
    QCSpecification,
    SinglepointRecord,
    compare_singlepoint_records,
)


class BSSECorrectionEnum(str, Enum):
    nocp = "nocp"
    cp = "cp"
    vmfc = "vmfc"


class ManybodyKeywords(BaseModel):
    model_config = ConfigDict(extra="forbid")

    return_total_data: bool = False


class ManybodySpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    program: LowerStr = "qcmanybody"
    levels: dict[int | Literal["supersystem"], QCSpecification]
    bsse_correction: list[BSSECorrectionEnum]
    keywords: ManybodyKeywords = Field(ManybodyKeywords())
    protocols: dict[str, Any] = Field(default_factory=dict)


class ManybodyInput(RestModelBase):
    record_type: Literal["manybody"] = "manybody"
    specification: ManybodySpecification
    initial_molecule: int | Molecule


class ManybodyMultiInput(RestModelBase):
    specification: ManybodySpecification
    initial_molecules: list[int | Molecule]


class ManybodyAddBody(RecordAddBodyBase, ManybodyMultiInput):
    pass


class ManybodyQueryFilters(RecordQueryFilters):
    program: list[str] | None = None
    qc_program: list[LowerStr] | None = None
    qc_method: list[LowerStr] | None = None
    qc_basis: list[LowerStr | None] | None = None
    initial_molecule_id: list[int] | None = None


class ManybodyClusterMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    molecule_id: int
    mc_level: str
    fragments: list[int]
    basis: list[int]
    singlepoint_id: int | None
    molecule: Molecule | None = None


class ManybodyCluster(ManybodyClusterMeta):
    singlepoint_record: SinglepointRecord | None = None


class ManybodyRecord(BaseRecord):
    record_type: Literal["manybody"] = "manybody"
    specification: ManybodySpecification

    initial_molecule_id: int

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    initial_molecule_: Molecule | None = Field(None, alias="initial_molecule")
    clusters_meta_: list[ManybodyClusterMeta] | None = Field(None, alias="clusters")

    ##############################################
    # Fields with child records
    # (generally not received from the server)
    ##############################################
    cluster_records_: list[ManybodyCluster] | None = Field(None, alias="cluster_records")

    def propagate_client(self, client, base_url_prefix: str | None):
        BaseRecord.propagate_client(self, client, base_url_prefix)

        if self.cluster_records_ is not None:
            for cluster in self.cluster_records_:
                if cluster.singlepoint_record:
                    cluster.singlepoint_record.propagate_client(client, base_url_prefix)

    @classmethod
    def _fetch_children_multi(
        cls, client, record_cache, records: Iterable[ManybodyRecord], include: Iterable[str], force_fetch: bool = False
    ):
        # Should be checked by the calling function
        assert records
        assert all(isinstance(x, ManybodyRecord) for x in records)

        base_url_prefix = next(iter(records))._base_url_prefix
        assert all(r._base_url_prefix == base_url_prefix for r in records)

        if "clusters" in include or "**" in include:
            # collect all singlepoint ids for all manybody records
            sp_ids = set()

            for r in records:
                if r.clusters_meta_:
                    sp_ids.update(x.singlepoint_id for x in r.clusters_meta_ if x.singlepoint_id is not None)

            sp_ids = list(sp_ids)
            sp_recs = get_records_with_cache(
                client,
                base_url_prefix,
                record_cache,
                SinglepointRecord,
                sp_ids,
                include=include,
                force_fetch=force_fetch,
            )
            sp_map = {x.id: x for x in sp_recs}

            for r in records:
                if r.clusters_meta_ is None:
                    r.cluster_records_ = None
                else:
                    r.cluster_records_ = []
                    for cm in r.clusters_meta_:
                        cluster = ManybodyCluster(**cm.model_dump())

                        if cluster.singlepoint_id is not None:
                            cluster.singlepoint_record = sp_map[cluster.singlepoint_id]

                        r.cluster_records_.append(cluster)

                r.propagate_client(client, base_url_prefix)

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_clusters(self):
        if self.clusters_meta_ is None:
            self._assert_online()
            self.clusters_meta_ = self._client.make_request(
                "get",
                f"api/v1/records/manybody/{self.id}/clusters",
                list[ManybodyClusterMeta],
            )

        self.fetch_children(["clusters"])

    def get_cache_dict(self, **kwargs) -> dict[str, Any]:
        return self.model_dump(exclude={"cluster_records_"}, **kwargs)

    @property
    def initial_molecule(self) -> Molecule:
        if self.initial_molecule_ is None:
            self._fetch_initial_molecule()
        return self.initial_molecule_

    @property
    def clusters(self) -> list[ManybodyCluster]:
        if self.cluster_records_ is None:
            self._fetch_clusters()
        return self.cluster_records_


def compare_manybody_records(record_1: ManybodyRecord, record_2: ManybodyRecord):
    compare_base_records(record_1, record_2)

    assert record_1.initial_molecule == record_2.initial_molecule

    assert (record_1.clusters is None) == (record_2.clusters is None)
    if record_1.clusters is not None:
        assert len(record_1.clusters) == len(record_2.clusters)

        # Record ids can be different, so sort by level, fragments, basis
        cluster_1 = sorted(record_1.clusters, key=lambda x: (x.mc_level, x.fragments, x.basis))
        cluster_2 = sorted(record_2.clusters, key=lambda x: (x.mc_level, x.fragments, x.basis))

        for r1, r2 in zip(cluster_1, cluster_2):
            assert r1.mc_level == r2.mc_level
            assert r1.fragments == r2.fragments
            assert r1.basis == r2.basis
            assert r1.molecule.get_hash() == r2.molecule.get_hash()
            compare_singlepoint_records(r1.singlepoint_record, r2.singlepoint_record)
