from enum import Enum
from typing import List, Union, Optional, Dict, Any, Iterable

try:
    from pydantic.v1 import BaseModel, Extra, validator, constr, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Extra, validator, constr, PrivateAttr
from typing_extensions import Literal

from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.singlepoint.record_models import (
    QCSpecification,
    SinglepointRecord,
)


class BSSECorrectionEnum(str, Enum):
    none = "none"
    cp = "cp"


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


class ManybodyCluster(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    fragments: List[int]
    basis: List[int]
    degeneracy: int
    singlepoint_id: Optional[int]

    molecule: Optional[Molecule] = None
    singlepoint_record: Optional[SinglepointRecord] = None


class ManybodyRecord(BaseRecord):
    record_type: Literal["manybody"] = "manybody"
    specification: ManybodySpecification
    results: Optional[Dict[str, Any]]

    initial_molecule_id: int

    ######################################################
    # Fields not included when fetching the record
    ######################################################
    initial_molecule_: Optional[Molecule] = None

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

    def fetch_all(self):
        BaseRecord.fetch_all(self)

        self._fetch_initial_molecule()
        self._fetch_clusters()

        for cluster in self._clusters:
            if cluster.singlepoint_record:
                cluster.singlepoint_record.fetch_all()

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_clusters(self):
        self._assert_online()

        if not self.offline or self._clusters is None:
            self._clusters = self._client.make_request(
                "get",
                f"api/v1/records/manybody/{self.id}/clusters",
                List[ManybodyCluster],
            )

            mol_ids = [x.molecule_id for x in self._clusters]
            mols = self._client.get_molecules(mol_ids)

            for cluster, mol in zip(self._clusters, mols):
                assert mol.id == cluster.molecule_id
                cluster.molecule = mol

        # Fetch singlepoint records and molecules
        sp_ids = [x.singlepoint_id for x in self._clusters]
        sp_recs = self._get_child_records(sp_ids, SinglepointRecord)

        for (
            cluster,
            sp,
        ) in zip(self._clusters, sp_recs):
            assert sp.id == cluster.singlepoint_id
            assert sp.molecule_id == cluster.molecule_id
            cluster.singlepoint_record = sp

        self.propagate_client(self._client)

    def _handle_includes(self, includes: Optional[Iterable[str]]):
        if includes is None:
            return

        BaseRecord._handle_includes(self, includes)

        if "initial_molecule" in includes:
            self._fetch_initial_molecule()
        if "clusters" in includes:
            self._fetch_clusters()

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
