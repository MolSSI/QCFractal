from __future__ import annotations

from copy import deepcopy
from typing import Optional, Union, Any, List, Dict

try:
    from pydantic.v1 import BaseModel, Field, constr, validator, Extra
except ImportError:
    from pydantic import BaseModel, Field, constr, validator, Extra
from qcelemental.models import Molecule
from qcelemental.models.procedures import (
    OptimizationResult,
    OptimizationProtocols,
    QCInputSpecification,
    Model as AtomicResultModel,
)
from typing_extensions import Literal
from typing import Iterable

from qcportal.base_models import RestModelBase
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters, RecordStatusEnum
from qcportal.utils import is_included
from qcportal.cache import get_records_with_cache
from qcportal.singlepoint import SinglepointProtocols, SinglepointRecord, QCSpecification, SinglepointDriver


class OptimizationSpecification(BaseModel):
    """
    An OptimizationSpecification as stored on the server

    This is the same as the input specification, with a few ids added
    """

    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = Field(..., description="The program to use for an optimization")
    qc_specification: QCSpecification
    keywords: Dict[str, Any] = Field({})
    protocols: OptimizationProtocols = Field(OptimizationProtocols())

    @validator("qc_specification", pre=True)
    def force_qcspec(cls, v):
        if isinstance(v, QCSpecification):
            v = v.dict()

        v["driver"] = SinglepointDriver.deferred
        v["protocols"] = SinglepointProtocols()
        return v


class OptimizationRecord(BaseRecord):
    record_type: Literal["optimization"] = "optimization"
    specification: OptimizationSpecification
    initial_molecule_id: int
    final_molecule_id: Optional[int]
    energies: Optional[List[float]]

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    initial_molecule_: Optional[Molecule] = Field(None, alias="initial_molecule")
    final_molecule_: Optional[Molecule] = Field(None, alias="final_molecule")
    trajectory_ids_: Optional[List[int]] = Field(None, alias="trajectory_ids")

    ##############################################
    # Fields with child records
    # (generally not received from the server)
    ##############################################
    trajectory_records_: Optional[List[SinglepointRecord]] = Field(None, alias="trajectory_records")

    @classmethod
    def _fetch_children_multi(
        cls,
        client,
        record_cache,
        records: Iterable[OptimizationRecord],
        include: Iterable[str],
        force_fetch: bool = False,
    ):
        # Should be checked by the calling function
        assert records
        assert all(isinstance(x, OptimizationRecord) for x in records)

        base_url_prefix = next(iter(records))._base_url_prefix
        assert all(r._base_url_prefix == base_url_prefix for r in records)

        if is_included("trajectory", include, None, False):
            # collect all singlepoint ids for all optimizations
            sp_ids = set()
            for r in records:
                if r.trajectory_ids_:
                    sp_ids.update(r.trajectory_ids_)

            sp_ids = list(sp_ids)
            sp_records = get_records_with_cache(
                client,
                base_url_prefix,
                record_cache,
                SinglepointRecord,
                sp_ids,
                include=include,
                force_fetch=force_fetch,
            )
            sp_map = {r.id: r for r in sp_records}

            for r in records:
                if r.trajectory_ids_ is None:
                    r.trajectory_records_ = None
                else:
                    r.trajectory_records_ = [sp_map[x] for x in r.trajectory_ids_]
                r.propagate_client(r._client, base_url_prefix)

    def propagate_client(self, client, base_url_prefix: Optional[str]):
        BaseRecord.propagate_client(self, client, base_url_prefix)

        if self.trajectory_records_ is not None:
            for sp in self.trajectory_records_:
                sp.propagate_client(client, base_url_prefix)

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_final_molecule(self):
        if self.final_molecule_id is not None:
            self._assert_online()
            self.final_molecule_ = self._client.get_molecules([self.final_molecule_id])[0]

    def _fetch_trajectory(self):
        if self.trajectory_ids_ is None:
            self._assert_online()
            self.trajectory_ids_ = self._client.make_request(
                "get",
                f"api/v1/records/optimization/{self.id}/trajectory",
                List[int],
            )

        self.fetch_children(["trajectory"])

    def get_cache_dict(self, **kwargs) -> Dict[str, Any]:
        return self.dict(exclude={"trajectory_records_"}, **kwargs)

    @property
    def initial_molecule(self) -> Molecule:
        if self.initial_molecule_ is None:
            self._fetch_initial_molecule()
        return self.initial_molecule_

    @property
    def final_molecule(self) -> Optional[Molecule]:
        if self.final_molecule_ is None:
            self._fetch_final_molecule()
        return self.final_molecule_

    @property
    def trajectory(self) -> Optional[List[SinglepointRecord]]:
        if self.trajectory_records_ is None:
            self._fetch_trajectory()
        return self.trajectory_records_

    def trajectory_element(self, trajectory_index: int) -> SinglepointRecord:
        if self.trajectory_records_ is not None:
            return self.trajectory_records_[trajectory_index]
        else:
            self._assert_online()

            if self.trajectory_ids_ is None:
                self.trajectory_ids_ = self._client.make_request(
                    "get",
                    f"api/v1/records/optimization/{self.id}/trajectory",
                    List[int],
                )

            if self.trajectory_ids_ is not None:
                traj_id = self.trajectory_ids_[trajectory_index]
                sp_rec = self._get_child_records([traj_id], SinglepointRecord)[0]
                sp_rec.propagate_client(self._client, self._base_url_prefix)
                return sp_rec
            else:
                raise RuntimeError(f"Cannot find trajectory for record {self.id}")

    def to_qcschema_result(self) -> OptimizationResult:
        if self.status != RecordStatusEnum.complete:
            raise RuntimeError(f"Cannot create QCSchema result from record with status {self.status}")

        extras = deepcopy(self.extras)
        extras["_qcfractal_modified_on"] = self.compute_history[0].modified_on

        if self.trajectory is not None:
            trajectory = [x.to_qcschema_result() for x in self.trajectory]
        else:
            trajectory = None

        # TODO - correct?
        new_keywords = deepcopy(self.specification.keywords)
        new_keywords["program"] = self.specification.qc_specification.program

        return OptimizationResult(
            initial_molecule=self.initial_molecule,
            final_molecule=self.final_molecule,
            trajectory=trajectory,
            energies=self.energies,
            keywords=new_keywords,
            input_specification=QCInputSpecification(
                driver=SinglepointDriver.gradient,  # forced
                model=AtomicResultModel(
                    method=self.specification.qc_specification.method,
                    basis=self.specification.qc_specification.basis,
                ),
                keywords=self.specification.qc_specification.keywords,
            ),
            protocols=self.specification.protocols,
            extras=extras,
            stdout=self.stdout,
            provenance=self.provenance,
            success=True,  # Status has been checked above
        )


class OptimizationQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = None
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    initial_molecule_id: Optional[List[int]] = None
    final_molecule_id: Optional[List[int]] = None

    @validator("qc_basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


class OptimizationInput(RestModelBase):
    record_type: Literal["optimization"] = "optimization"
    specification: OptimizationSpecification
    initial_molecule: Union[int, Molecule]


class OptimizationMultiInput(RestModelBase):
    specification: OptimizationSpecification
    initial_molecules: List[Union[int, Molecule]]


class OptimizationAddBody(RecordAddBodyBase, OptimizationMultiInput):
    pass
