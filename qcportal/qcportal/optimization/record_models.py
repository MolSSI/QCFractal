from typing import Optional, Union, Any, List, Dict, Set, Iterable

import pydantic
from pydantic import BaseModel, Field, constr, validator, Extra
from qcelemental.models import Molecule
from qcelemental.models.procedures import (
    OptimizationProtocols,
)
from typing_extensions import Literal

from qcportal.base_models import ProjURLParameters
from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
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

    @pydantic.validator("qc_specification", pre=True)
    def force_qcspec(cls, v):
        if isinstance(v, QCSpecification):
            v = v.dict()

        v["driver"] = SinglepointDriver.deferred
        v["protocols"] = SinglepointProtocols()
        return v


class OptimizationTrajectory(BaseModel):
    class Config:
        extra = Extra.forbid

    singlepoint_id: int
    singlepoint_record: Optional[SinglepointRecord]


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
    trajectory_: Optional[List[OptimizationTrajectory]] = Field(None, alias="trajectory")

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:

        if includes is None:
            return None

        ret = BaseRecord.transform_includes(includes)

        if "initial_molecule" in includes:
            ret.add("initial_molecule")
        if "final_molecule" in includes:
            ret.add("final_molecule")
        if "trajectory" in includes:
            ret |= {"trajectory.*", "trajectory.singlepoint_record"}

        return ret

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self.trajectory_ is not None:
            for sp in self.trajectory_:
                if sp.singlepoint_record:
                    sp.singlepoint_record.propagate_client(client)

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.initial_molecule_ = self._client.get_molecules([self.initial_molecule_id])[0]

    def _fetch_final_molecule(self):
        self._assert_online()
        if self.final_molecule_id is not None:
            self.final_molecule_ = self._client.get_molecules([self.final_molecule_id])[0]
        else:
            self.final_molecule_ = None

    def _fetch_trajectory(self):
        self._assert_online()

        url_params = {"include": ["*", "singlepoint_record"]}

        self.trajectory_ = self._client._auto_request(
            "get",
            f"v1/records/optimization/{self.id}/trajectory",
            None,
            ProjURLParameters,
            List[OptimizationTrajectory],
            None,
            url_params,
        )

        self.propagate_client(self._client)

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
        if self.trajectory_ is None:
            self._fetch_trajectory()

        return [x.singlepoint_record for x in self.trajectory_]

    def trajectory_element(self, trajectory_index: int) -> SinglepointRecord:
        if self.trajectory_ is not None:
            return self.trajectory_[trajectory_index].singlepoint_record
        else:
            url_params = {}

            sp_rec = self._client._auto_request(
                "get",
                f"v1/records/optimization/{self.id}/trajectory/{trajectory_index}",
                None,
                ProjURLParameters,
                SinglepointRecord,
                None,
                url_params,
            )

            sp_rec.propagate_client(self._client)
            return sp_rec


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


class OptimizationAddBody(RecordAddBodyBase):
    specification: OptimizationSpecification
    initial_molecules: List[Union[int, Molecule]]
