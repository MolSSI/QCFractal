from typing import Optional, Union, Any, List, Dict, Set, Iterable

import pydantic
from pydantic import BaseModel, Field, constr, validator, Extra
from qcelemental.models import Molecule
from qcelemental.models.procedures import (
    OptimizationProtocols,
)
from typing_extensions import Literal

from qcportal.records.singlepoint import SinglepointProtocols
from ..models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from ..singlepoint import (
    SinglepointRecord,
    QCSpecification,
    SinglepointDriver,
)
from ...base_models import ProjURLParameters


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
    singlepoint_id: int
    singlepoint_record: Optional[SinglepointRecord._DataModel]


class OptimizationRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["optimization"] = "optimization"
        specification: OptimizationSpecification
        initial_molecule_id: int
        initial_molecule: Optional[Molecule]
        final_molecule_id: Optional[int]
        final_molecule: Optional[Molecule]
        energies: Optional[List[float]]
        trajectory: Optional[List[OptimizationTrajectory]]

    # This is needed for disambiguation by pydantic
    record_type: Literal["optimization"] = "optimization"
    raw_data: _DataModel

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

    def _fetch_initial_molecule(self):
        self._assert_online()
        self.raw_data.initial_molecule = self.client.get_molecules([self.raw_data.initial_molecule_id])[0]

    def _fetch_final_molecule(self):
        self._assert_online()
        if self.raw_data.final_molecule_id is not None:
            self.raw_data.final_molecule = self.client.get_molecules([self.raw_data.final_molecule_id])[0]
        else:
            self.raw_data.final_molecule = None

    def _fetch_trajectory(self):
        self._assert_online()

        url_params = {"include": ["*", "singlepoint_record"]}

        self.raw_data.trajectory = self.client._auto_request(
            "get",
            f"v1/records/optimization/{self.raw_data.id}/trajectory",
            None,
            ProjURLParameters,
            List[OptimizationTrajectory],
            None,
            url_params,
        )

    @property
    def specification(self) -> OptimizationSpecification:
        return self.raw_data.specification

    @property
    def initial_molecule_id(self) -> int:
        return self.raw_data.initial_molecule_id

    @property
    def initial_molecule(self) -> Molecule:
        if self.raw_data.initial_molecule is None:
            self._fetch_initial_molecule()
        return self.raw_data.initial_molecule

    @property
    def final_molecule_id(self) -> Optional[Molecule]:
        return self.raw_data.final_molecule

    @property
    def final_molecule(self) -> Optional[Molecule]:
        if self.raw_data.final_molecule is None:
            self._fetch_final_molecule()
        return self.raw_data.final_molecule

    @property
    def energies(self) -> Optional[List[float]]:
        return self.raw_data.energies

    @property
    def trajectory(self) -> List[Optional[SinglepointRecord]]:
        if self.raw_data.trajectory is None:
            self._fetch_trajectory()
        traj_dm = [x.singlepoint_record for x in self.raw_data.trajectory]
        return [SinglepointRecord.from_datamodel(x, self.client) for x in traj_dm]


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
