from typing import Optional, Union, Any, List, Dict

import pydantic
from pydantic import BaseModel, Field, constr, validator, Extra
from qcelemental.models import Molecule
from qcelemental.models.procedures import (
    OptimizationProtocols,
)
from typing_extensions import Literal

from .. import BaseRecord, RecordAddBodyBase, RecordQueryBody
from ..singlepoint import (
    SinglepointRecord,
    QCSpecification,
    QCInputSpecification,
    SinglepointDriver,
)
from ...base_models import ProjURLParameters


class OptimizationQCInputSpecification(QCInputSpecification):
    driver: SinglepointDriver = SinglepointDriver.deferred

    @pydantic.validator("driver", pre=True)
    def force_driver(cls, v):
        return SinglepointDriver.deferred


class OptimizationInputSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = Field(..., description="The program to use for an optimization")
    qc_specification: OptimizationQCInputSpecification
    keywords: Dict[str, Any] = Field({})
    protocols: OptimizationProtocols = Field(OptimizationProtocols())


class OptimizationSpecification(OptimizationInputSpecification):
    """
    An OptimizationSpecification as stored on the server

    This is the same as the input specification, with a few ids added
    """

    id: int
    qc_specification: QCSpecification
    qc_specification_id: int

    def as_input(self) -> OptimizationInputSpecification:
        qc_input_spec = self.qc_specification.as_input()
        return OptimizationInputSpecification(
            **self.dict(exclude={"id", "qc_specification_id", "qc_specification"}),
            qc_specification=OptimizationQCInputSpecification(**qc_input_spec.dict()),
        )


class OptimizationTrajectory(BaseModel):
    singlepoint_id: int
    optimization_id: int
    singlepoint_record: Optional[SinglepointRecord._DataModel]


class OptimizationRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["optimization"]
        specification_id: int
        specification: OptimizationSpecification
        initial_molecule_id: int
        initial_molecule: Optional[Molecule]
        final_molecule_id: Optional[int]
        final_molecule: Optional[Molecule]
        energies: Optional[List[float]]
        trajectory: Optional[List[OptimizationTrajectory]]

    # This is needed for disambiguation by pydantic
    record_type: Literal["optimization"]
    raw_data: _DataModel

    def _retrieve_initial_molecule(self):
        self.raw_data.initial_molecule = self.client.get_molecules([self.raw_data.initial_molecule_id])[0]

    def _retrieve_final_molecule(self):
        if self.raw_data.final_molecule_id is not None:
            self.raw_data.final_molecule = self.client.get_molecules([self.raw_data.final_molecule_id])[0]
        else:
            self.raw_data.final_molecule = None

    def _retrieve_trajectory(self):
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
    def specification_id(self) -> int:
        return self.raw_data.specification_id

    @property
    def specification(self) -> OptimizationSpecification:
        return self.raw_data.specification

    @property
    def initial_molecule_id(self) -> int:
        return self.raw_data.initial_molecule_id

    @property
    def initial_molecule(self) -> Molecule:
        if self.raw_data.initial_molecule is None:
            self._retrieve_initial_molecule()
        return self.raw_data.initial_molecule

    @property
    def final_molecule_id(self) -> Optional[Molecule]:
        return self.raw_data.final_molecule

    @property
    def final_molecule(self) -> Optional[Molecule]:
        if self.raw_data.final_molecule is None:
            self._retrieve_final_molecule()
        return self.raw_data.final_molecule

    @property
    def energies(self) -> Optional[List[float]]:
        return self.raw_data.energies

    @property
    def trajectory(self) -> Molecule:
        if self.raw_data.trajectory is None:
            self._retrieve_trajectory()
        traj_dm = [x.singlepoint_record for x in self.raw_data.trajectory]
        return self.client.recordmodel_from_datamodel(traj_dm)


class OptimizationQueryBody(RecordQueryBody):
    program: Optional[List[str]] = None
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    qc_keywords_id: Optional[List[int]] = None
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
    specification: OptimizationInputSpecification
    initial_molecules: List[Union[int, Molecule]]
