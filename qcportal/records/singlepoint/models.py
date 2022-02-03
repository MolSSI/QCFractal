from enum import Enum
from typing import Optional, Union, Any, List

from pydantic import BaseModel, Field, constr, validator, Extra
from qcelemental.models import Molecule
from qcelemental.models.results import (
    AtomicResultProtocols as SinglepointProtocols,
    AtomicResultProperties as SinglepointProperties,
    WavefunctionProperties,
)
from typing_extensions import Literal

from .. import BaseRecord, RecordAddBodyBase, RecordQueryBody
from ...keywords import KeywordSet


class SinglepointDriver(str, Enum):
    # Copied from qcelemental to add "deferred"
    energy = "energy"
    gradient = "gradient"
    hessian = "hessian"
    properties = "properties"
    deferred = "deferred"


class QCInputSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = Field(
        ...,
        description="The quantum chemistry program to evaluate the computation with. Not all quantum chemistry programs"
        " support all combinations of driver/method/basis.",
    )
    driver: SinglepointDriver = Field(...)
    method: constr(to_lower=True) = Field(
        ..., description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...)."
    )
    basis: Optional[constr(to_lower=True)] = Field(
        ...,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
        "methods without basis sets.",
    )
    keywords: Union[int, KeywordSet] = Field(
        KeywordSet(values={}),
        description="Keywords to use. Can be an ID of the keywords on the server or a KeywordSet object",
    )
    protocols: SinglepointProtocols = Field(SinglepointProtocols(), description=str(SinglepointProtocols.__base_doc__))

    @validator("basis", pre=True)
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        return None if v == "" else v


class QCSpecification(QCInputSpecification):
    """
    A QCSpecification as stored on the server

    This is the same as the input specification, with a few ids added
    """

    id: int
    keywords_id: int

    def as_input(self) -> QCInputSpecification:
        return QCInputSpecification(**self.dict(exclude={"id", "keywords_id"}))


class SinglepointRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["singlepoint"]
        specification_id: int
        specification: QCSpecification
        molecule_id: int
        molecule: Optional[Molecule]
        return_result: Any
        properties: Optional[SinglepointProperties]
        wavefunction: Optional[WavefunctionProperties] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["singlepoint"]
    raw_data: _DataModel

    def _retrieve_molecule(self):
        self.raw_data.molecule = self.client.get_molecules([self.raw_data.molecule_id])[0]

    def _retrieve_wavefunction(self):
        self.raw_data.wavefunction = self.client._auto_request(
            "get",
            f"v1/record/singlepoint/{self.raw_data.id}/wavefunction",
            None,
            None,
            Optional[WavefunctionProperties],
            None,
            None,
        )

    @property
    def specification_id(self) -> int:
        return self.raw_data.specification_id

    @property
    def specification(self) -> QCSpecification:
        return self.raw_data.specification

    @property
    def molecule_id(self) -> int:
        return self.raw_data.molecule_id

    @property
    def molecule(self) -> Molecule:
        if self.raw_data.molecule is None:
            self._retrieve_molecule()
        return self.raw_data.molecule

    @property
    def return_result(self) -> Any:
        return self.raw_data.return_result

    @property
    def properties(self) -> SinglepointProperties:
        return self.raw_data.properties

    @property
    def wavefunction(self) -> WavefunctionProperties:
        if self.raw_data.wavefunction is None:
            self._retrieve_wavefunction()
        return self.raw_data.wavefunction


class SinglepointAddBody(RecordAddBodyBase):
    specification: QCInputSpecification
    molecules: List[Union[int, Molecule]]


class SinglepointQueryBody(RecordQueryBody):
    program: Optional[List[constr(to_lower=True)]] = None
    driver: Optional[List[SinglepointDriver]] = None
    method: Optional[List[constr(to_lower=True)]] = None
    basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    keywords_id: Optional[List[int]] = None
    molecule_id: Optional[List[int]] = None

    @validator("basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None
