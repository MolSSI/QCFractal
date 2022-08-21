from enum import Enum
from typing import Optional, Union, Any, List, Dict, Set, Iterable

from pydantic import BaseModel, Field, constr, validator, Extra
from qcelemental.models import Molecule
from qcelemental.models.results import (
    AtomicResultProtocols as SinglepointProtocols,
    AtomicResultProperties as SinglepointProperties,
    WavefunctionProperties,
)
from typing_extensions import Literal

from ..models import BaseRecord, RecordAddBodyBase, RecordQueryFilters


class SinglepointDriver(str, Enum):
    # Copied from qcelemental to add "deferred"
    energy = "energy"
    gradient = "gradient"
    hessian = "hessian"
    properties = "properties"
    deferred = "deferred"


class QCSpecification(BaseModel):
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
    keywords: Dict[str, Any] = Field({}, description="Program-specific keywords to use for the computation")
    protocols: SinglepointProtocols = Field(SinglepointProtocols(), description=str(SinglepointProtocols.__base_doc__))

    @validator("basis", pre=True)
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        return None if v == "" else v


class SinglepointRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["singlepoint"] = "singlepoint"
        specification: QCSpecification
        molecule_id: int
        molecule: Optional[Molecule]
        return_result: Any
        properties: Optional[SinglepointProperties]
        wavefunction: Optional[WavefunctionProperties] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["singlepoint"] = "singlepoint"
    raw_data: _DataModel

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:

        if includes is None:
            return None

        ret = BaseRecord.transform_includes(includes)

        if "molecule" in includes:
            ret.add("molecule")
        if "wavefunction" in includes:
            ret.add("wavefunction")

        return ret

    def _fetch_molecule(self):
        self._assert_online()
        self.raw_data.molecule = self.client.get_molecules([self.raw_data.molecule_id])[0]

    def _fetch_wavefunction(self):
        self._assert_online()

        self.raw_data.wavefunction = self.client._auto_request(
            "get",
            f"v1/records/singlepoint/{self.raw_data.id}/wavefunction",
            None,
            None,
            Optional[WavefunctionProperties],
            None,
            None,
        )

    @property
    def specification(self) -> QCSpecification:
        return self.raw_data.specification

    @property
    def molecule_id(self) -> int:
        return self.raw_data.molecule_id

    @property
    def molecule(self) -> Molecule:
        if self.raw_data.molecule is None:
            self._fetch_molecule()
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
            self._fetch_wavefunction()
        return self.raw_data.wavefunction


class SinglepointAddBody(RecordAddBodyBase):
    specification: QCSpecification
    molecules: List[Union[int, Molecule]]


class SinglepointQueryFilters(RecordQueryFilters):
    program: Optional[List[constr(to_lower=True)]] = None
    driver: Optional[List[SinglepointDriver]] = None
    method: Optional[List[constr(to_lower=True)]] = None
    basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    molecule_id: Optional[List[int]] = None

    @validator("basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None
