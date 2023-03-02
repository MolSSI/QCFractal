from enum import Enum
from typing import Optional, Union, Any, List, Dict, Iterable

from pydantic import BaseModel, Field, constr, validator, Extra
from qcelemental.models import Molecule
from qcelemental.models.results import (
    AtomicResultProtocols as SinglepointProtocols,
    WavefunctionProperties,
)
from typing_extensions import Literal

from qcportal.record_models import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from qcportal.wavefunctions.models import Wavefunction


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
    record_type: Literal["singlepoint"] = "singlepoint"
    specification: QCSpecification
    molecule_id: int
    return_result: Any
    properties: Optional[Dict[str, Any]]

    ######################################################
    # Fields not included when fetching the record
    ######################################################
    molecule_: Optional[Molecule] = None
    wavefunction_: Optional[Wavefunction] = None

    def propagate_client(self, client):
        BaseRecord.propagate_client(self, client)

        if self.wavefunction_ is not None:
            self.wavefunction_.propagate_client(self._client, self._base_url)

    def _fetch_molecule(self):
        self._assert_online()
        self.molecule_ = self._client.get_molecules([self.molecule_id])[0]

    def _fetch_wavefunction(self):
        self._assert_online()

        self.wavefunction_ = self._client.make_request(
            "get",
            f"v1/records/singlepoint/{self.id}/wavefunction",
            Optional[Wavefunction],
        )

        self.propagate_client(self._client)

    def _handle_includes(self, includes: Optional[Iterable[str]]):
        if includes is None:
            return

        BaseRecord._handle_includes(self, includes)

        if "molecule" in includes:
            self._fetch_molecule()
        if "wavefunction" in includes:
            self._fetch_wavefunction()

    @property
    def molecule(self) -> Molecule:
        if self.molecule_ is None:
            self._fetch_molecule()
        return self.molecule_

    @property
    def wavefunction(self) -> Optional[WavefunctionProperties]:
        if self.wavefunction_ is None:
            self._fetch_wavefunction()

        if self.wavefunction_ is not None:
            return self.wavefunction_.data
        else:
            return None


class SinglepointAddBody(RecordAddBodyBase):
    specification: QCSpecification
    molecules: List[Union[int, Molecule]]


class SinglepointQueryFilters(RecordQueryFilters):
    program: Optional[List[constr(to_lower=True)]] = None
    driver: Optional[List[SinglepointDriver]] = None
    method: Optional[List[constr(to_lower=True)]] = None
    basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    molecule_id: Optional[List[int]] = None
    keywords: Optional[List[Dict[str, Any]]] = None

    @validator("basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None
