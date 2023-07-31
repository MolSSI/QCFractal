from copy import deepcopy
from enum import Enum
from typing import Optional, Union, Any, List, Dict, Iterable, Tuple

try:
    from pydantic.v1 import BaseModel, Field, constr, validator, Extra, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Field, constr, validator, Extra, PrivateAttr
from qcelemental.models import Molecule
from qcelemental.models.results import (
    AtomicResult,
    Model as AtomicResultModel,
    AtomicResultProtocols as SinglepointProtocols,
    AtomicResultProperties,
    WavefunctionProperties,
)
from typing_extensions import Literal

from qcportal.compression import CompressionEnum, decompress
from qcportal.record_models import RecordStatusEnum, BaseRecord, RecordAddBodyBase, RecordQueryFilters


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


class Wavefunction(BaseModel):
    """
    Storage of wavefunctions, with compression
    """

    class Config:
        extra = Extra.forbid

    compression_type: CompressionEnum

    data_url_: Optional[str] = None
    compressed_data_: Optional[bytes] = None
    decompressed_data_: Optional[WavefunctionProperties] = None

    _client: Any = PrivateAttr(None)

    def propagate_client(self, client, record_base_url):
        self._client = client
        self.data_url_ = f"{record_base_url}/wavefunction/data"

    def _fetch_raw_data(self):
        if self.compressed_data_ is None and self.decompressed_data_ is None:
            cdata, ctype = self._client.make_request(
                "get",
                self.data_url_,
                Tuple[bytes, CompressionEnum],
            )

            assert self.compression_type == ctype
            self.compressed_data_ = cdata

    @property
    def data(self) -> WavefunctionProperties:
        self._fetch_raw_data()

        # Decompress, then remove compressed form
        if self.decompressed_data_ is None:
            wfn_dict = decompress(self.compressed_data_, self.compression_type)
            self.decompressed_data_ = WavefunctionProperties(**wfn_dict)
            self.compressed_data_ = None

        return self.decompressed_data_


class SinglepointRecord(BaseRecord):
    record_type: Literal["singlepoint"] = "singlepoint"
    specification: QCSpecification
    molecule_id: int

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
            f"api/v1/records/singlepoint/{self.id}/wavefunction",
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
    def return_result(self) -> Any:
        # Return result is stored in properties in QCFractal
        return self.properties.get("return_result", None)

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

    def to_qcschema_result(self) -> AtomicResult:
        if self.status != RecordStatusEnum.complete:
            raise RuntimeError(f"Cannot create QCSchema result from record with status {self.status}")

        extras = deepcopy(self.extras)
        extras["_qcfractal_modified_on"] = self.compute_history[0].modified_on

        # QCArchive properties include more than AtomicResultProperties
        if self.properties:
            prop_fields = AtomicResultProperties.__fields__.keys()
            new_properties = {k: v for k, v in self.properties.items() if k in prop_fields}
            extras["extra_properties"] = {k: v for k, v in self.properties.items() if k not in prop_fields}
        else:
            new_properties = {}

        return AtomicResult(
            driver=self.specification.driver,
            model=AtomicResultModel(
                method=self.specification.method,
                basis=self.specification.basis,
            ),
            molecule=self.molecule,
            keywords=self.specification.keywords,
            properties=AtomicResultProperties(**new_properties),
            protocols=self.specification.protocols,
            return_result=self.return_result,
            extras=extras,
            stdout=self.stdout,
            native_files={k: v.data for k, v in self.native_files.items()},
            wavefunction=self.wavefunction,
            provenance=self.provenance,
            success=True,  # Status has been checked above
        )


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
