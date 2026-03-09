from __future__ import annotations

from copy import deepcopy
from enum import Enum
from typing import Literal, Any

from pydantic import BaseModel, Field, field_validator, ConfigDict, PrivateAttr

from qcportal.base_models import RestModelBase
from qcportal.common_types import LowerStr, QCPortalBytes
from qcportal.compression import CompressionEnum, decompress
from qcportal.exceptions import NoClientError
from qcportal.molecules import Molecule
from qcportal.qcschema_v1 import WavefunctionProperties, AtomicResult, AtomicResultProperties
from qcportal.record_models import (
    RecordStatusEnum,
    BaseRecord,
    RecordAddBodyBase,
    RecordQueryFilters,
    compare_base_records,
)


class Model(BaseModel):
    """The computational molecular sciences model to run."""

    method: str = Field(  # type: ignore
        ...,
        description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...). "
        "For MM, name of the force field.",
    )
    basis: str | None = Field(  # type: ignore
        None,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
        "methods without basis sets. For molecular mechanics, name of the atom-typer.",
    )

    class Config:
        extra: str = "allow"


class SinglepointDriver(str, Enum):
    # Copied from qcelemental to add "deferred"
    energy = "energy"
    gradient = "gradient"
    hessian = "hessian"
    properties = "properties"
    deferred = "deferred"


class WavefunctionProtocolEnum(str, Enum):
    r"""Wavefunction to keep from a computation."""

    all = "all"
    orbitals_and_eigenvalues = "orbitals_and_eigenvalues"
    occupations_and_eigenvalues = "occupations_and_eigenvalues"
    return_results = "return_results"
    none = "none"


class ErrorCorrectionProtocol(BaseModel):
    r"""Configuration for how computational chemistry programs handle error correction"""

    default_policy: bool = Field(
        True, description="Whether to allow error corrections to be used " "if not directly specified in `policies`"
    )
    policies: dict[str, bool] | None = Field(
        None,
        description="Settings that define whether specific error corrections are allowed. "
        "Keys are the name of a known error and values are whether it is allowed to be used.",
    )

    def allows(self, policy: str):
        if self.policies is None:
            return self.default_policy
        return self.policies.get(policy, self.default_policy)


class NativeFilesProtocolEnum(str, Enum):
    r"""Any program-specific files to keep from a computation."""

    all = "all"
    input = "input"
    none = "none"


class SinglepointProtocols(BaseModel):
    r"""Protocols regarding the manipulation of computational result data."""

    wavefunction: WavefunctionProtocolEnum = Field(
        WavefunctionProtocolEnum.none, description=str(WavefunctionProtocolEnum.__doc__)
    )
    stdout: bool = Field(True, description="Primary output file to keep from the computation")
    error_correction: ErrorCorrectionProtocol = Field(
        default_factory=ErrorCorrectionProtocol, description="Policies for error correction"
    )
    native_files: NativeFilesProtocolEnum = Field(
        NativeFilesProtocolEnum.none,
        description="Policies for keeping processed files from the computation",
    )


class QCSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    program: LowerStr = Field(
        ...,
        description="The quantum chemistry program to evaluate the computation with. Not all quantum chemistry programs"
        " support all combinations of driver/method/basis.",
    )
    driver: SinglepointDriver = Field(...)
    method: LowerStr = Field(..., description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...).")
    basis: LowerStr | None = Field(
        ...,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
        "methods without basis sets.",
    )
    keywords: dict[str, Any] = Field({}, description="Program-specific keywords to use for the computation")
    protocols: SinglepointProtocols = Field(default_factory=SinglepointProtocols)

    @field_validator("basis", mode="before")
    @classmethod
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by pydantic
        return None if v == "" else v


class Wavefunction(BaseModel):
    """
    Storage of wavefunctions, with compression
    """

    model_config = ConfigDict(extra="forbid")

    compression_type: CompressionEnum
    data_: QCPortalBytes | None = Field(None, alias="data")

    _data_url: str | None = PrivateAttr(None)
    _client: Any = PrivateAttr(None)

    def propagate_client(self, client, record_base_url):
        self._client = client
        self._data_url = f"{record_base_url}/wavefunction/data"

    def _fetch_raw_data(self):
        if self.data_ is not None:
            return

        if self._client is None:
            raise RuntimeError("No client to fetch wavefunction data from")

        cdata, ctype = self._client.make_request(
            "get",
            self._data_url,
            tuple[QCPortalBytes, CompressionEnum],
        )

        assert self.compression_type == ctype
        self.data_ = cdata

    @property
    def data(self) -> WavefunctionProperties:
        self._fetch_raw_data()
        wfn_dict = decompress(self.data_, self.compression_type)
        return WavefunctionProperties(**wfn_dict)


class SinglepointRecord(BaseRecord):
    record_type: Literal["singlepoint"] = "singlepoint"
    specification: QCSpecification
    molecule_id: int

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    molecule_: Molecule | None = Field(None, alias="molecule")
    wavefunction_: Wavefunction | None = Field(None, alias="wavefunction")

    def propagate_client(self, client, base_url_prefix: str | None):
        BaseRecord.propagate_client(self, client, base_url_prefix)

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
            Wavefunction | None,
        )

        self.propagate_client(self._client, self._base_url_prefix)

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
    def wavefunction(self) -> WavefunctionProperties | None:
        # wavefunction may be None if it doesn't exist or hasn't been fetched yet
        if self.wavefunction_ is None and "wavefunction_" not in self.model_fields_set and not self.offline:
            self._fetch_wavefunction()

        if self.wavefunction_ is not None:
            return self.wavefunction_.data
        else:
            return None

    def to_qcschema_result(self) -> AtomicResult:
        if self.status != RecordStatusEnum.complete:
            raise RuntimeError(f"Cannot create QCSchema result from record with status {self.status}")

        try:
            extras = deepcopy(self.extras)
            extras["_qcfractal_modified_on"] = self.compute_history[0].modified_on

            # QCArchive properties include more than AtomicResultProperties
            if self.properties:
                prop_fields = AtomicResultProperties.model_fields.keys()
                new_properties = {k: v for k, v in self.properties.items() if k in prop_fields}
                extras["extra_properties"] = {k: v for k, v in self.properties.items() if k not in prop_fields}
            else:
                new_properties = {}

            return AtomicResult(
                driver=self.specification.driver,
                model=dict(
                    method=self.specification.method,
                    basis=self.specification.basis,
                ),
                molecule=self.molecule,
                keywords=self.specification.keywords,
                properties=AtomicResultProperties(**new_properties),
                protocols=self.specification.protocols.model_dump(),
                return_result=self.return_result,
                extras=extras,
                stdout=self.stdout,
                native_files={k: v.data for k, v in self.native_files.items()},
                wavefunction=self.wavefunction,
                provenance=self.provenance.model_dump(),
                success=True,  # Status has been checked above
            )
        except NoClientError:
            raise RuntimeError(
                "Record does not contain the required data for a QCSchema result, and this record is "
                "not connected to a client. If fetching records, use include=['**']. "
                "If this is from a dataset view, use include=['**'] and include_children=True "
                "when creating the view"
            )


class SinglepointInput(RestModelBase):
    record_type: Literal["singlepoint"] = "singlepoint"
    specification: QCSpecification
    molecule: int | Molecule


class SinglepointMultiInput(RestModelBase):
    specification: QCSpecification
    molecules: list[int | Molecule]


class SinglepointAddBody(RecordAddBodyBase, SinglepointMultiInput):
    pass


class SinglepointQueryFilters(RecordQueryFilters):
    program: list[LowerStr] | None = None
    driver: list[SinglepointDriver] | None = None
    method: list[LowerStr] | None = None
    basis: list[LowerStr | None] | None = None
    molecule_id: list[int] | None = None
    keywords: list[dict[str, Any]] | None = None

    @field_validator("basis")
    @classmethod
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by pydantic
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


def compare_singlepoint_records(record_1: SinglepointRecord, record_2: SinglepointRecord):
    compare_base_records(record_1, record_2)

    assert record_1.molecule == record_2.molecule
    assert (record_1.wavefunction is not None) == (record_2.wavefunction is not None)
    if record_1.wavefunction is not None:
        assert record_1.wavefunction.model_dump(mode="json") == record_2.wavefunction.model_dump(mode="json")
