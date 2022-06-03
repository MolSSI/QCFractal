from typing import List, Optional, Tuple, Union, Dict

import pydantic
from pydantic import BaseModel, Field, Extra, root_validator, constr, validator
from typing_extensions import Literal

from .. import BaseRecord, RecordAddBodyBase, RecordQueryFilters
from ..singlepoint.models import QCSpecification, SinglepointRecord, SinglepointDriver, SinglepointProtocols
from ...base_models import ProjURLParameters
from ...molecules import Molecule
from ...utils import recursive_normalizer


class NEBKeywords(BaseModel):
    """
    NEBRecord options
    """

    images: int = Field(
        11,
        description="Number of images that will be used to locate a rough transition state structure.",
        gt=5,
    )
 
    spring_constant: float = Field(
        0.1,
        description="Spring constant in eV/Ang^2.",
    )
        

    energy_weighted: bool = Field(
        False,
        description="Energy weighted NEB method varies the spring constant based on the image's energy.",
    )

    @root_validator
    def normalize(cls, values):
        return recursive_normalizer(values)


class NEBSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "geometric"
    singlepoint_specification: QCSpecification
    keywords: NEBKeywords

    @pydantic.validator("singlepoint_specification", pre=True)
    def force_qcspec(cls, v):
        if isinstance(v, QCSpecification):
            v = v.dict()
 
        v["driver"] = SinglepointDriver.gradient
        v["protocols"] = SinglepointProtocols()
        return v



class NEBSinglepoint(BaseModel):
    class Config:
        extra = Extra.forbid

    singlepoint_id: int
    iteration: int
    position: int

    gradients: Optional[List[float]] = None
    singlepoint_record: Optional[SinglepointRecord._DataModel]


class NEBInitialchain(BaseModel):
    id: int
    molecule_id: int
    position: int

    molecule: Optional[Molecule]
    

class NEBAddBody(RecordAddBodyBase):
    specification: NEBSpecification
    initial_chains: List[List[Union[int, Molecule]]]


class NEBQueryFilters(RecordQueryFilters):
    program: Optional[List[str]] = None
    neb_program: Optional[List[str]]
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    initial_chain_id: Optional[List[int]] = None

    @validator("qc_basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


class NEBRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["neb"] = 'neb'
        specification: NEBSpecification
        initial_chain: Optional[List[Molecule]] = None
        singlepoints: Optional[List[NEBSinglepoint]] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["neb"] = 'neb'
    raw_data: _DataModel

    singlepoint_cache: Optional[Dict[str, SinglepointRecord]] = None

    def _fetch_initial_chain(self):
        self.raw_data.initial_chain = self.client._auto_request(
            "get",
            f"v1/records/neb/{self.raw_data.id}/initial_chain",
            None,
            None,
            List[Molecule],
            None,
            None,
        )

    def _fetch_singlepoints(self):
        url_params = {"include": ["*", "singlepoint_record"]}

        self.raw_data.singlepoints = self.client._auto_request(
            "get",
            f"v1/records/neb/{self.raw_data.id}/singlepoints",
            None,
            ProjURLParameters,
            List[NEBSinglepoint],
            None,
            url_params,
        )

    @property
    def specification(self) -> NEBSpecification:
        return self.raw_data.specification

    @property
    def initial_chain(self) -> List[Molecule]:
        if self.raw_data.initial_chain is None:
            self._fetch_initial_chain()
        return self.raw_data.initial_chain

    @property
    def singlepoints(self) -> Dict[str, SinglepointRecord]:
        if self.singlepoint_cache is not None:
            return self.singlepoint_cache

        # convert the raw singlepoint data to a dictionary of key -> SinglepointRecord
        if self.raw_data.singlepoints is None:
            self._fetch_singlepoints()

        ret = {}
        for sp in self.raw_data.singlepoints:
            ret.setdefault(sp.key, list())
            ret[sp.key].append(self.client.recordmodel_from_datamodel([sp.singlepoint_record])[0])
        self.singlepoint_cache = ret
        return ret
