from typing import List, Union, Optional, Tuple

from pydantic import BaseModel, Extra, validator
from typing_extensions import Literal

from .. import BaseRecord, RecordAddBodyBase
from ..singlepoint.models import (
    QCSpecification,
    SinglepointQueryBody,
    SinglepointRecord,
    SinglepointDriver,
)
from ...base_models import ProjURLParameters
from ...molecules import Molecule


class ReactionQCSpecification(QCSpecification):
    driver: SinglepointDriver = SinglepointDriver.energy

    @validator("driver", pre=True)
    def force_driver(cls, v):
        return SinglepointDriver.energy


class ReactionAddBody(RecordAddBodyBase):
    specification: ReactionQCSpecification
    stoichiometries: List[List[Tuple[float, Union[int, Molecule]]]]


class ReactionQueryBody(SinglepointQueryBody):
    pass


class ReactionStoichiometry(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    coefficient: float

    molecule: Optional[Molecule]


class ReactionComponent(BaseModel):
    class Config:
        extra = Extra.forbid

    molecule_id: int
    singlepoint_id: int

    energy: Optional[float] = None
    singlepoint_record: Optional[SinglepointRecord._DataModel]


class ReactionRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["reaction"]
        specification: QCSpecification

        total_energy: Optional[float]

        stoichiometries: Optional[List[ReactionStoichiometry]] = None
        components: Optional[List[ReactionComponent]] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["reaction"]
    raw_data: _DataModel

    def _fetch_components(self):
        url_params = {"include": ["*", "singlepoint_record"]}

        self.raw_data.components = self.client._auto_request(
            "get",
            f"v1/records/reaction/{self.raw_data.id}/components",
            None,
            ProjURLParameters,
            List[ReactionComponent],
            None,
            url_params,
        )

    def _fetch_stoichiometries(self):
        url_params = {"include": ["*", "molecule"]}

        self.raw_data.stoichiometries = self.client._auto_request(
            "get",
            f"v1/records/reaction/{self.raw_data.id}/stoichiometries",
            None,
            ProjURLParameters,
            List[ReactionStoichiometry],
            None,
            url_params,
        )

    @property
    def specification_id(self) -> int:
        return self.raw_data.specification_id

    @property
    def specification(self) -> QCSpecification:
        return self.raw_data.specification

    @property
    def components(self) -> List[ReactionComponent]:
        if self.raw_data.components is None:
            self._fetch_components()

        return self.raw_data.components

    @property
    def stoichiometries(self) -> List[ReactionStoichiometry]:
        if self.raw_data.stoichiometries is None:
            self._fetch_stoichiometries()

        return self.raw_data.stoichiometries

    @property
    def total_energy(self) -> Optional[float]:
        return self.raw_data.total_energy
