from enum import Enum
from typing import List, Union, Optional

from pydantic import BaseModel, Extra, Field, constr, validator
from typing_extensions import Literal

from .. import BaseRecord, RecordAddBodyBase, RecordQueryBody
from ..optimization.models import OptimizationInputSpecification, OptimizationSpecification, OptimizationRecord
from ...molecules import Molecule
from ...utils import recursive_normalizer


class ScanTypeEnum(str, Enum):
    """
    The type of scan to perform. This choices is limited to the scan types allowed by the scan dimensions.
    """

    distance = "distance"
    angle = "angle"
    dihedral = "dihedral"


class StepTypeEnum(str, Enum):
    """
    The types of steps to take in a scan dimension: either in absolute or relative terms. ``relative`` indicates that
    the values are relative to the starting value (e.g., a bond starts as 2.1 Bohr, relative steps of [-0.1, 0, 1.0]
    indicate grid points of [2.0, 2.1, 3.1] Bohr. An ``absolute`` ``step_type`` will be exactly those values instead."
    """

    absolute = "absolute"
    relative = "relative"


class ScanDimension(BaseModel):
    """
    A full description of a dimension to scan over.
    """

    type: ScanTypeEnum = Field(..., description=str(ScanTypeEnum.__doc__))
    indices: List[int] = Field(
        ...,
        description="The indices of atoms to select for the scan. The size of this is a function of the type. e.g., "
        "distances, angles and dihedrals require 2, 3, and 4 atoms, respectively.",
    )
    steps: List[float] = Field(
        ...,
        description="Step sizes to scan in relative to your current location in the scan. This must be a strictly "
        "monotonic series.",
        units=["Bohr", "degrees"],
    )
    step_type: StepTypeEnum = Field(..., description=str(StepTypeEnum.__doc__))

    @validator("type", "step_type", pre=True)
    def check_lower_type_step_type(cls, v):
        return v.lower()

    @validator("indices")
    def check_indices(cls, v, values, **kwargs):
        sizes = {ScanTypeEnum.distance: 2, ScanTypeEnum.angle: 3, ScanTypeEnum.dihedral: 4}
        if sizes[values["type"]] != len(v):
            raise ValueError(
                "ScanDimension of type {} must have {} values, found {}.".format(
                    values["type"], sizes[values["type"]], len(v)
                )
            )

        return v

    @validator("steps")
    def check_steps(cls, v):
        if not (all(x < y for x, y in zip(v, v[1:])) or all(x > y for x, y in zip(v, v[1:]))):
            raise ValueError("Steps are not strictly monotonically increasing or decreasing.")

        v = recursive_normalizer(v)

        return v


class GridoptimizationKeywords(BaseModel):
    """
    Keywords for grid optimizations
    """

    scans: List[ScanDimension] = Field(
        ..., description="The dimensions to scan along (along with their options) for the Gridoptimization."
    )
    preoptimization: bool = Field(
        True,
        description="If ``True``, first runs an unrestricted optimization before starting the grid computations. "
        "This is especially useful when combined with ``relative`` ``step_types``.",
    )


class GridoptimizationInputSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    program: constr(to_lower=True) = "gridoptimization"
    optimization_specification: OptimizationInputSpecification
    keywords: GridoptimizationKeywords


class GridoptimizationSpecification(GridoptimizationInputSpecification):
    id: int
    optimization_specification_id: int
    optimization_specification: OptimizationSpecification


class GridoptimizationAddBody(RecordAddBodyBase):
    specification: GridoptimizationInputSpecification
    initial_molecules: List[Union[int, Molecule]]


class GridoptimizationQueryBody(RecordQueryBody):
    program: Optional[List[str]] = None
    optimization_program: Optional[List[str]]
    qc_program: Optional[List[constr(to_lower=True)]] = None
    qc_method: Optional[List[constr(to_lower=True)]] = None
    qc_basis: Optional[List[Optional[constr(to_lower=True)]]] = None
    qc_keywords_id: Optional[List[int]] = None
    initial_molecule_id: Optional[List[int]] = None

    @validator("qc_basis")
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        if v is not None:
            return ["" if x is None else x for x in v]
        else:
            return None


class GridoptimizationOptimization(BaseModel):
    class Config:
        extra = Extra.forbid

    gridoptimization_id: int
    optimization_id: int
    key: str

    energy: float
    optimization_record: OptimizationRecord._DataModel


class GridoptimizationRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["gridoptimization"]
        specification_id: int
        specification: GridoptimizationSpecification
        starting_grid: Optional[List[int]]
        initial_molecule_id: int
        initial_molecule: Optional[Molecule] = None
        starting_molecule_id: Optional[int]
        starting_molecule: Optional[Molecule] = None
        optimizations: Optional[List[GridoptimizationOptimization]] = None

    # This is needed for disambiguation by pydantic
    record_type: Literal["gridoptimization"]
    raw_data: _DataModel

    def _retrieve_initial_molecule(self):
        self.raw_data.initial_molecule = self.client.get_molecules([self.raw_data.initial_molecule_id])[0]

    def _retrieve_starting_molecule(self):
        self.raw_data.starting_molecule = self.client.get_molecules([self.raw_data.starting_molecule_id])[0]

    def _retrieve_optimizations(self):
        self.raw_data.optimizations = self.client._auto_request(
            "get",
            f"v1/records/gridoptimization/{self.raw_data.id}/optimizations",
            None,
            None,
            List[GridoptimizationOptimization],
            None,
            None,
        )

    @property
    def specification_id(self) -> int:
        return self.raw_data.specification_id

    @property
    def specification(self) -> GridoptimizationSpecification:
        return self.raw_data.specification

    @property
    def starting_grid(self) -> Optional[List[int]]:
        return self.raw_data.starting_grid

    @property
    def initial_molecule_id(self) -> int:
        return self.raw_data.initial_molecule_id

    @property
    def initial_molecule(self) -> Molecule:
        if self.raw_data.initial_molecule is None:
            self._retrieve_initial_molecule()
        return self.raw_data.initial_molecule

    @property
    def starting_molecule_id(self) -> Optional[int]:
        return self.raw_data.starting_molecule_id

    @property
    def starting_molecule(self) -> Optional[Molecule]:
        if self.raw_data.initial_molecule is None:
            self._retrieve_initial_molecule()
        return self.raw_data.initial_molecule

    @property
    def optimizations(self) -> List[OptimizationRecord]:
        if self.raw_data.optimizations is None:
            self._retrieve_optimizations()
        opt_dm = [x.optimization_record for x in self.raw_data.optimizations]
        return self.client.recordmodel_from_datamodel(opt_dm)
