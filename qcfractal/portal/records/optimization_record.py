from typing import Any, Optional, Dict, List

from pydantic import Field, constr, validator
import qcelemental as qcel

from ...interface.models import QCSpecification, OptimizationSpecification
from ...interface.models import ObjectId, DriverEnum
from .record import Record
from .record_utils import register_record


class OptimizationRecord(Record):
    """
    User-facing API for accessing data for a single optimization.

    """
    _SpecModel = OptimizationSpecification
    _type = "optimization"

    class _DataModel(Record._DataModel):
        # Class data
        _hash_indices = {"initial_molecule", "keywords", "qc_spec"}

        # Version data
        version: int = Field(1, description="Version of the OptimizationRecord Model which this data was created with.")
        procedure: constr(strip_whitespace=True, regex="optimization") = Field(
            "optimization", description='A fixed string indication this is a record for an "Optimization".'
        )
        schema_version: int = Field(1, description="The version number of QCSchema under which this record conforms to.")

        # Input data
        initial_molecule: ObjectId = Field(
            ..., description="The Id of the molecule which was passed in as the reference for this Optimization."
        )
        qc_spec: QCSpecification = Field(
            ..., description="The specification of the quantum chemistry calculation to run at each point."
        )
        keywords: Dict[str, Any] = Field(
            {},
            description="The keyword options which were passed into the Optimization program. "
            "Note: These are a dictionary and not a :class:`KeywordSet` object.",
        )
        protocols: Optional[qcel.models.procedures.OptimizationProtocols] = Field(
            qcel.models.procedures.OptimizationProtocols(), description=""
        )

        # Automatting issue currently
        # description=str(qcel.models.procedures.OptimizationProtocols.__doc__))

        # Results
        energies: List[float] = Field(None, description="The ordered list of energies at each step of the Optimization.")
        final_molecule: ObjectId = Field(
            None, description="The ``ObjectId`` of the final, optimized Molecule the Optimization procedure converged to."
        )
        trajectory: List[ObjectId] = Field(
            None,
            description="The list of Molecule Id's the Optimization procedure generated at each step of the optimization."
            "``initial_molecule`` will be the first index, and ``final_molecule`` will be the last index.",
        )

    def __init__(self, **kwargs: Any):
        """

        Parameters
        ----------
        **kwargs : Dict[str, Any]
            Additional keywords passed to the OptimizationRecord and the initial data constructor.
        """
        self._data = self._DataModel(**kwargs)

    @property
    def initial_molecule(self):
        pass

    @property
    def final_molecule(self):
        pass

    # TODO: add in optimization-specific methods


register_record(OptimizationRecord)
