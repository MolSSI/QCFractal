"""
QCPortal Database ODM
"""
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

from ..models import GridOptimizationInput, ObjectId, OptimizationSpecification, ProtoModel, QCSpecification
from ..models.gridoptimization import GOKeywords
from .collection import BaseProcedureDataset
from .collection_utils import register_collection

if TYPE_CHECKING:  # pragma: no cover
    from ..models.gridoptimization import ScanDimension
    from ..models import Molecule


class GOEntry(ProtoModel):
    """Data model for the `reactions` list in Dataset"""

    name: str
    initial_molecule: ObjectId
    go_keywords: GOKeywords
    attributes: Dict[str, Any]  # Might be overloaded key types
    object_map: Dict[str, ObjectId] = {}


class GOEntrySpecification(ProtoModel):
    name: str
    description: Optional[str]
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification


class GridOptimizationDataset(BaseProcedureDataset):
    class DataModel(BaseProcedureDataset.DataModel):

        records: Dict[str, GOEntry] = {}
        history: Set[str] = set()
        specs: Dict[str, GOEntrySpecification] = {}

        class Config(BaseProcedureDataset.DataModel.Config):
            pass

    def _internal_compute_add(self, spec: Any, entry: Any, tag: str, priority: str) -> ObjectId:
        service = GridOptimizationInput(
            initial_molecule=entry.initial_molecule,
            keywords=entry.go_keywords,
            optimization_spec=spec.optimization_spec,
            qc_spec=spec.qc_spec,
        )

        return self.client.add_service([service], tag=tag, priority=priority).ids[0]

    def add_specification(
        self,
        name: str,
        optimization_spec: OptimizationSpecification,
        qc_spec: QCSpecification,
        description: str = None,
        overwrite=False,
    ) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the specification
        optimization_spec : OptimizationSpecification
            A full optimization specification for TorsionDrive
        qc_spec : QCSpecification
            A full quantum chemistry specification for TorsionDrive
        description : str, optional
            A short text description of the specification
        overwrite : bool, optional
            Overwrite existing specification names

        """

        spec = GOEntrySpecification(
            name=name, optimization_spec=optimization_spec, qc_spec=qc_spec, description=description
        )

        return self._add_specification(name, spec, overwrite=overwrite)

    def add_entry(
        self,
        name: str,
        initial_molecule: "Molecule",
        scans: List["ScanDimension"],
        preoptimization: bool = True,
        attributes: Dict[str, Any] = None,
        save: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the entry, will be used for the index
        initial_molecule : Molecule
            The initial molecule to start the GridOptimization
        scans : List[ScanDimension]
            A list of ScanDimension objects detailing the dimensions to scan over.
        preoptimization : bool, optional
            If True, pre-optimizes the molecules before scanning, otherwise
        attributes : Dict[str, Any], optional
            Additional attributes and descriptions for the entry
        save : bool, optional
            If true, saves the collection after adding the entry. If this is False be careful
            to call save after all entries are added, otherwise data pointers may be lost.

        """

        self._check_entry_exists(name)  # Fast skip

        if attributes is None:
            attributes = {}

        # Build new objects
        molecule_id = self.client.add_molecules([initial_molecule])[0]
        go_keywords = GOKeywords(scans=scans, preoptimization=preoptimization)

        record = GOEntry(name=name, initial_molecule=molecule_id, go_keywords=go_keywords, attributes=attributes)
        self._add_entry(name, record, save)


register_collection(GridOptimizationDataset)
