"""
QCPortal Database ODM
"""
from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel

from ..models import GridOptimizationInput, Molecule, ObjectId, OptimizationSpecification, QCSpecification
from ..models.gridoptimization import GOKeywords, ScanDimension
from .collection import BaseProcedureDataset
from .collection_utils import register_collection


class GORecord(BaseModel):
    """Data model for the `reactions` list in Dataset"""
    name: str
    initial_molecule: ObjectId
    go_keywords: GOKeywords
    attributes: Dict[str, Any]  # Might be overloaded key types
    object_map: Dict[str, ObjectId] = {}


class GOSpecification(BaseModel):
    name: str
    description: Optional[str]
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification


class GridOptimizationDataset(BaseProcedureDataset):
    class DataModel(BaseProcedureDataset.DataModel):

        records: Dict[str, GORecord] = {}
        history: Set[str] = set()
        specs: Dict[str, GOSpecification] = {}

        class Config(BaseProcedureDataset.DataModel.Config):
            pass

    def add_specification(self,
                          name: str,
                          optimization_spec: OptimizationSpecification,
                          qc_spec: QCSpecification,
                          description: str = None,
                          overwrite=False) -> None:
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

        spec = GOSpecification(name=name,
                               optimization_spec=optimization_spec,
                               qc_spec=qc_spec,
                               description=description)

        return self._add_specification(name, spec, overwrite=overwrite)

    def add_entry(self,
                  name: str,
                  initial_molecule: Union[ObjectId, Molecule],
                  scans: List[ScanDimension],
                  preoptimization: bool = True,
                  attributes: Dict[str, Any] = None,
                  save: bool = True) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the entry, will be used for the index
        initial_molecule : Union[ObjectId, Molecule]
            The initial molecule to start the GridOptimization
        scans : List[ScanDimension]
            A list of ScanDimension objects detailing the dimensions to scan over.
        preoptimization : bool, optional
            If True, pre-optimizes the molecules before scanning, otherwise
        attributes : Dict[str, Any], optional
            Additional attributes and descriptions for the record
        save : bool, optional
            If true, saves the collection after adding the entry. If this is False be careful
            to call save after all entries are added, otherwise data pointers may be lost.

        """
        self._check_entry_exists(name) # Fast skip

        if attributes is None:
            attributes = {}

        # Build new objects
        molecule_id = self.client.add_molecules([initial_molecule])[0]
        go_keywords = GOKeywords(scans=scans, preoptimization=preoptimization)

        record = GORecord(name=name, initial_molecule=molecule_id, go_keywords=go_keywords, attributes=attributes)
        self._add_entry(name, record, save)

    def compute(self,
                specification: str,
                subset: Set[str] = None,
                tag: Optional[str] = None,
                priority: Optional[str] = None) -> int:
        """Computes a specification for all records in the dataset.

        Parameters
        ----------
        specification : str
            The specification name.
        subset : Set[str], optional
            Computes only a subset of the dataset.
        tag : Optional[str], optional
            The queue tag to use when submitting compute requests.
        priority : Optional[str], optional
            The priority of the jobs low, medium, or high.

        Returns
        -------
        int
            The number of submitted torsiondrives
        """
        specification = specification.lower()
        spec = self.get_specification(specification)
        if subset:
            subset = set(subset)

        submitted = 0
        for rec in self.data.records.values():
            if specification in rec.object_map:
                continue

            if (subset is not None) and (rec.name not in subset):
                continue

            service = GridOptimizationInput(initial_molecule=rec.initial_molecule,
                                            keywords=rec.go_keywords,
                                            optimization_spec=spec.optimization_spec,
                                            qc_spec=spec.qc_spec)

            rec.object_map[spec.name] = self.client.add_service([service], tag=tag, priority=priority).ids[0]
            submitted += 1

        self.data.history.add(specification)
        self.save()
        return submitted


register_collection(GridOptimizationDataset)
