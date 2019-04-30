"""
QCPortal Database ODM
"""
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
from pydantic import BaseModel

from .collection import BaseProcedureDataset
from .collection_utils import register_collection
from ..models import Molecule, ObjectId, OptimizationSpecification, QCSpecification


class OptRecord(BaseModel):
    """Data model for the optimizations in a Dataset"""
    name: str
    initial_molecule: ObjectId
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Union[int, float, str]] = {}  # Might be overloaded key types
    object_map: Dict[str, ObjectId] = {}


class OptSpecification(BaseModel):
    name: str
    description: Optional[str]
    optimization_spec: OptimizationSpecification
    qc_spec: QCSpecification


class OptimizationDataset(BaseProcedureDataset):
    class DataModel(BaseProcedureDataset.DataModel):

        records: Dict[str, OptRecord] = {}
        history: Set[str] = set()
        specs: Dict[str, OptSpecification] = {}

        class Config(BaseProcedureDataset.DataModel.Config):
            pass

    def add_specification(self,
                          name: str,
                          optimization_spec: OptimizationSpecification,
                          qc_spec: QCSpecification,
                          description: str=None,
                          overwrite=False) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the specification
        optimization_spec : OptimizationSpecification
            A full optimization specification for Optimization
        qc_spec : QCSpecification
            A full quantum chemistry specification for Optimization
        description : str, optional
            A short text description of the specification
        overwrite : bool, optional
            Overwrite existing specification names

        """

        spec = OptSpecification(
            name=name, optimization_spec=optimization_spec, qc_spec=qc_spec, description=description)

        return self._add_specification(name, spec, overwrite=overwrite)

    def add_entry(self, name: str, initial_molecule: Molecule, additional_keywords: Dict[str, Any]=None, attributes: Dict[str, Any]=None) -> None:
        """
        Parameters
        ----------
        name : str
            The name of the entry, will be used for the index
        initial_molecule : Molecule
            The list of starting Molecules for the Optimization
        additional_keywords : Dict[str, Any], optional
            Additional keywords to add to the optimization run
        attributes : Dict[str, Any], optional
            Additional attributes and descriptions for the record

        """

        if additional_keywords is None:
            additional_keywords = {}

        if attributes is None:
            attributes = {}

        # Build new objects
        molecule_id = self.client.add_molecules([initial_molecule])[0]
        record = OptRecord(
            name=name, initial_molecule=molecule_id, additional_keywords=additional_keywords, attributes=attributes)

        self._add_entry(name, record)

    def compute(self, specification: str, subset: Set[str]=None, tag: Optional[str]=None,
                priority: Optional[str]=None) -> int:
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

            # Form per-procedure keywords dictionary
            general_keywords = spec.optimization_spec.keywords
            if general_keywords is None:
                general_keywords = {}
            keywords = {**general_keywords, **rec.additional_keywords}

            procedure_parameters = {
                "keywords": keywords,
                "qc_spec": spec.qc_spec.dict()
            }

            rec.object_map[spec.name] = self.client.add_procedure(
                "optimization",
                spec.optimization_spec.program,
                procedure_parameters, [rec.initial_molecule],
                tag=tag,
                priority=priority).ids[0]
            submitted += 1

        self.data.history.add(specification)
        self.save()
        return submitted

    def counts(self, entries: Optional[Union[str, List[str]]]=None,
               specs: Optional[Union[str, List[str]]]=None) -> 'DataFrame':
        """Counts the number of optimization or gradient evaluations associated with the
        Optimizations.

        Parameters
        ----------
        entries : Union[str, List[str]]
            The entries to query for
        specs : Optional[Union[str, List[str]]], optional
            The specifications to query for
        count_gradients : bool, optional
            If True, counts the total number of gradient calls. Warning! This can be slow for large datasets.

        Returns
        -------
        DataFrame
            The queried counts.
        """

        if isinstance(specs, str):
            specs = [specs]

        if isinstance(entries, str):
            entries = [entries]

        # Query all of the specs and make sure they are valid
        if specs is None:
            specs = list(self.df.columns)
        else:
            new_specs = []
            for spec in specs:
                new_specs.append(self.query(spec))

            # Remap names
            specs = new_specs

        def count_gradients(opt):
            if opt.status != "COMPLETE":
                return None
            return len(opt.trajectory)

        # Loop over the data and apply the count function
        ret = []
        for col in specs:
            data = self.df[col]
            if entries:
                data = data[entries]

            cnts = data.apply(lambda td: count_gradients(td))
            ret.append(cnts)

        ret = pd.DataFrame(ret).transpose()
        ret.dropna(inplace=True, how="all")
        # ret = pd.DataFrame([ret[x].astype(int) for x in ret.columns]).transpose()
        return ret

register_collection(OptimizationDataset)
